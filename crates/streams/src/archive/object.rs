//! An archive on object storage through the `object_store` crate: S3 and S3-compatible stores.

use super::StreamArchive;
use crate::{StreamError, StreamResult};
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path,
};
use std::sync::Arc;

/// A [StreamArchive] on object storage, under one prefix of one bucket.
///
/// A write sends `If-None-Match: *`, so a name is written once. The listing calls take the whole
/// listing after their start, since `object_store` makes no promise about its order.
pub struct ObjectStoreArchive {
    store: Arc<dyn ObjectStore>,
    root: String,
}

impl std::fmt::Debug for ObjectStoreArchive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreArchive")
            .field("store", &self.store.to_string())
            .field("root", &self.root)
            .finish()
    }
}

fn failed(error: object_store::Error) -> StreamError {
    StreamError::storage(format!("the archive failed: {error}"))
}

impl ObjectStoreArchive {
    /// An archive in `store`, with every name under `root`.
    pub fn new(store: Arc<dyn ObjectStore>, root: &str) -> Self {
        Self {
            store,
            root: root.trim_matches('/').to_string(),
        }
    }

    /// An archive at an `s3://<bucket>/<prefix>` URL. Credentials, the region and the endpoint
    /// come from the environment, as `AWS_ACCESS_KEY_ID`, `AWS_REGION`, `AWS_ENDPOINT_URL` and
    /// the others that `object_store` reads.
    pub fn from_s3_url(url: &str) -> StreamResult<Self> {
        let rest = url.strip_prefix("s3://").ok_or_else(|| {
            StreamError::refused(format!("the archive URL {url:?} is no s3:// URL"))
        })?;
        let (bucket, root) = rest.split_once('/').unwrap_or((rest, ""));
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .map_err(|error| StreamError::refused(format!("the archive URL {url:?}: {error}")))?;
        Ok(Self::new(Arc::new(store), root))
    }

    fn path(&self, name: &str) -> StreamResult<Path> {
        let full = if self.root.is_empty() {
            name.to_string()
        } else {
            format!("{}/{name}", self.root)
        };
        Path::parse(full.trim_end_matches('/'))
            .map_err(|error| StreamError::storage(format!("archive name {name:?}: {error}")))
    }

    /// The name of `path` under the root.
    fn name(&self, path: &Path) -> String {
        let path = path.as_ref();
        match self.root.as_str() {
            "" => path.to_string(),
            root => path
                .strip_prefix(root)
                .and_then(|rest| rest.strip_prefix('/'))
                .unwrap_or(path)
                .to_string(),
        }
    }

    /// The names that start with `prefix` and sort after `after`, in no order.
    async fn names(&self, prefix: &str, after: Option<&str>) -> StreamResult<Vec<String>> {
        let dir = self.path(prefix.rsplit_once('/').map_or("", |(dir, _)| dir))?;
        let listing = match after {
            Some(after) => self.store.list_with_offset(Some(&dir), &self.path(after)?),
            None => self.store.list(Some(&dir)),
        };
        let objects: Vec<_> = listing.try_collect().await.map_err(failed)?;
        Ok(objects
            .iter()
            .map(|object| self.name(&object.location))
            .filter(|name| name.starts_with(prefix) && after.is_none_or(|after| **name > *after))
            .collect())
    }
}

#[async_trait::async_trait]
impl StreamArchive for ObjectStoreArchive {
    async fn put_new(&self, name: &str, bytes: Vec<u8>) -> StreamResult<bool> {
        let options = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&self.path(name)?, PutPayload::from(bytes), options)
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(error) => Err(failed(error)),
        }
    }

    async fn get(&self, name: &str) -> StreamResult<Option<Vec<u8>>> {
        match self.store.get(&self.path(name)?).await {
            Ok(found) => Ok(Some(found.bytes().await.map_err(failed)?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(failed(error)),
        }
    }

    async fn first_after(&self, prefix: &str, after: &str) -> StreamResult<Option<String>> {
        Ok(self.names(prefix, Some(after)).await?.into_iter().min())
    }

    async fn last(&self, prefix: &str) -> StreamResult<Option<String>> {
        Ok(self.names(prefix, None).await?.into_iter().max())
    }

    async fn delete_prefix(&self, prefix: &str) -> StreamResult<u64> {
        let names = self.names(prefix, None).await?;
        let paths = names
            .iter()
            .map(|name| self.path(name))
            .collect::<StreamResult<Vec<_>>>()?;
        let count = paths.len() as u64;
        self.store
            .delete_stream(futures_util::stream::iter(paths.into_iter().map(Ok)).boxed())
            .try_collect::<Vec<_>>()
            .await
            .map_err(failed)?;
        Ok(count)
    }
}
