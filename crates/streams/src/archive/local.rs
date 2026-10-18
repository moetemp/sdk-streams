//! An archive in a local directory, for tests and single-host deployments.

use super::StreamArchive;
use crate::{StreamError, StreamResult};
use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

/// A [StreamArchive] in a directory. Each `/` in a name is a directory step.
///
/// A write goes to a hidden file first and then takes its name with a hard link, which fails when
/// the name exists, so a name is written once and a reader never sees part of an object.
#[derive(Debug)]
pub struct LocalFileArchive {
    root: PathBuf,
    next_temp: AtomicU64,
}

impl LocalFileArchive {
    /// An archive under `root`, which it creates when it writes.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            next_temp: AtomicU64::new(0),
        }
    }

    /// The names that start with `prefix`, sorted. Names live in the directory of `prefix`'s
    /// last step.
    async fn names(&self, prefix: &str) -> StreamResult<Vec<String>> {
        let (root, prefix) = (self.root.clone(), prefix.to_string());
        blocking(move || listing(&root, &prefix)).await
    }
}

fn listing(root: &Path, prefix: &str) -> std::io::Result<Vec<String>> {
    let (dir, start) = prefix.rsplit_once('/').unwrap_or(("", prefix));
    let entries = match std::fs::read_dir(root.join(dir)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(vec![]);
        }
        Err(error) => return Err(error),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        // Hidden files are writes in flight.
        if name.starts_with('.') || !name.starts_with(start) || !entry.file_type()?.is_file() {
            continue;
        }
        names.push(if dir.is_empty() {
            name
        } else {
            format!("{dir}/{name}")
        });
    }
    names.sort();
    Ok(names)
}

fn storage(error: std::io::Error) -> StreamError {
    StreamError::storage(format!("the local archive failed: {error}"))
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> StreamResult<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| StreamError::storage(error.to_string()))?
        .map_err(storage)
}

fn count_files(path: &Path) -> std::io::Result<u64> {
    let mut count = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        count += if entry.file_type()?.is_dir() {
            count_files(&entry.path())?
        } else {
            1
        };
    }
    Ok(count)
}

#[async_trait::async_trait]
impl StreamArchive for LocalFileArchive {
    async fn put_new(&self, name: &str, bytes: Vec<u8>) -> StreamResult<bool> {
        let path = self.root.join(name);
        let (dir, leaf) = name.rsplit_once('/').unwrap_or(("", name));
        let temp = self.root.join(dir).join(format!(
            ".{leaf}.{}.{}",
            std::process::id(),
            self.next_temp.fetch_add(1, Ordering::Relaxed)
        ));
        blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&temp, bytes)?;
            let linked = std::fs::hard_link(&temp, &path);
            std::fs::remove_file(&temp)?;
            match linked {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(false),
                Err(error) => Err(error),
            }
        })
        .await
    }

    async fn get(&self, name: &str) -> StreamResult<Option<Vec<u8>>> {
        let path = self.root.join(name);
        blocking(move || match std::fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        })
        .await
    }

    async fn first_after(&self, prefix: &str, after: &str) -> StreamResult<Option<String>> {
        Ok(self
            .names(prefix)
            .await?
            .into_iter()
            .find(|name| name.as_str() > after))
    }

    async fn last(&self, prefix: &str) -> StreamResult<Option<String>> {
        Ok(self.names(prefix).await?.pop())
    }

    async fn delete_prefix(&self, prefix: &str) -> StreamResult<u64> {
        // The prefixes the store deletes are whole directories, so only those are deleted.
        let path = self.root.join(prefix.trim_end_matches('/'));
        blocking(move || {
            let count = match count_files(&path) {
                Ok(count) => count,
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(0),
                Err(error) => return Err(error),
            };
            std::fs::remove_dir_all(&path)?;
            Ok(count)
        })
        .await
    }
}
