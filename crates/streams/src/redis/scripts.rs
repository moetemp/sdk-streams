//! The Lua scripts, one per operation. They are the canonical form of the stored format, so
//! their hashes are pinned. A change to a script is a change of the format.

use redis::Script;
use std::sync::LazyLock;

#[cfg(test)]
const LIB: &str = include_str!("../../lua/lib.lua");

pub(crate) const CREATE: &str = concat!(
    include_str!("../../lua/lib.lua"),
    include_str!("../../lua/create.lua")
);
pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/lib.lua"),
    include_str!("../../lua/append.lua")
);
pub(crate) const CLOSE: &str = concat!(
    include_str!("../../lua/lib.lua"),
    include_str!("../../lua/close.lua")
);
pub(crate) const AUTO_CLOSE: &str = concat!(
    include_str!("../../lua/lib.lua"),
    include_str!("../../lua/auto_close.lua")
);
/// Read only, so it also runs on a replica and under a user that may only read.
pub(crate) const READ: &str = concat!(
    "#!lua flags=no-writes\n",
    include_str!("../../lua/read.lua")
);

pub(crate) static CREATE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(CREATE));
pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));
pub(crate) static CLOSE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(CLOSE));
pub(crate) static AUTO_CLOSE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(AUTO_CLOSE));
pub(crate) static READ_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(READ));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_writing_script_starts_with_the_prelude() {
        for script in [CREATE, APPEND, CLOSE, AUTO_CLOSE] {
            assert!(script.starts_with(LIB));
        }
        assert!(!READ.contains("redis.call('XADD'"));
    }

    #[test]
    fn the_auto_close_record_is_an_encoded_close_record() {
        use crate::proto::{StreamRecord, StreamRecordKind};
        use prost::Message;
        let record = StreamRecord {
            kind: StreamRecordKind::Close as i32,
            ..Default::default()
        };
        assert_eq!(record.encode_to_vec(), b"\x08\x03");
        assert!(LIB.contains("local AUTO_CLOSE_RECORD = '\\8\\3'"));
    }
}
