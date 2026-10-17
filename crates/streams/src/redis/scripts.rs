//! The Lua scripts, on the stored format the SDKs used before the store moved into Core.
//!
//! The append script is the one the SDKs sent, with one more check, the topic's close flag, so a
//! batch can't land after the topic's close returned (DD-16). Redis doesn't roll a script back,
//! so it also runs every step that can fail before its first record lands. `keep.lua` is the
//! prelude of every script that writes a log.

use redis::Script;
use std::sync::LazyLock;

pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/append.lua")
);

pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));

#[cfg(test)]
mod tests {
    use super::*;

    /// The close check the append script adds to the SDKs' one.
    const TOPIC_CHECK: &str = " or redis.call('HGET', KEYS[2], 'closed')";

    #[test]
    fn the_append_script_is_pinned() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "806e20d928e761eff5467386c0ffe78372e48bec"
        );
        assert_eq!(APPEND.matches(TOPIC_CHECK).count(), 1);
    }
}
