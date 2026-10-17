//! The Lua scripts, on the stored format the SDKs used before the store moved into Core.
//!
//! The append script is the one the SDKs sent, with one more check, the topic's close flag, so a
//! batch can't land after the topic's close returned (DD-16), and its trim keeps what
//! positions-only readers hold. Its trim reads back from the floor only when the oldest record is
//! below it, since `XREVRANGE` from the floor reads the whole retained log when no record is older,
//! and a forward read copies every doomed record into Lua. Redis doesn't roll a script back, so it
//! runs every step that can fail before its first record lands. `keep.lua` is the prelude of every
//! script that writes a log, and `floor.lua` of every script that reads or writes the holds.

use redis::Script;
use std::sync::LazyLock;

pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/floor.lua"),
    include_str!("../../lua/keep.lua"),
    include_str!("../../lua/append.lua")
);

pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));

/// The line that names the key of a Workflow Activity's topic set, which only its appends pass.
/// It is the key after the notification stream when the store keeps a notification log.
const TOPICS_KEY: &str = "local topics = KEYS[5]\n";

/// What a store with a notification log writes after the batch, in place of the append's last
/// answer: an entry in the chain's notification stream (`KEYS[5]`), and the stream's length, so
/// Core rings the doorbell when the entry is the stream's only one. A repeat writes no entry.
const LOGGED_ANSWER: &str = "redis.call('XADD', KEYS[5], 'MAXLEN', '~', 10000, '*', \
'k', KEYS[1], 'p', last)
return {first, last, redis.call('XLEN', KEYS[5])}
";

/// [APPEND] with its last answer replaced by [LOGGED_ANSWER], and the topic set as `KEYS[6]`.
pub(crate) static APPEND_LOGGED: LazyLock<String> = LazyLock::new(|| {
    let (body, last) = APPEND
        .trim_end()
        .rsplit_once('\n')
        .expect("the append script has a last line");
    assert_eq!(last, "return {first, last}");
    assert_eq!(body.matches(TOPICS_KEY).count(), 1);
    let body = body.replacen(TOPICS_KEY, "local topics = KEYS[6]\n", 1);
    format!("{body}\n{LOGGED_ANSWER}")
});

pub(crate) static APPEND_LOGGED_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(&APPEND_LOGGED));

/// What a store with an archive adds before the batch: the archive's hold on a topic's first
/// write, from the topic's start, with the lease in `ARGV[6]`. So retention can't drop a record
/// before the archive's first pass. The holds hash lives at least as long as the lease, since a
/// reader's hold only lengthens its life and a shorter one must not take the archive's hold away.
const ARCHIVE_HOLD: &str = "if redis.call('EXISTS', KEYS[2]) == 0 and \
redis.call('HEXISTS', KEYS[4], 'archive') == 0 then
  local lease = tonumber(ARGV[6])
  redis.call('HSET', KEYS[4], 'archive', '0-1|' .. (now_ms() + lease))
  if redis.call('PTTL', KEYS[4]) < lease then
    redis.call('PEXPIRE', KEYS[4], lease)
  end
end
revive(KEYS[1], KEYS[2])
";

/// What a store with an archive adds after the notification entry: the log key in the slot's
/// set of topics with records to archive (`KEYS[6]`), scored by the time of its newest write.
const ARCHIVE_MARK: &str = "redis.call('ZADD', KEYS[6], now_ms(), KEYS[1])
return {first, last, redis.call('XLEN', KEYS[5])}
";

/// [APPEND_LOGGED] with the archive's hold and mark. The lease takes `ARGV[6]`, so the records
/// start at `ARGV[7]`, and the topic set moves to `KEYS[7]`.
pub(crate) static APPEND_ARCHIVED: LazyLock<String> = LazyLock::new(|| {
    let edits = [
        ("revive(KEYS[1], KEYS[2])\n", ARCHIVE_HOLD),
        ("for i = 6, #ARGV do", "for i = 7, #ARGV do"),
        ("local topics = KEYS[6]\n", "local topics = KEYS[7]\n"),
        (
            "return {first, last, redis.call('XLEN', KEYS[5])}\n",
            ARCHIVE_MARK,
        ),
    ];
    let mut script = APPEND_LOGGED.clone();
    for (was, now) in edits {
        assert_eq!(script.matches(was).count(), 1, "{was}");
        script = script.replacen(was, now, 1);
    }
    assert_eq!(script.matches("#ARGV - 5").count(), 2);
    script.replace("#ARGV - 5", "#ARGV - 6")
});

pub(crate) static APPEND_ARCHIVED_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(&APPEND_ARCHIVED));

/// Moves the archive's hold forward and keeps the position as the meta's `archived`. When the
/// archive holds the topic's last record, the topic leaves the slot's set to archive.
pub(crate) static ADVANCE_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        include_str!("../../lua/floor.lua"),
        include_str!("../../lua/advance.lua")
    ))
});
// Marks the topic closed and keeps its first close's outcome in one step, so two closes can't both
// find no outcome and the second overwrite the first.
pub(crate) static CLOSE_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../../lua/close.lua")));
pub(crate) static HOLD_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        include_str!("../../lua/floor.lua"),
        include_str!("../../lua/hold.lua")
    ))
});
pub(crate) static RELEASE_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        include_str!("../../lua/floor.lua"),
        include_str!("../../lua/release.lua")
    ))
});

#[cfg(test)]
mod tests {
    use super::*;

    /// The close check the append script adds to the SDKs' one.
    const TOPIC_CHECK: &str = " or redis.call('HGET', KEYS[2], 'closed')";

    /// What the append script adds for a Workflow Activity's topic set.
    const TOPIC_SET: &str = "local topics = KEYS[5]
if topics then
  -- On every batch, since the first one can be lost and the close must still find the topic.
  redis.call('SADD', topics, KEYS[1])
  redis.call('PEXPIRE', topics, retention + grace)
end
";

    /// How the trim finds the newest record to drop, and what it replaces.
    const TRIM_SCAN: (&str, &str) = (
        "  -- Two reads that cost O(log n) each. Reading back from the floor walks the whole log
  -- when no record is older, so it runs only when the oldest record is.
  local oldest = redis.call('XRANGE', log, '-', '+', 'COUNT', 1)
  if #oldest > 0 and id_below(oldest[1][1], floor) then
    local doomed = redis.call('XREVRANGE', log, '(' .. floor, '-', 'COUNT', 1)
    redis.call('XTRIM', log, 'MINID', floor)
    redis.call('HSET', meta, 'trimmed', doomed[1][1])
  end
",
        "  local doomed = redis.call('XREVRANGE', log, '(' .. floor, '-', 'COUNT', 1)
  if #doomed > 0 then
    redis.call('XTRIM', log, 'MINID', floor)
    redis.call('HSET', meta, 'trimmed', doomed[1][1])
  end
",
    );

    /// What the floor adds to the append script, each with what it replaces.
    const FLOOR_EDITS: [(&str, &str); 8] = [
        (
            "local function trim(log, meta, retention, floors)",
            "local function trim(log, meta, retention)",
        ),
        (
            "  local life\n  floor, life = held(floors, floor, retention)\n",
            "",
        ),
        ("  return floor_ms, life\n", "  return floor_ms\n"),
        (
            "local function keep(log, meta, added, last, grace, floor_ms, life)",
            "local function keep(log, meta, added, last, retention, grace, floor_ms)",
        ),
        (
            "redis.call('PEXPIRE', log, life)",
            "redis.call('PEXPIRE', log, retention)",
        ),
        (
            "redis.call('PEXPIRE', meta, life + grace)",
            "redis.call('PEXPIRE', meta, retention + grace)",
        ),
        (
            "local floor_ms, life = trim(KEYS[1], KEYS[2], retention, KEYS[4])",
            "local floor_ms = trim(KEYS[1], KEYS[2], retention)",
        ),
        (
            "keep(KEYS[1], KEYS[2], #ARGV - 5, last, grace, floor_ms, life)",
            "keep(KEYS[1], KEYS[2], #ARGV - 5, last, retention, grace, floor_ms)",
        ),
    ];

    #[test]
    fn the_logged_append_differs_in_its_topic_set_key_and_last_answer_only() {
        assert_eq!(
            APPEND_LOGGED
                .replacen(LOGGED_ANSWER, "return {first, last}\n", 1)
                .replacen("local topics = KEYS[6]\n", TOPICS_KEY, 1),
            APPEND
        );
        assert_eq!(APPEND_LOGGED.matches("XADD', KEYS[5]").count(), 1);
        assert_eq!(
            APPEND_LOGGED_SCRIPT.get_hash(),
            "49820889a3c7680122a0b43ed0946c4b4cb0286e"
        );
    }

    #[test]
    fn the_archived_append_differs_from_the_logged_one_in_its_hold_and_mark_only() {
        let mut was = APPEND_ARCHIVED
            .replacen(ARCHIVE_HOLD, "revive(KEYS[1], KEYS[2])\n", 1)
            .replacen(
                ARCHIVE_MARK,
                "return {first, last, redis.call('XLEN', KEYS[5])}\n",
                1,
            )
            .replacen("local topics = KEYS[7]\n", "local topics = KEYS[6]\n", 1)
            .replacen("for i = 7, #ARGV do", "for i = 6, #ARGV do", 1);
        was = was.replace("#ARGV - 6", "#ARGV - 5");
        assert_eq!(was, *APPEND_LOGGED);
        assert_eq!(
            APPEND_ARCHIVED_SCRIPT.get_hash(),
            "98302a233c87f8a10303d4ca17b418cb51fe2799"
        );
    }

    #[test]
    fn the_append_script_is_the_one_before_the_floor_with_the_floor_edits_only() {
        assert_eq!(
            APPEND_SCRIPT.get_hash(),
            "b818ae457ec5777a3a1d321e17215037cfdf5c6e"
        );
        let mut before_floor = APPEND
            .strip_prefix(include_str!("../../lua/floor.lua"))
            .unwrap()
            .to_string();
        let (scan, was) = TRIM_SCAN;
        assert_eq!(before_floor.matches(scan).count(), 1);
        before_floor = before_floor.replacen(scan, was, 1);
        assert_eq!(before_floor.matches(TOPIC_SET).count(), 1);
        before_floor = before_floor.replacen(TOPIC_SET, "", 1);
        for (added, was) in FLOOR_EDITS {
            assert_eq!(before_floor.matches(added).count(), 1, "{added}");
            before_floor = before_floor.replacen(added, was, 1);
        }
        assert_eq!(
            Script::new(&before_floor).get_hash(),
            "806e20d928e761eff5467386c0ffe78372e48bec"
        );
        assert_eq!(before_floor.matches(TOPIC_CHECK).count(), 1);
    }
}
