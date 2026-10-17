//! One hash tag per Redis Cluster slot, so a key can name the slot it lands in.
//!
//! A chain's notification entries go to the notification stream of the chain's own slot, so the
//! append script writes them in the same step. `testdata/redis/notify_slots.json` pins the table,
//! since every SDK must name the same keys.

use super::conn::slot;
use std::sync::LazyLock;

/// How many slots a Redis Cluster has.
pub(crate) const SLOTS: usize = 16_384;

/// The tag of each slot: the first base-36 numeral, counting from zero, that hashes to it.
static TAGS: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut tags = vec![String::new(); SLOTS];
    let mut left = SLOTS;
    let mut numeral = 0u64;
    while left > 0 {
        let tag = base36(numeral);
        let at = usize::from(slot(&tag));
        if tags[at].is_empty() {
            tags[at] = tag;
            left -= 1;
        }
        numeral += 1;
    }
    tags
});

fn base36(mut value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = vec![];
    loop {
        out.push(DIGITS[(value % 36) as usize]);
        value /= 36;
        if value == 0 {
            break;
        }
    }
    out.reverse();
    String::from_utf8(out).expect("base-36 digits are ASCII")
}

/// The hash tag that lands in `slot`.
pub(crate) fn tag(slot: u16) -> &'static str {
    &TAGS[usize::from(slot) % SLOTS]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_tag_hashes_to_its_slot() {
        for at in 0..SLOTS as u16 {
            assert_eq!(slot(&format!("{{{}}}", tag(at))), at, "slot {at}");
        }
    }

    #[test]
    fn the_table_matches_the_pinned_vectors() {
        let pinned: Vec<String> =
            serde_json::from_str(include_str!("../../testdata/redis/notify_slots.json")).unwrap();
        assert_eq!(pinned.len(), SLOTS);
        assert!(pinned.iter().zip(TAGS.iter()).all(|(a, b)| a == b));
    }
}
