//! Offsets. An offset is the Redis entry id `<ms>-<seq>` packed as `ms << 20 | seq` in an `i64`.
//! It is ordered and unique in one Stream, and sparse. Zero is never an offset.

use crate::{StreamError, StreamResult};

/// The number of bits of the entry id's sequence part.
pub const SEQ_BITS: u32 = 20;

const SEQ_MASK: i64 = (1 << SEQ_BITS) - 1;
/// The largest millisecond part that fits, which is past the year 2200.
const MAX_MS: u64 = (i64::MAX >> SEQ_BITS) as u64;

/// Packs a Redis entry id.
pub fn offset_of_entry_id(id: &str) -> StreamResult<i64> {
    let bad = || StreamError::storage(format!("The Store gave the entry id {id:?}"));
    let (ms, seq) = id.split_once('-').ok_or_else(bad)?;
    let ms: u64 = ms.parse().map_err(|_| bad())?;
    let seq: u64 = seq.parse().map_err(|_| bad())?;
    if ms > MAX_MS || seq > SEQ_MASK as u64 {
        return Err(StreamError::storage(format!(
            "The entry id {id:?} doesn't fit in an offset"
        )));
    }
    Ok(((ms as i64) << SEQ_BITS) | seq as i64)
}

/// Unpacks an offset into its Redis entry id. The offset must be above zero.
pub fn entry_id_of_offset(offset: i64) -> String {
    debug_assert!(offset > 0);
    format!("{}-{}", offset >> SEQ_BITS, offset & SEQ_MASK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_offset_round_trips_and_keeps_the_entry_order() {
        let ids = [
            "0-1",
            "1-0",
            "1-1",
            "1700000000000-0",
            "1700000000000-1048575",
        ];
        let offsets: Vec<_> = ids
            .iter()
            .map(|id| offset_of_entry_id(id).unwrap())
            .collect();
        for (id, offset) in ids.iter().zip(&offsets) {
            assert_eq!(&entry_id_of_offset(*offset), id);
        }
        assert!(offsets.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn an_entry_id_that_doesnt_fit_is_refused() {
        assert!(offset_of_entry_id("1-1048576").is_err());
        assert!(offset_of_entry_id(&format!("{}-0", MAX_MS + 1)).is_err());
        assert!(offset_of_entry_id("x").is_err());
        assert_eq!(
            offset_of_entry_id(&format!("{MAX_MS}-{SEQ_MASK}")).unwrap(),
            i64::MAX
        );
    }
}
