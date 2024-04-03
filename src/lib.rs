//! blackfrost is a snowflake ID generator library designed to be easily used in
//! concurrent environments, such as web servers running on tokio.
//!
//! blackfrost is fast and has a small memory footprint. Both properties
//! arise from the use of a single atomic integer to remember the last generated
//! ID, which allows IDs to be generated using a lockless algorithm.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::time;

const fn mask(start: usize, bits: usize) -> i64 {
  assert!((start + bits) <= 63, "bit overflow into the sign bit");
  ((1 << bits) - 1) << start
}

const fn base_mask(bits: usize) -> i64 {
  mask(0, bits)
}

const NODE_BITS: usize = 8;
const SEQUENCE_BITS: usize = 12;
const TIMESTAMP_BITS: usize = 43;

const NODE_SHIFT: usize = 0;
const SEQUENCE_SHIFT: usize = NODE_SHIFT + NODE_BITS;
const TIMESTAMP_SHIFT: usize = SEQUENCE_SHIFT + SEQUENCE_BITS;

const NODE_MASK: i64 = mask(NODE_SHIFT, NODE_BITS);
const SEQUENCE_MASK: i64 = mask(SEQUENCE_SHIFT, SEQUENCE_BITS);
const TIMESTAMP_MASK: i64 = mask(TIMESTAMP_SHIFT, TIMESTAMP_BITS);

const MAX_NODE: i64 = base_mask(NODE_BITS);
const MAX_SEQUENCE: i64 = base_mask(SEQUENCE_BITS);

const SEQUENCE_EXHAUSTED_DELAY: Duration = Duration::from_micros(100);

fn timestamp() -> i64 {
  SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_secs()
    .try_into()
    .unwrap()
}

struct Snowflake {
  timestamp: i64,
  sequence: i64,
  node: i64,
}

fn pack(snowflake: Snowflake) -> i64 {
  (snowflake.timestamp << TIMESTAMP_SHIFT)
    | (snowflake.sequence << SEQUENCE_SHIFT)
    | (snowflake.node << NODE_SHIFT)
}

fn unpack(id: i64) -> Snowflake {
  Snowflake {
    timestamp: (id & TIMESTAMP_MASK) >> TIMESTAMP_SHIFT,
    sequence: (id & SEQUENCE_MASK) >> SEQUENCE_SHIFT,
    node: (id & NODE_MASK) >> NODE_SHIFT,
  }
}

/// Concurrent snowflake ID generator with the following layout:
/// ```text
/// | 1 bit | 43 bits   | 12 bits  | 8 bits |
/// | sign  | timestamp | sequence | node   |
/// ```
///
/// Timestamps are relative to the UNIX epoch. A 43-bit timestamp allows enough
/// space to generate valid timestamps for ~278 years after 1970, until the year
/// 2248.
///
/// A 12-bit sequence number allows 4,096 IDs to be generated per node per
/// second. Combined with an 8-bit node number (allowing up to 256 nodes to
/// generate IDs concurrently), 1,048,576 IDs can be generated per second across
/// all concurrent threads.
///
/// Used as follows:
/// ```
/// use blackfrost::SnowflakeGenerator;
///
/// # tokio_test::block_on(async {
/// let generator = SnowflakeGenerator::new(0);
/// let id = generator.generate_id().await;
/// # })
/// ```
///
/// The timestamp must use the highest bits to preserve monotonicity, otherwise
/// a previous timestamp with a larger sequence or node number would create an
/// ID larger than the ID with the current timestamp.
///
/// To avoid generating consecutive IDs for objects generated at the same time,
/// we avoid putting the sequence number at the lowest bits, therefore we put it
/// in the middle and leave the node number at the lowest bits.
pub struct SnowflakeGenerator {
  id: AtomicI64,
}

impl SnowflakeGenerator {
  /// Creates a new [`SnowflakeGenerator`], with the specified node ID. `node`
  /// must not be larger than [`MAX_NODE`].
  pub fn new(node: i64) -> Self {
    assert!(
      node <= MAX_NODE,
      "node is larger than the max node ({} > {})",
      node,
      MAX_NODE
    );

    Self {
      id: AtomicI64::new(pack(Snowflake {
        timestamp: 0,
        sequence: 0,
        node,
      })),
    }
  }

  /// Generates a new, signed 64-bit ID, packed with the current timestamp, the
  /// number of IDs generated during the current timestamp, and the node ID
  /// specified when the [`SnowflakeGenerator`] was created.
  ///
  /// If the [`MAX_SEQUENCE`] was exhausted, asynchronously waits for
  /// [`SEQUENCE_EXHAUSTED_DELAY`] to elapse, and retries.
  ///
  /// Synchronises with concurrent invocations where necessary.
  pub async fn generate_id(&self) -> i64 {
    let mut last_id = self.id.load(Ordering::Relaxed);
    loop {
      let Snowflake {
        timestamp: last_timestamp,
        sequence: last_sequence,
        node: last_node,
      } = unpack(last_id);

      let timestamp = timestamp();
      let sequence = if timestamp == last_timestamp {
        last_sequence + 1
      } else {
        0
      };

      if sequence > MAX_SEQUENCE {
        time::sleep(SEQUENCE_EXHAUSTED_DELAY).await;
        last_id = self.id.load(Ordering::Relaxed);
        continue;
      }

      let new_id = pack(Snowflake {
        timestamp,
        sequence,
        node: last_node,
      });

      match self
        .id
        .compare_exchange(last_id, new_id, Ordering::Relaxed, Ordering::Relaxed)
      {
        Ok(_) => break new_id,
        Err(id) => last_id = id,
      }
    }
  }
}
