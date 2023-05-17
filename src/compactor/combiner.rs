use std::{cmp, collections::BinaryHeap};

use crate::protocol::ReadRecord;

struct IterBuf<T>
where
    T: Iterator<Item = ReadRecord>,
{
    iter: T,
    buf: Option<ReadRecord>,
    level: u32,
    sequence: Option<u32>,
}

impl<T> Ord for IterBuf<T>
where
    T: Iterator<Item = ReadRecord>,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.buf, &other.buf) {
            (Some(s), Some(o)) => {
                if o.key() != s.key() {
                    // Always order by keys (ascending) as the primary criteria. If the keys are
                    // equal, order in ascending age.
                    return o.key().cmp(s.key());
                } else if self.level != other.level {
                    // Lower levels are newer.
                    return other.level.cmp(&self.level);
                }

                // Higher sequence within the same level (only possible in level 0) are newer.
                match (&self.sequence, &other.sequence) {
                    (Some(ss), Some(os)) => ss.cmp(&os),
                    (_, _) => {
                        unreachable!("cannot have equal keys within the same level and sequence")
                    }
                }
            }
            (Some(_), None) => cmp::Ordering::Greater,
            (None, Some(_)) => cmp::Ordering::Less,
            (None, None) => cmp::Ordering::Equal,
        }
    }
}

impl<T> PartialOrd for IterBuf<T>
where
    T: Iterator<Item = ReadRecord>,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Eq for IterBuf<T> where T: Iterator<Item = ReadRecord> {}

impl<T> PartialEq for IterBuf<T>
where
    T: Iterator<Item = ReadRecord>,
{
    fn eq(&self, other: &Self) -> bool {
        match (&self.buf, &other.buf) {
            (Some(s), Some(o)) => s.key() == o.key(),
            (None, None) => true,
            (None, Some(_)) => false,
            (Some(_), None) => false,
        }
    }
}

struct MergeIter<T>
where
    T: Iterator<Item = ReadRecord>,
{
    iters: BinaryHeap<IterBuf<T>>,
}

impl<T> MergeIter<T>
where
    T: Iterator<Item = ReadRecord>,
{
    pub fn new() -> Self {
        return MergeIter {
            iters: BinaryHeap::new(),
        };
    }

    pub fn push_iter(&mut self, mut iter: T, level: u32, sequence: Option<u32>) {
        let buf = iter.next();
        self.iters.push(IterBuf {
            iter,
            buf,
            level,
            sequence,
        })
    }
}

impl<T> Iterator for MergeIter<T>
where
    T: Iterator<Item = ReadRecord>,
{
    type Item = ReadRecord;

    fn next(&mut self) -> Option<Self::Item> {
        // Get the highest priority iterator.
        let mut n = self.iters.pop()?;
        let record = n.buf.expect("Buffer must not be None");

        // This might be the first of this key value and if it is it's the newest, so clear out
        // everything with the same key value.
        while let Some(nn) = self.iters.peek() {
            let next_record = nn.buf.as_ref().expect("Buffer must not be None");
            if record.key() == next_record.key() {
                // Pop and re-fill the buf of the next iterator.
                self.iters.pop().map(|mut taken| {
                    taken.buf = taken.iter.next();
                    self.iters.push(taken);
                });
            } else {
                break;
            }
        }

        // Put this iterator back in, first re-filling its buffer, as long as the iterator isn't
        // empty.
        if let Some(new_buf) = n.iter.next() {
            n.buf = Some(new_buf);
            self.iters.push(n)
        }

        Some(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_iter() {
        let iter_1: Vec<ReadRecord> = vec![
            ReadRecord::Exists {
                key: b"key1".to_vec(),
                val: b"val1_1".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key2".to_vec(),
                val: b"val2_1".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key3".to_vec(),
                val: b"val3_1".to_vec(),
            },
        ];

        let iter_2: Vec<ReadRecord> = vec![
            ReadRecord::Exists {
                key: b"key1".to_vec(),
                val: b"val1_2".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key2".to_vec(),
                val: b"val2_2".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key3".to_vec(),
                val: b"val3_2".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key4".to_vec(),
                val: b"val4_2".to_vec(),
            },
        ];

        let iter_3: Vec<ReadRecord> = vec![ReadRecord::Deleted {
            key: b"key2".to_vec(),
        }];

        let mut merged = MergeIter::new();

        merged.push_iter(iter_1.into_iter(), 0, Some(0));
        merged.push_iter(iter_2.into_iter(), 1, None);
        merged.push_iter(iter_3.into_iter(), 0, Some(1));

        let want: Vec<ReadRecord> = vec![
            ReadRecord::Exists {
                key: b"key1".to_vec(),
                val: b"val1_1".to_vec(),
            },
            ReadRecord::Deleted {
                key: b"key2".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key3".to_vec(),
                val: b"val3_1".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key4".to_vec(),
                val: b"val4_2".to_vec(),
            },
        ];

        for (got, want) in merged.zip(want.into_iter()) {
            assert_eq!(want, got)
        }
    }
}
