use crate::chunk::chunk_payload_data::ChunkPayloadData;

use std::collections::VecDeque;

/// pendingBaseQueue
pub(crate) type PendingBaseQueue = VecDeque<ChunkPayloadData>;

/// pendingQueue
#[derive(Debug, Default)]
pub(crate) struct PendingQueue {
    unordered_queue: PendingBaseQueue,
    ordered_queue: PendingBaseQueue,
    queue_len: usize,
    n_bytes: usize,
    selected: bool,
    unordered_is_selected: bool,
}

impl PendingQueue {
    pub(crate) fn new() -> Self {
        PendingQueue::default()
    }

    pub(crate) fn push(&mut self, c: ChunkPayloadData) {
        self.n_bytes += c.user_data.len();
        if c.unordered {
            self.unordered_queue.push_back(c);
        } else {
            self.ordered_queue.push_back(c);
        }
        self.queue_len += 1;
    }

    pub(crate) fn peek(&self) -> Option<&ChunkPayloadData> {
        if self.selected {
            if self.unordered_is_selected {
                return self.unordered_queue.front();
            } else {
                return self.ordered_queue.front();
            }
        }

        let c = self.unordered_queue.front();

        if c.is_some() {
            return c;
        }

        self.ordered_queue.front()
    }

    pub(crate) fn pop(
        &mut self,
        beginning_fragment: bool,
        unordered: bool,
    ) -> Option<ChunkPayloadData> {
        let popped = if self.selected {
            let popped = if self.unordered_is_selected {
                self.unordered_queue.pop_front()
            } else {
                self.ordered_queue.pop_front()
            };
            if let Some(p) = &popped
                && p.ending_fragment
            {
                self.selected = false;
            }
            popped
        } else {
            if !beginning_fragment {
                return None;
            }
            if unordered {
                let popped = { self.unordered_queue.pop_front() };
                if let Some(p) = &popped
                    && !p.ending_fragment
                {
                    self.selected = true;
                    self.unordered_is_selected = true;
                }
                popped
            } else {
                let popped = { self.ordered_queue.pop_front() };
                if let Some(p) = &popped
                    && !p.ending_fragment
                {
                    self.selected = true;
                    self.unordered_is_selected = false;
                }
                popped
            }
        };

        if let Some(p) = &popped {
            self.n_bytes -= p.user_data.len();
            self.queue_len -= 1;
        }

        popped
    }

    /// Drops every message of `stream_identifier` that has not started to leave the queue,
    /// and returns the user bytes dropped.
    ///
    /// A message whose first fragment was already popped (`selected`, at the head of the
    /// selected queue) cannot be cut: its TSNs are contiguous and the peer would hold the
    /// head forever. Its remaining fragments stay, and the returned flag says so.
    pub(crate) fn purge_stream(&mut self, stream_identifier: u16) -> (usize, bool) {
        let mut partial_len = 0;
        if self.selected {
            let queue = if self.unordered_is_selected {
                &self.unordered_queue
            } else {
                &self.ordered_queue
            };
            if queue
                .front()
                .is_some_and(|c| c.stream_identifier == stream_identifier)
            {
                for c in queue {
                    partial_len += 1;
                    if c.ending_fragment {
                        break;
                    }
                }
            }
        }

        let mut n_bytes = 0;
        let mut n_chunks = 0;
        for unordered in [true, false] {
            let kept_head = if unordered == self.unordered_is_selected {
                partial_len
            } else {
                0
            };
            let queue = if unordered {
                &mut self.unordered_queue
            } else {
                &mut self.ordered_queue
            };
            let mut index = 0;
            queue.retain(|c| {
                let keep = index < kept_head || c.stream_identifier != stream_identifier;
                index += 1;
                if !keep {
                    n_bytes += c.user_data.len();
                    n_chunks += 1;
                }
                keep
            });
        }

        self.n_bytes -= n_bytes;
        self.queue_len -= n_chunks;

        (n_bytes, partial_len > 0)
    }

    pub(crate) fn get_num_bytes(&self) -> usize {
        self.n_bytes
    }

    pub(crate) fn len(&self) -> usize {
        self.queue_len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
