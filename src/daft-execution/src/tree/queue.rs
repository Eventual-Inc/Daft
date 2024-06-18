use std::collections::{BinaryHeap, VecDeque};

#[derive(Debug, Clone)]
pub struct OrderedDequeItem<T: Clone> {
    pub item: T,
    pub seqno: i64,
}

impl<T: Clone> OrderedDequeItem<T> {
    pub fn new(item: T, seqno: i64) -> Self {
        Self { item, seqno }
    }
}

impl<T: Clone> PartialEq for OrderedDequeItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.seqno == other.seqno
    }
}

impl<T: Clone> Eq for OrderedDequeItem<T> {}

impl<T: Clone> PartialOrd for OrderedDequeItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Clone> Ord for OrderedDequeItem<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seqno.cmp(&other.seqno)
    }
}

impl<T: Clone> From<(usize, T)> for OrderedDequeItem<T> {
    fn from(value: (usize, T)) -> Self {
        let (seqno, item) = value;
        Self::new(item, -(seqno as i64))
    }
}

/// An ordered queue that maintains ordering based on provided sequence numbers, rather than FIFO insertion order.
#[derive(Debug)]
pub struct OrderedDeque<T: Clone> {
    queue: VecDeque<OrderedDequeItem<T>>,
    last_seqno: i64,
    buffer: BinaryHeap<OrderedDequeItem<T>>,
}

impl<T: Clone> OrderedDeque<T> {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            last_seqno: -1,
            buffer: BinaryHeap::new(),
        }
    }

    pub fn push_back(&mut self, item: impl Into<OrderedDequeItem<T>>) {
        let item: OrderedDequeItem<T> = item.into();
        self.buffer.push(item);
        while let Some(item) = self.buffer.peek()
            && -item.seqno == self.last_seqno + 1
        {
            self.last_seqno = -item.seqno;
            self.queue.push_back(self.buffer.pop().unwrap());
        }
    }

    pub fn pop_front(&mut self) -> Option<OrderedDequeItem<T>> {
        self.queue.pop_front()
    }

    pub fn front(&self) -> Option<&OrderedDequeItem<T>> {
        self.queue.front()
    }

    pub fn num_ready(&self) -> usize {
        self.queue.len()
    }

    pub fn num_buffered(&self) -> usize {
        self.buffer.len()
    }

    pub fn len(&self) -> usize {
        self.num_ready() + self.num_buffered()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Clone, I: Iterator<Item = OrderedDequeItem<T>>> From<I> for OrderedDeque<T> {
    fn from(value: I) -> Self {
        let mut deque = Self::new();
        for item in value {
            deque.push_back(item);
        }
        deque
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_error::DaftResult;

    use super::{OrderedDeque, OrderedDequeItem};

    /// Test ordering semantics.
    #[test]
    fn ordering() -> DaftResult<()> {
        let mut queue = OrderedDeque::<&str>::new();

        // Add 4th item from front.
        queue.push_back((3, "a"));
        assert_eq!(queue.len(), 1);
        assert!(!queue.is_empty());
        // Should be buffered since we're missing the first three sequence numbers.
        assert_eq!(queue.num_buffered(), 1);
        assert_eq!(queue.num_ready(), 0);
        assert_matches!(queue.front(), None);
        assert_matches!(queue.pop_front(), None);

        // Add 5th item from front.
        queue.push_back((4, "b"));
        assert_eq!(queue.len(), 2);
        assert!(!queue.is_empty());
        // Should be buffered since we're missing the first three sequence numbers.
        assert_eq!(queue.num_buffered(), 2);
        assert_eq!(queue.num_ready(), 0);
        assert_matches!(queue.front(), None);
        assert_matches!(queue.pop_front(), None);

        // Add first item in sequence.
        let item = (0, "c");
        queue.push_back(item.clone());
        assert_eq!(queue.len(), 3);
        assert!(!queue.is_empty());
        // 3rd and 4th items in sequence should still be buffered since we're missing the second item in sequence.
        assert_eq!(queue.num_buffered(), 2);
        // First item in sequence is ready.
        assert_eq!(queue.num_ready(), 1);
        let front_item: OrderedDequeItem<&str> = item.into();
        // First item in sequence is at front of ready queue.
        assert_matches!(queue.front(), Some(front_item));
        // Pop first item in sequence from front of ready queue.
        assert_matches!(queue.pop_front(), Some(front_item));
        assert_eq!(queue.len(), 2);
        // After popping, num_ready should be set back to 0.
        assert_eq!(queue.num_ready(), 0);

        // Add second item in sequence.
        let item = (1, "d");
        queue.push_back(item.clone());
        assert_eq!(queue.len(), 3);
        assert!(!queue.is_empty());
        assert_eq!(queue.num_buffered(), 2);
        // Ordered queue should remember that the first item in the sequence was observed, even though we've popped it
        // out of the queue. So, the second item should be in the ready set.
        assert_eq!(queue.num_ready(), 1);
        // Second item should be new front item.
        let front_item = OrderedDequeItem::from((1, "d"));
        assert_matches!(queue.front(), Some(front_item));

        // Add third item in sequence.
        let item = (2, "e");
        queue.push_back(item.clone());
        assert_eq!(queue.len(), 4);
        assert!(!queue.is_empty());
        // We should have items 1, 2, 3, 4 in the queue and remember that we've already observed (and popped) items 0,
        // so after adding item 2, all 4 items should be considered ready with no items buffered.
        assert_eq!(queue.num_buffered(), 0);
        assert_eq!(queue.num_ready(), 4);
        // Item 2 should still be front item.
        assert_matches!(queue.front(), Some(front_item));

        // Consume items from queue in order.
        // First one should be (1, "d"), defined above.
        assert_matches!(queue.pop_front(), Some(front_item));
        let front_item = OrderedDequeItem::from((2, "e"));
        assert_matches!(queue.pop_front(), Some(front_item));
        let front_item = OrderedDequeItem::from((3, "a"));
        assert_matches!(queue.pop_front(), Some(front_item));
        let front_item = OrderedDequeItem::from((4, "b"));
        assert_matches!(queue.pop_front(), Some(front_item));
        assert_matches!(queue.pop_front(), None);
        Ok(())
    }
}
