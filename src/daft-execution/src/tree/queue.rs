use std::collections::{BinaryHeap, VecDeque};

#[derive(Debug, Clone)]
pub struct OrderedDequeItem<T: Clone> {
    pub item: T,
    pub seqno: i64,
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
        Self {
            item,
            seqno: -(seqno as i64),
        }
    }
}

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
