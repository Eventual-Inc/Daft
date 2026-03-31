use std::fmt;

/// A compact bitmask representation of a set of relation IDs (up to 32).
///
/// All set operations (union, intersection, membership, subset test) are O(1).
/// Iteration over set members is O(popcount) via bit manipulation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct RelationSet(u32);

#[allow(dead_code)]
impl RelationSet {
    pub const EMPTY: Self = Self(0);

    pub fn singleton(id: usize) -> Self {
        debug_assert!(id < 32, "RelationSet supports at most 32 relations");
        Self(1 << id)
    }

    /// Creates a set containing IDs 0..n.
    pub fn from_range(n: usize) -> Self {
        debug_assert!(n <= 32, "RelationSet supports at most 32 relations");
        if n == 32 {
            Self(u32::MAX)
        } else {
            Self((1u32 << n) - 1)
        }
    }

    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    pub fn difference(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    pub fn contains(self, id: usize) -> bool {
        debug_assert!(id < 32, "RelationSet supports at most 32 relations");
        (self.0 >> id) & 1 == 1
    }

    pub fn is_subset_of(self, other: Self) -> bool {
        self.0 & other.0 == self.0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    pub fn remove(self, id: usize) -> Self {
        debug_assert!(id < 32, "RelationSet supports at most 32 relations");
        Self(self.0 & !(1 << id))
    }

    /// Raw bitmask. Useful for canonical ordering (e.g. `left.bits() < right.bits()`).
    pub fn bits(self) -> u32 {
        self.0
    }

    pub fn iter(self) -> RelationSetIter {
        RelationSetIter(self.0)
    }

    /// Enumerates all non-empty proper subsets of this set.
    pub fn subsets(self) -> SubsetIter {
        // Initialize current one step ahead so the first call to next() yields
        // the largest proper subset directly, without needing a `done` flag or
        // a skip-if-full check.
        SubsetIter {
            full: self.0,
            current: self.0.wrapping_sub(1) & self.0,
        }
    }
}

/// Iterates over the set bits (relation IDs) in a `RelationSet`.
pub(super) struct RelationSetIter(u32);

impl Iterator for RelationSetIter {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            let id = self.0.trailing_zeros() as usize;
            self.0 &= self.0 - 1; // Clear lowest set bit
            Some(id)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.0.count_ones() as usize;
        (n, Some(n))
    }
}

impl ExactSizeIterator for RelationSetIter {}

/// Enumerates all non-empty proper subsets of a bitmask.
///
/// Uses the identity: `next = (current - 1) & full` to walk subsets in descending order.
pub(super) struct SubsetIter {
    full: u32,
    current: u32,
}

impl Iterator for SubsetIter {
    type Item = RelationSet;

    fn next(&mut self) -> Option<RelationSet> {
        if self.current == 0 {
            return None;
        }
        let result = self.current;
        self.current = self.current.wrapping_sub(1) & self.full;
        Some(RelationSet(result))
    }
}

impl fmt::Debug for RelationSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for RelationSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        let mut first = true;
        for id in self.iter() {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{id}")?;
            first = false;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let s = RelationSet::EMPTY;
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert_eq!(s.iter().collect::<Vec<usize>>(), Vec::<usize>::new());
    }

    #[test]
    fn test_singleton() {
        let s = RelationSet::singleton(3);
        assert!(!s.is_empty());
        assert_eq!(s.len(), 1);
        assert!(s.contains(3));
        assert!(!s.contains(0));
        assert_eq!(s.iter().collect::<Vec<usize>>(), vec![3]);
    }

    #[test]
    fn test_from_range() {
        let s = RelationSet::from_range(4);
        assert_eq!(s.len(), 4);
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_union_intersection_difference() {
        let a = RelationSet::singleton(1).union(RelationSet::singleton(3));
        let b = RelationSet::singleton(2).union(RelationSet::singleton(3));

        assert_eq!(a.union(b).iter().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(a.intersection(b).iter().collect::<Vec<_>>(), vec![3]);
        assert_eq!(a.difference(b).iter().collect::<Vec<_>>(), vec![1]);
        assert_eq!(b.difference(a).iter().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn test_is_subset_of() {
        let a = RelationSet::singleton(1);
        let b = RelationSet::singleton(1).union(RelationSet::singleton(2));
        assert!(a.is_subset_of(b));
        assert!(!b.is_subset_of(a));
        assert!(RelationSet::EMPTY.is_subset_of(a));
    }

    #[test]
    fn test_remove() {
        let s = RelationSet::from_range(3);
        let r = s.remove(1);
        assert_eq!(r.iter().collect::<Vec<_>>(), vec![0, 2]);
        assert!(!r.contains(1));
    }

    #[test]
    fn test_subsets() {
        // Set {0, 1, 2} has 6 non-empty proper subsets
        let s = RelationSet::from_range(3);
        let subsets: Vec<RelationSet> = s.subsets().collect();
        assert_eq!(subsets.len(), 6);

        // Every subset should be non-empty and a proper subset
        for sub in &subsets {
            assert!(!sub.is_empty());
            assert_ne!(*sub, s);
            assert!(sub.is_subset_of(s));
        }

        // No duplicates
        let mut unique = subsets.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), subsets.len());
    }

    #[test]
    fn test_subsets_pair() {
        // Set {0, 1} has 2 non-empty proper subsets: {0} and {1}
        let s = RelationSet::singleton(0).union(RelationSet::singleton(1));
        assert_eq!(s.subsets().count(), 2);
    }

    #[test]
    fn test_subsets_singleton() {
        // A singleton has no non-empty proper subsets
        let s = RelationSet::singleton(5);
        assert_eq!(s.subsets().count(), 0);
    }

    #[test]
    fn test_display() {
        let s = RelationSet::singleton(0)
            .union(RelationSet::singleton(2))
            .union(RelationSet::singleton(5));
        assert_eq!(format!("{s}"), "{0, 2, 5}");
        assert_eq!(format!("{}", RelationSet::EMPTY), "{}");
    }

    #[test]
    fn test_iter_exact_size() {
        let s = RelationSet::from_range(5);
        let iter = s.iter();
        assert_eq!(iter.len(), 5);
    }
}
