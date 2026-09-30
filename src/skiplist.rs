use std::{cmp::Ordering, ptr::NonNull};

const MAX_LEVEL: usize = 32;

type Link<T> = Option<NonNull<Node<T>>>;

struct Node<T> {
    value: Option<T>,
    next: Vec<Link<T>>,
}

pub struct SkipList<T> {
    head: NonNull<Node<T>>,
    level: usize,
    len: usize,
    rng: u64,
}

impl<T: Ord> SkipList<T> {
    pub fn new() -> Self {
        let head = Box::new(Node {
            value: None,
            next: vec![None; MAX_LEVEL],
        });
        SkipList {
            head: NonNull::from(Box::leak(head)),
            level: 0,
            len: 0,
            rng: 0x2545_F491_4F6C_DD1D,
        }
    }

    fn find_predecessors(&self, target: &T) -> Vec<NonNull<Node<T>>> {
        let mut predecessors = vec![self.head; MAX_LEVEL];
        let mut cur = self.head;
        for lvl in (0..self.level).rev() {
            loop {
                let next = unsafe { cur.as_ref().next[lvl] };
                let Some(p) = next else {
                    break; // next is None, this is prev
                };
                if unsafe { p.as_ref().value.as_ref().unwrap() } >= target {
                    break; // next value exceeds, this is prev
                }
                cur = p; // move horizontally ahead on the same level
            }
            predecessors[lvl] = cur
        }
        predecessors
    }

    // we don't compare with cur's value because the head has no value
    // so it will check next value. so the next node must check its next value
    pub fn contains(&self, target: &T) -> bool {
        let mut cur = self.head;
        for lvl in (0..=self.level).rev() {
            loop {
                let next = unsafe { cur.as_ref().next[lvl] };
                let Some(p) = next else {
                    break;
                };
                match unsafe { p.as_ref().value.as_ref().unwrap() }.cmp(target) {
                    Ordering::Less => cur = p, // move horizontally on the same level
                    Ordering::Equal => return true,
                    Ordering::Greater => break, // move vertically down 1 level
                };
            }
        }
        false
    }

    pub fn insert(&mut self, value: T) {
        let predecesors = self.find_predecessors(&value);

        if let Some(p) = unsafe { predecesors[0].as_ref().next[0] } {
            if unsafe { p.as_ref().value.as_ref().unwrap() == &value } {
                return;
            }
        }

        let level = self.random_level();
        if level > self.level {
            self.level = level;
        }

        let mut node = Node {
            value: Some(value),
            next: vec![None; level + 1],
        };
        // collect old links: node.next = prev.next
        for lvl in 0..level + 1 {
            node.next[lvl] = unsafe { predecesors[lvl].as_ref().next[lvl] };
        }

        let boxed = Box::new(node);
        let ptr = NonNull::from(Box::leak(boxed));

        // link with new node: prev.next = node
        for (i, mut pred) in predecesors.into_iter().enumerate().take(level + 1) {
            unsafe { pred.as_mut().next[i] = Some(ptr) }
        }
        self.len += 1;
    }

    pub fn remove(&mut self, target: &T) -> bool {
        let predecessors = self.find_predecessors(target);

        let Some(p) = (unsafe { predecessors[0].as_ref().next[0] }) else {
            return false;
        };

        if unsafe { p.as_ref().value.as_ref().unwrap() } != target {
            return false;
        }

        let level = unsafe { p.as_ref().next.len() - 1 };

        // unlink the predecessors with this node
        for (lvl, mut pred) in predecessors.into_iter().enumerate().take(level + 1) {
            let new_next = unsafe { p.as_ref().next[lvl] };
            unsafe { pred.as_mut().next[lvl] = new_next }; // prev.next = node.next
        }
        while self.level > 0 && unsafe { self.head.as_ref().next[self.level] }.is_none() {
            self.level -= 1;
        }
        unsafe {
            drop(Box::from_raw(p.as_ptr()));
        }
        self.len -= 1;
        true
    }
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn random_level(&mut self) -> usize {
        let mut level = 0;
        while level < MAX_LEVEL - 1 && self.next_rand() & 1 == 1 {
            level += 1;
        }
        level
    }
}
