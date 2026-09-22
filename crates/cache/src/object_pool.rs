//! Object pool is a generic thread-safe cache pool with an RAII IO pattern.
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};

/// A trait that can reset the content of T.
pub trait Clear {
    /// clear reset the content of T to default.
    fn clear(&mut self);
}

/// Implement Clear for Vec<T> thus that, the object pool's guard can reset the content on drop.
impl<T> Clear for Vec<T> {
    fn clear(&mut self) {
        self.clear();
    }
}

/// A generic object cache / object pool.
///
/// Objects are pre-allocated at construction time. Callers obtain an object
/// via [`Cache::acquire`], which returns a [`CacheGuard`]. When the guard goes
/// out of scope the underlying object is reset via [`Clear::clear`] and then
/// returned to the pool so that it can be reused by another caller (this is
/// the `Drop` behaviour).
///
/// The pool can be resized at runtime via [`Cache::resize`].
pub struct Cache<T: Clear> {
    inner: Arc<Mutex<Inner<T>>>,
}

struct Inner<T: Clear> {
    /// Objects currently sitting in the pool, ready to be handed out.
    available: Vec<T>,
    /// Factory used to build new objects on demand.
    factory: Box<dyn Fn() -> T + Send + Sync + 'static>,
    /// Target number of objects the pool tries to keep available.
    capacity: usize,
}

impl<T: Clear> Cache<T> {
    /// Create a new cache, pre-allocating `initial` items using `factory`.
    pub fn new<F>(initial: usize, factory: F) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        let mut available = Vec::with_capacity(initial);
        for _ in 0..initial {
            available.push(factory());
        }
        Self {
            inner: Arc::new(Mutex::new(Inner {
                available,
                factory: Box::new(factory),
                capacity: initial,
            })),
        }
    }

    /// Acquire an object from the pool.
    ///
    /// If the pool is empty (all objects are checked out) a new object is
    /// created on the fly using the factory. The returned [`CacheGuard`]
    /// dereferences to `&mut T` and returns the object to the pool when
    /// dropped, after resetting its content via [`Clear::clear`].
    pub fn acquire(&self) -> CacheGuard<T> {
        let item = {
            let mut inner = self.lock();
            match inner.available.pop() {
                Some(item) => item,
                None => (inner.factory)(),
            }
        };
        CacheGuard { inner: Arc::clone(&self.inner), item: Some(item) }
    }

    /// Resize the pool so that it keeps up to `new_size` objects available.
    ///
    /// * Growing: fresh objects are created using the factory.
    /// * Shrinking: surplus *available* objects are dropped.
    ///
    /// Objects that are currently checked out are **not** affected. When they
    /// are eventually returned, they are dropped if the pool is already at
    /// capacity, so the pool never grows past `new_size`.
    pub fn resize(&self, new_size: usize) {
        let mut inner = self.lock();
        inner.capacity = new_size;
        while inner.available.len() > new_size {
            inner.available.pop();
        }
        while inner.available.len() < new_size {
            let item = (inner.factory)();
            inner.available.push(item);
        }
    }

    /// Number of objects currently available in the pool.
    pub fn available(&self) -> usize {
        self.lock().available.len()
    }

    /// The pool's current target capacity.
    pub fn capacity(&self) -> usize {
        self.lock().capacity
    }

    /// Poison-tolerant lock helper.
    fn lock(&self) -> MutexGuard<'_, Inner<T>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl<T: Clear> Clone for Cache<T> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

/// RAII guard holding a checked-out object.
///
/// Dereferences to `T`. When dropped, the object is reset via [`Clear::clear`]
/// and returned to the pool (or freed if the pool is already at capacity).
pub struct CacheGuard<T: Clear> {
    inner: Arc<Mutex<Inner<T>>>,
    item: Option<T>,
}

impl<T: Clear> CacheGuard<T> {
    /// Consume the guard and take ownership of the object *without* returning
    /// it to the pool. The content is preserved — [`Clear::clear`] is not
    /// called. Useful when the object is in a broken state and should not be
    /// reused.
    pub fn take(mut self) -> T {
        self.item.take().expect("guard already consumed")
    }
}

impl<T: Clear> Deref for CacheGuard<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        self.item.as_ref().expect("guard already consumed")
    }
}

impl<T: Clear> DerefMut for CacheGuard<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        self.item.as_mut().expect("guard already consumed")
    }
}

impl<T: Clear> Drop for CacheGuard<T> {
    fn drop(&mut self) {
        if let Some(mut item) = self.item.take() {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.available.len() < inner.capacity {
                // Reset the content before handing the object back to the pool.
                item.clear();
                inner.available.push(item);
            }
            // else: pool is at capacity, `item` is dropped here.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    /// A toy item that counts how many instances are alive.
    struct TestItem {
        value: i32,
        live: Arc<AtomicUsize>,
    }

    impl TestItem {
        fn new(live: Arc<AtomicUsize>) -> Self {
            live.fetch_add(1, Ordering::SeqCst);
            Self { value: 0, live }
        }
    }

    impl Clear for TestItem {
        fn clear(&mut self) {
            self.value = 0;
        }
    }

    impl Drop for TestItem {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn make_cache(initial: usize) -> (Cache<TestItem>, Arc<AtomicUsize>) {
        let live = Arc::new(AtomicUsize::new(0));
        let live_clone = Arc::clone(&live);
        let cache = Cache::new(initial, move || TestItem::new(Arc::clone(&live_clone)));
        (cache, live)
    }

    type LinearItem<T> = Vec<T>;

    #[test]
    fn test_linear_set() {
        let cap = 100usize;
        let cache: Cache<LinearItem<u32>> = Cache::new(cap, LinearItem::new);

        assert_eq!(cache.available(), 100usize);

        let mut v1 = cache.acquire();
        assert_eq!(cache.available(), cap - 1);

        v1.append(&mut vec![1, 2, 3]);
        drop(v1);
        assert_eq!(cache.available(), cap);

        let v2 = cache.acquire();
        assert!(v2.is_empty(), "item should be reset by Clear when returned");
        drop(v2);
    }

    #[test]
    fn test_preallocation() {
        let (cache, live) = make_cache(5);
        assert_eq!(cache.available(), 5);
        assert_eq!(cache.capacity(), 5);
        assert_eq!(live.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn test_acquire_and_release() {
        let (cache, live) = make_cache(3);
        {
            let g1 = cache.acquire();
            assert_eq!(cache.available(), 2);
            let g2 = cache.acquire();
            assert_eq!(cache.available(), 1);

            drop(g2);
            assert_eq!(cache.available(), 2);

            drop(g1);
            assert_eq!(cache.available(), 3);
        }
        // All items still alive because they're back in the pool.
        assert_eq!(live.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_acquire_beyond_capacity_creates_new() {
        let (cache, live) = make_cache(2);
        let _g1 = cache.acquire();
        let _g2 = cache.acquire();
        let _g3 = cache.acquire();
        assert_eq!(cache.available(), 0);
        // Third item was created on-demand.
        assert_eq!(live.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_grow_resize() {
        let (cache, live) = make_cache(2);
        cache.resize(5);
        assert_eq!(cache.capacity(), 5);
        assert_eq!(cache.available(), 5);
        assert_eq!(live.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn test_shrink_resize() {
        let (cache, live) = make_cache(5);
        cache.resize(2);
        assert_eq!(cache.capacity(), 2);
        assert_eq!(cache.available(), 2);
        assert_eq!(live.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_shrink_with_checked_out_items() {
        let (cache, live) = make_cache(5);
        let g1 = cache.acquire();
        let g2 = cache.acquire();
        let g3 = cache.acquire();
        assert_eq!(cache.available(), 2);

        // Shrink while three are checked out.
        cache.resize(2);
        assert_eq!(cache.available(), 2);
        assert_eq!(live.load(Ordering::SeqCst), 5); // checked-out ones unaffected

        // Return the checked-out items: the pool is already at capacity, so
        // they should be dropped, not buffered.
        drop(g1);
        drop(g2);
        drop(g3);
        assert_eq!(cache.available(), 2);
        assert_eq!(live.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_resize_to_zero() {
        let (cache, live) = make_cache(3);
        cache.resize(0);
        assert_eq!(cache.available(), 0);
        assert_eq!(cache.capacity(), 0);
        assert_eq!(live.load(Ordering::SeqCst), 0);

        // Acquiring after resize(0) still works — creates a new item.
        let _g = cache.acquire();
        assert_eq!(live.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_item_is_cleared_on_return() {
        let (cache, live) = make_cache(1);
        {
            let mut g = cache.acquire();
            g.value = 42;
        }
        assert_eq!(live.load(Ordering::SeqCst), 1);
        {
            let g = cache.acquire();
            // Same instance came back — no new allocation — and the content
            // was reset by Clear when the previous guard was dropped.
            assert_eq!(g.value, 0);
        }
    }

    #[test]
    fn test_take_removes_from_pool() {
        let (cache, live) = make_cache(2);
        let mut g = cache.acquire();
        g.value = 7;
        let _item = g.take(); // object now owned by caller, never returned, not cleared
        assert_eq!(_item.value, 7);
        assert_eq!(cache.available(), 1);

        // Remove the last object permanently too.
        let g2 = cache.acquire();
        let _item2 = g2.take();
        assert_eq!(cache.available(), 0);

        // Both objects are still alive — the caller owns them.
        assert_eq!(live.load(Ordering::SeqCst), 2);

        drop(_item);
        drop(_item2);
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_cache_drop_releases_items() {
        let live;
        {
            let (cache, l) = make_cache(3);
            live = l;
            assert_eq!(live.load(Ordering::SeqCst), 3);
            drop(cache);
        }
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_clone_shares_pool() {
        let (cache, _) = make_cache(3);
        let cache2 = cache.clone();
        let _g = cache.acquire();
        assert_eq!(cache2.available(), 2);
    }

    #[test]
    fn test_multithreaded_stress() {
        let (cache, live) = make_cache(4);

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let c = cache.clone();
                thread::spawn(move || {
                    for _ in 0..500 {
                        let mut g = c.acquire();
                        g.value += 1;
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // After all threads finish, everything has been returned.
        // Because we temporarily exceeded capacity, extras were dropped on
        // return — but the pool must end up holding exactly `capacity` items.
        assert_eq!(cache.capacity(), 4);
        assert_eq!(cache.available(), 4);
        assert_eq!(live.load(Ordering::SeqCst), 4);
    }
}
