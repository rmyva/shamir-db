//! Shared allocation-count oracle for regression tests (RV-INDEX-0695AADA).
//! The allocator tracks allocations and bytes on the measuring thread only;
//! unrelated parallel test threads cannot affect the cumulative window. The
//! largest-allocation window is process-wide to observe `spawn_blocking`.
//! Mirrors `shamir-types`' `testing_alloc` oracle, extended
//! with a total-bytes API and a start/finish window that may span `.await`
//! points (single-threaded `#[tokio::test]` runs on one thread).

#![cfg(test)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

static MEASURE_LARGEST: AtomicUsize = AtomicUsize::new(0);
static LARGEST: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

impl CountingAllocator {
    fn record_allocation(size: usize) {
        // TLS teardown can invoke allocator callbacks; try_with deliberately
        // ignores access failure during teardown rather than panicking there.
        let _ = ACTIVE.try_with(|active| {
            if active.get() {
                let _ = ALLOCS.try_with(|allocs| allocs.set(allocs.get() + 1));
                let _ = BYTES.try_with(|bytes| bytes.set(bytes.get() + size));
            }
        });
    }

    fn record_largest(size: usize) {
        if MEASURE_LARGEST.load(Ordering::SeqCst) == 1 {
            LARGEST.fetch_max(size, Ordering::SeqCst);
        }
    }
}

// SAFETY: every operation forwards its arguments unchanged to `System`;
// callback bookkeeping uses non-allocating TLS Cells via `try_with` and does
// not panic, including when TLS is being destroyed.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::record_allocation(layout.size());
        // SAFETY: the caller supplies a valid nonzero layout, forwarded unchanged.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            Self::record_largest(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::record_allocation(layout.size());
        // SAFETY: the valid nonzero caller layout is forwarded unchanged.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            Self::record_largest(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr was allocated by System through this allocator and the
        // caller provides its original layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A realloc requests `new_size` bytes of fresh capacity; counting the
        // old layout would under-report growth of a large buffer.
        Self::record_allocation(new_size);
        // SAFETY: ptr/layout identify a live System allocation; new_size and
        // alignment satisfy the GlobalAlloc caller's contract. A null result
        // leaves the original allocation live and is deliberately not recorded
        // as a largest successful allocation.
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !ptr.is_null() {
            Self::record_largest(new_size);
        }
        ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

pub(crate) struct LargestWindow;

impl LargestWindow {
    pub(crate) fn start() -> Self {
        assert!(
            MEASURE_LARGEST
                .compare_exchange(0, 2, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "nested process-wide allocation window"
        );
        LARGEST.store(0, Ordering::SeqCst);
        MEASURE_LARGEST.store(1, Ordering::SeqCst);
        Self
    }

    /// Stop this process-wide measurement window.
    pub(crate) fn finish(self) -> usize {
        MEASURE_LARGEST.store(0, Ordering::SeqCst);
        LARGEST.load(Ordering::SeqCst)
    }
}

impl Drop for LargestWindow {
    fn drop(&mut self) {
        MEASURE_LARGEST.store(0, Ordering::SeqCst);
    }
}
