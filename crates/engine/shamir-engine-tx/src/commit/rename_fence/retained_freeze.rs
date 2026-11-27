use std::any::Any;

/// Opaque holder for the shell's rename freeze. The `tx` layer only keeps and
/// drops it (dropping runs the freeze's own `Drop`); the shell puts the
/// concrete value in and takes it back out by type. Never called through the
/// trait object and never on a commit, read or drain path.
///
/// Recorded debt: remove once the freeze type itself lives below `tx`.
pub struct RetainedFreeze(Box<dyn Any + Send + Sync>);

impl RetainedFreeze {
    pub fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self(Box::new(value))
    }

    /// Take the value back by type; `Err(self)` hands the holder back
    /// unchanged on a type mismatch so the caller can fail closed.
    pub fn into_inner<T: Any + Send + Sync>(self) -> Result<T, Self> {
        match self.0.downcast::<T>() {
            Ok(value) => Ok(*value),
            Err(other) => Err(Self(other)),
        }
    }

    pub fn as_ref<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.0.downcast_ref::<T>()
    }
}
