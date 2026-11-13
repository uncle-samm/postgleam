//// Cleanup that still runs when a callback panics.

/// Run `f`. If it panics, run `cleanup` and re-raise the panic unchanged.
/// `cleanup` does not run when `f` returns normally.
@external(erlang, "postgleam_ffi", "protect")
pub fn on_panic(f: fn() -> a, cleanup: fn() -> Nil) -> a
