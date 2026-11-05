//// Request/reply with an actor that never panics: the caller monitors the
//// actor, so a dead actor is reported as a value instead of a crash.

import gleam/erlang/process.{type Subject}

/// How long to wait for the reply.
pub type Wait {
  /// Give up after this many milliseconds.
  Within(Int)
  /// Wait until the actor replies or exits. Only for actors that are known
  /// to always reply in bounded time.
  UntilReplyOrExit
}

/// Send `make(reply_subject)` to `subject` and wait for the reply.
/// Returns `on_down` if the actor is not running or exits first, and
/// `on_timeout` if the wait runs out.
pub fn call(
  subject: Subject(message),
  make: fn(Subject(reply)) -> message,
  wait: Wait,
  on_down: reply,
  on_timeout: reply,
) -> reply {
  case process.subject_owner(subject) {
    Error(Nil) -> on_down
    Ok(pid) -> {
      let reply = process.new_subject()
      let monitor = process.monitor(pid)
      process.send(subject, make(reply))
      let selector =
        process.new_selector()
        |> process.select(reply)
        |> process.select_specific_monitor(monitor, fn(_) { on_down })
      let result = case wait {
        UntilReplyOrExit -> Ok(process.selector_receive_forever(selector))
        Within(ms) -> process.selector_receive(selector, ms)
      }
      process.demonitor_process(monitor)
      case result {
        Ok(r) -> r
        Error(Nil) -> on_timeout
      }
    }
  }
}
