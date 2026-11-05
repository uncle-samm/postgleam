//// Receive LISTEN/NOTIFY notifications as messages.
////
//// A listener owns a dedicated connection. Every notification on a channel
//// it listens to is sent to the `Subject` given to `start`.
////
//// ```gleam
//// import gleam/erlang/process
//// import postgleam
//// import postgleam/listener
////
//// pub fn main() {
////   let assert Ok(cfg) = config.from_url("postgres://app:secret@localhost/app")
////   let inbox = process.new_subject()
////   let assert Ok(l) = listener.start(cfg, inbox)
////   let assert Ok(Nil) = listener.listen(l, "jobs")
////
////   let assert Ok(conn) = postgleam.connect(cfg)
////   let assert Ok(Nil) = postgleam.notify(conn, "jobs", "42")
////
////   let assert Ok(notification) = process.receive(inbox, 1000)
////   // notification.channel == "jobs", notification.payload == "42"
//// }
//// ```
////
//// If the connection drops, the listener reconnects (retrying with
//// backoff) and listens to its channels again. Notifications sent while it
//// was disconnected are lost; PostgreSQL does not keep them.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/set.{type Set}
import postgleam/config.{type Config}
import postgleam/connection.{type ConnectionState}
import postgleam/error.{type Error}
import postgleam/internal/call
import postgleam/notifications

/// A notification: the channel, the payload, and the server process id of
/// the session that sent it.
pub type Notification =
  connection.Notification

/// A running listener.
pub opaque type Listener {
  Listener(subject: Subject(Message), config: Config)
}

/// Messages the listener process handles.
pub opaque type Message {
  Listen(channel: String, reply: Subject(Result(Nil, Error)))
  Unlisten(channel: String, reply: Subject(Result(Nil, Error)))
  Poll
  Reconnect
  Stop(reply: Subject(Nil))
}

type State {
  State(
    conn: Option(ConnectionState),
    config: Config,
    self: Subject(Message),
    deliver_to: Subject(Notification),
    channels: Set(String),
    /// Delay before the next reconnect attempt (ms).
    backoff: Int,
  )
}

/// How long one poll waits for data before the listener looks at its
/// mailbox again. This bounds how long `listen`, `unlisten` and `stop` wait.
const poll_interval = 100

const min_backoff = 100

const max_backoff = 30_000

/// Start a listener with its own connection. Notifications go to
/// `deliver_to`. The listener is linked to the calling process.
pub fn start(
  config: Config,
  deliver_to: Subject(Notification),
) -> Result(Listener, Error) {
  // A failed initialiser can only return a string; the typed error comes
  // back on this subject.
  let errors = process.new_subject()
  let started =
    actor.new_with_initialiser(config.connect_timeout + 1000, fn(subject) {
      case connection.connect(config) {
        Ok(conn) -> {
          process.send(subject, Poll)
          State(
            conn: Some(conn),
            config:,
            self: subject,
            deliver_to:,
            channels: set.new(),
            backoff: min_backoff,
          )
          |> actor.initialised
          |> actor.returning(subject)
          |> Ok
        }
        Error(e) -> {
          process.send(errors, e)
          Error(error.to_string(e))
        }
      }
    })
    |> actor.on_message(handle_message)
    |> actor.start
  case started {
    Ok(started) -> Ok(Listener(subject: started.data, config:))
    Error(actor.InitFailed(reason)) ->
      case process.receive(errors, 0) {
        Ok(e) -> Error(e)
        Error(Nil) -> Error(error.ConnectionError(reason))
      }
    Error(actor.InitTimeout) ->
      Error(error.ConnectionError("Listener initialization timed out"))
    Error(actor.InitExited(_)) ->
      Error(error.ConnectionError("Listener process exited during init"))
  }
}

/// Start listening on `channel`. Channel names are used as given (they are
/// quoted, so they are case-sensitive).
pub fn listen(listener: Listener, channel: String) -> Result(Nil, Error) {
  request(listener, Listen(channel, _))
}

/// Stop listening on `channel`.
pub fn unlisten(listener: Listener, channel: String) -> Result(Nil, Error) {
  request(listener, Unlisten(channel, _))
}

/// Stop the listener and close its connection.
pub fn stop(listener: Listener) -> Nil {
  call.call(
    listener.subject,
    Stop,
    call.Within(listener.config.timeout + poll_interval + 1000),
    Nil,
    Nil,
  )
}

fn request(
  listener: Listener,
  make: fn(Subject(Result(Nil, Error))) -> Message,
) -> Result(Nil, Error) {
  let config = listener.config
  call.call(
    listener.subject,
    make,
    call.Within(config.timeout + config.connect_timeout + poll_interval + 1000),
    Error(error.ConnectionError("Listener process is not running")),
    Error(error.TimeoutError),
  )
}

fn handle_message(state: State, msg: Message) -> actor.Next(State, Message) {
  case msg {
    Listen(channel, reply) -> {
      let #(result, state) =
        with_connection(state, notifications.listen(
          _,
          channel,
          state.config.timeout,
        ))
      let state = case result {
        Ok(Nil) -> State(..state, channels: set.insert(state.channels, channel))
        Error(_) -> state
      }
      process.send(reply, result)
      actor.continue(state)
    }

    Unlisten(channel, reply) -> {
      let #(result, state) =
        with_connection(state, notifications.unlisten(
          _,
          channel,
          state.config.timeout,
        ))
      let state = case result {
        Ok(Nil) -> State(..state, channels: set.delete(state.channels, channel))
        Error(_) -> state
      }
      process.send(reply, result)
      actor.continue(state)
    }

    Poll ->
      case state.conn {
        // Disconnected: a Reconnect is already scheduled.
        None -> actor.continue(state)
        Some(conn) ->
          case connection.wait_for_notifications(conn, poll_interval) {
            Ok(#(received, conn)) -> {
              list.each(received, process.send(state.deliver_to, _))
              process.send(state.self, Poll)
              actor.continue(State(..state, conn: Some(conn)))
            }
            Error(_) -> actor.continue(disconnected(state, conn))
          }
      }

    Reconnect ->
      case state.conn {
        Some(_) -> actor.continue(state)
        None -> actor.continue(reconnect(state))
      }

    Stop(reply) -> {
      case state.conn {
        Some(conn) -> connection.disconnect(conn)
        None -> Nil
      }
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

/// Run a LISTEN/UNLISTEN. If the connection is down (or goes down), report
/// success: the caller then updates the channel set, which is applied when
/// the listener reconnects.
fn with_connection(
  state: State,
  f: fn(ConnectionState) -> Result(ConnectionState, Error),
) -> #(Result(Nil, Error), State) {
  case state.conn {
    None -> #(Ok(Nil), state)
    Some(conn) ->
      case f(conn) {
        Ok(conn) -> #(Ok(Nil), State(..state, conn: Some(conn)))
        // The server rejected the statement; the connection is still fine.
        Error(error.PgError(..) as e) -> #(Error(e), state)
        Error(_) -> #(Ok(Nil), disconnected(state, conn))
      }
  }
}

fn disconnected(state: State, conn: ConnectionState) -> State {
  connection.disconnect(conn)
  process.send_after(state.self, state.backoff, Reconnect)
  State(..state, conn: None)
}

fn reconnect(state: State) -> State {
  let attempt = {
    use conn <- result.try(connection.connect(state.config))
    let listened =
      set.to_list(state.channels)
      |> list.try_fold(conn, fn(conn, channel) {
        notifications.listen(conn, channel, state.config.timeout)
      })
    case listened {
      Ok(conn) -> Ok(conn)
      Error(e) -> {
        connection.disconnect(conn)
        Error(e)
      }
    }
  }
  case attempt {
    Ok(conn) -> {
      process.send(state.self, Poll)
      State(..state, conn: Some(conn), backoff: min_backoff)
    }
    Error(_) -> {
      let backoff = int.min(state.backoff * 2, max_backoff)
      process.send_after(state.self, backoff, Reconnect)
      State(..state, backoff:)
    }
  }
}
