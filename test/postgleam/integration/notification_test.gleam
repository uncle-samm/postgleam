import postgleam/config
import postgleam/connection
import postgleam/notifications

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

pub fn listen_unlisten_test() {
  let cfg = test_config()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(state) = notifications.listen(state, "test_channel", cfg.timeout)
  let assert Ok(state) = notifications.unlisten(state, "test_channel", cfg.timeout)

  connection.disconnect(state)
}

pub fn notify_test() {
  let cfg = test_config()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(state) = notifications.listen(state, "test_chan", cfg.timeout)
  let assert Ok(state) = notifications.notify(state, "test_chan", "hello", cfg.timeout)

  connection.disconnect(state)
}

pub fn cross_connection_notify_test() {
  let cfg = test_config()
  // Listener connection
  let assert Ok(listener) = connection.connect(cfg)
  let assert Ok(listener) = notifications.listen(listener, "cross_test", cfg.timeout)

  // Sender connection
  let assert Ok(sender) = connection.connect(cfg)
  let assert Ok(sender) = notifications.notify(sender, "cross_test", "world", cfg.timeout)

  // Check for notifications on listener
  let assert Ok(#(_notifs, listener)) =
    notifications.receive_notifications(listener, cfg.timeout)

  connection.disconnect(listener)
  connection.disconnect(sender)
}
