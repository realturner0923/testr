import React, { useEffect, useState } from "react";

const WEBSOCKET_URL = "ws://192.168.0.22:3003";

function App() {
  const [notifications, setNotifications] = useState([]);
  const [connectionStatus, setConnectionStatus] =
    useState("connecting");

  useEffect(() => {
    const socket = new WebSocket(WEBSOCKET_URL);

    socket.onopen = () => {
      console.log("WebSocket connected");
      setConnectionStatus("connected");
    };

    socket.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);

        console.log("Received:", message);

        if (message.type === "connected") {
          return;
        }

        if (
          message.type ===
          "notification.create"
        ) {
          setNotifications((current) => [
            message,
            ...current
          ]);
        }
      } catch (error) {
        console.error(
          "Failed to parse WebSocket message:",
          error
        );
      }
    };

    socket.onerror = (error) => {
      console.error(
        "WebSocket error:",
        error
      );

      setConnectionStatus("error");
    };

    socket.onclose = () => {
      console.log("WebSocket disconnected");
      setConnectionStatus("disconnected");
    };

    return () => {
      socket.close();
    };
  }, []);

  return (
    <main className="app">
      <header className="header">
        <div>
          <h1>Notification Center</h1>

          <p className="subtitle">
            Distributed notification platform
          </p>
        </div>

        <ConnectionStatus
          status={connectionStatus}
        />
      </header>

      <section className="notifications">
        <div className="section-header">
          <h2>Notifications</h2>

          <span>
            {notifications.length}
          </span>
        </div>

        {notifications.length === 0 ? (
          <EmptyState />
        ) : (
          <div className="notification-list">
            {notifications.map(
              (notification, index) => (
                <NotificationCard
                  key={`${notification.timestamp}-${index}`}
                  notification={notification}
                />
              )
            )}
          </div>
        )}
      </section>
    </main>
  );
}

function ConnectionStatus({ status }) {
  const labels = {
    connecting: "Connecting...",
    connected: "Connected",
    disconnected: "Disconnected",
    error: "Connection error"
  };

  return (
    <div
      className={`connection ${status}`}
    >
      <span className="dot" />

      {labels[status]}
    </div>
  );
}

function NotificationCard({
  notification
}) {
  return (
    <article className="notification">
      <div className="notification-icon">
        🚀
      </div>

      <div className="notification-content">
        <h3>{notification.title}</h3>

        <p>{notification.message}</p>

        {notification.repository && (
          <div className="repository">
            {notification.repository}
          </div>
        )}

        {notification.sender && (
          <div className="sender">
            pushed by {notification.sender}
          </div>
        )}

        <time>
          {new Date(
            notification.timestamp
          ).toLocaleString()}
        </time>
      </div>
    </article>
  );
}

function EmptyState() {
  return (
    <div className="empty">
      <div className="empty-icon">
        🔔
      </div>

      <h3>No notifications yet</h3>

      <p>
        Push a commit to GitHub and watch it
        appear here.
      </p>
    </div>
  );
}

export default App;
