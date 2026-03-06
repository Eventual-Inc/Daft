import { createContext, useCallback, useContext, useState } from "react";

export type NotificationSettings = {
  onQueryStart: boolean;
  onQueryEnd: boolean;
  setOnQueryStart: (newVal: boolean) => void;
  setOnQueryEnd: (newVal: boolean) => void;
};

export const NotificationsContext = createContext<NotificationSettings>({
  onQueryStart: false,
  onQueryEnd: false,
  setOnQueryStart: () => {},
  setOnQueryEnd: () => {},
});

// ---------------------- Hooks ---------------------- //

// Hook to fetch all queries
export function useNotifications() {
  const notifications = useContext(NotificationsContext);
  return notifications;
}

// ---------------------- Provider ---------------------- //

async function requestNotificationPermission(): Promise<boolean> {
  if (!("Notification" in window)) {
    console.warn("This browser does not support notifications");
    return false;
  }
  if (Notification.permission === "granted") {
    return true;
  }
  if (Notification.permission === "denied") {
    console.warn("Notification permission was previously denied");
    return false;
  }
  const result = await Notification.requestPermission();
  return result === "granted";
}

export function NotificationsProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const [onQueryStart, setOnQueryStartRaw] = useState(false);
  const [onQueryEnd, setOnQueryEndRaw] = useState(false);

  const setOnQueryStart = useCallback((newVal: boolean) => {
    if (newVal) {
      requestNotificationPermission().then(granted => {
        setOnQueryStartRaw(granted);
      });
    } else {
      setOnQueryStartRaw(false);
    }
  }, []);

  const setOnQueryEnd = useCallback((newVal: boolean) => {
    if (newVal) {
      requestNotificationPermission().then(granted => {
        setOnQueryEndRaw(granted);
      });
    } else {
      setOnQueryEndRaw(false);
    }
  }, []);

  return (
    <NotificationsContext.Provider
      value={{ onQueryStart, onQueryEnd, setOnQueryStart, setOnQueryEnd }}
    >
      {children}
    </NotificationsContext.Provider>
  );
}
