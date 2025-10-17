import { createContext, useContext, useState } from "react";

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

export function NotificationsProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const [onQueryStart, setOnQueryStart] = useState(false);
  const [onQueryEnd, setOnQueryEnd] = useState(false);

  return (
    <NotificationsContext.Provider
      value={{ onQueryStart, onQueryEnd, setOnQueryStart, setOnQueryEnd }}
    >
      {children}
    </NotificationsContext.Provider>
  );
}
