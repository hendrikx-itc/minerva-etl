CREATE CAST (notification.notification_store AS text)
WITH FUNCTION notification.to_char(notification.notification_store);
