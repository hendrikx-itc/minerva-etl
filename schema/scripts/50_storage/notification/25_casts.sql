SET search_path = notification, pg_catalog;


CREATE CAST (notification.notificationstore AS text)
WITH FUNCTION notification.to_char(notification.notificationstore);
