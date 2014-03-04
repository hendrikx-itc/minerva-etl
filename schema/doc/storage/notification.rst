Storage Class 'notification'
============================

The attribute storage class stores notifications about/from entities.
Notifications can be grouped into sets. This can be useful for e.g. logging
changes in state in the case of a workflow ticket.

The original idea for notification sets would use an existing column of the
notifications and use that directly as the primary key of the notification set.
So if there are alarm notifications coming in from an external system that
maintains its own set of alarm Ids, then these alarm Ids would become the
primary key for an alarm notification set.


+--------------+                       +-------+
| notification |                       | alarm |
+==============+                       +=======+
| id           |           +---------->| id    |
+--------------+           |           +-------+
| entity_id    |           |           | ...   |
+--------------+           |           +-------+
| timestamp    |           |
+--------------+           |
| alarm_id     |-----------+
+--------------+
| ...          |
+--------------+


Now, it seems that this would not be a good match for all uses of notification
sets. For example when there is no apparent grouping column available in the
notifications, it would be more logical to use a link table between the
notifications and the notification sets.


+--------------+            +-----------------+               +--------+
| notification |            |      link       |               | ticket |
+==============+            +=================+               +========+
| id           |<-----------| notification_id |       +------>| id     |
+--------------+            +-----------------+       |       +--------+
| entity_id    |            | set_id          |-------+       | ...    |
+--------------+            +-----------------+               +--------+
| timestamp    |
+--------------+
| ...          |
+--------------+


Notification Store Example
--------------------------

A data table of a Notification Store is structured as follows:

+-----------+--------------------------+
|   name    |           type           |
+===========+==========================+
| id        | integer                  |
+-----------+--------------------------+
| entity_id | integer                  |
+-----------+--------------------------+
| timestamp | timestamp with time zone |
+-----------+--------------------------+
| custom_1  | any                      |
+-----------+--------------------------+
| custom_2  | any                      |
+-----------+--------------------------+
| custom_3  | any                      |
+-----------+--------------------------+
| ...       | ...                      |
+-----------+--------------------------+


Notification Set Store
----------------------

+----------------------+------------------+
|         name         |       type       |
+======================+==================+
| id                   | integer not null |
+----------------------+------------------+
| notificationstore_id | integer not null |
+----------------------+------------------+
| set_column           | name not null    |
+----------------------+------------------+


Notification Set Store Example
------------------------------

A data table of a Notification Set Store is structured as follows:


+------+---------+
| name |  type   |
+======+=========+
| id   | integer |
+------+---------+

