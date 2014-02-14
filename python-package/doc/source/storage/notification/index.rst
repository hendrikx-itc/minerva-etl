:mod:`minerva.storage.notification` --- notification storage class
==================================================================

This package provides the notification storage class. The notification storage
class is used for storing fixed format 'messages' without a fixed interval
size, such as SNMP traps, log records, and alarm notifications.

Notifications are records with 2 parts:

- A standard part containing the fields id, entity_id and timestamp
- A custom part containing the data specific to a certain type of notification

An example of a nsNotifyStart notification:


+------+-----------+------------------------+------------------------+
|  id  | entity_id |       timestamp        |          oid           |
+======+===========+========================+========================+
| 3047 | 142       | 2014-02-14 11:02:45+01 | 1.3.6.1.4.1.8072.4.0.1 |
+------+-----------+------------------------+------------------------+


Next to notifications there is support for grouping notifications that relate
to eachother in some way, such as state changes of an alarm. Such a group of
notifications is called a Notification Set. A Notification Set can be seen as
an aggregation of Notifications.

An example of a Notification Set for an alarm:


alarm notifications

+------+-----------+------------------------+-----------+-------------+
|  id  | entity_id |       timestamp        | alarm_id  | event_type  |
+======+===========+========================+===========+=============+
| 3047 | 142       | 2014-02-14 11:02:45+01 | X_0010013 | raise       |
+------+-----------+------------------------+-----------+-------------+
| 3048 | 142       | 2014-02-14 11:18:11+01 | X_0010013 | acknowledge |
+------+-----------+------------------------+-----------+-------------+
| 3048 | 142       | 2014-02-14 13:25:03+01 | X_0010013 | clear       |
+------+-----------+------------------------+-----------+-------------+

alarm notification set

+-----------+------------------------+------------------------+
|    id     |         start          |          end           |
+===========+========================+========================+
| X_0010013 | 2014-02-14 11:02:45+01 | 2014-02-14 13:25:03+01 |
+-----------+------------------------+------------------------+


.. automodule:: minerva.storage.notification
    :members:
    :undoc-members:
