#
# Regular cron jobs for the minerva-database package
#
0 4	* * *	root	[ -x /usr/bin/minerva-database_maintenance ] && /usr/bin/minerva-database_maintenance
