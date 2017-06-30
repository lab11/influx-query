influx-query
============

Tools to query and download data from an InfluxDB instance.


# Setup

A configuration file for the InfluxDB instance is needed. The file should be
created as follows:

```
$ cat confs/influxdb.conf

#
# Config file for connection to influxdb.
#

# Where the influxdb instance is running.
host = HOST_DOMAIN
port = 443
protocol = https

# The database to post points to
database = DATABASE_NAME

# HTTPAuth Credentials
username = USERNAME
password = PASSWORD
```

