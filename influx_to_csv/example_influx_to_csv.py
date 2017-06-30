#! /usr/bin/env python3

import os
import sys

try:
    import configparser
except ImportError:
    print('Could not import configparser library')
    print('sudo pip3 install configparser')
    sys.exit(1)

# import influx to csv library
sys.path.insert(0, '../influx_to_csv/')
from influx_to_csv import generate_csv

# check for influxdb config
if not os.path.isfile('../confs/influxdb.conf'):
    print('Error: need a valid influxdb.conf file')
    sys.exit(1)

# hack to read sectionless ini files from: 
#   http://stackoverflow.com/a/25493615/4422122
influxdb_config = configparser.ConfigParser()
with open('../confs/influxdb.conf', 'r') as f:
    config_str = '[global]\n' + f.read()
influxdb_config.read_string(config_str)
config = influxdb_config['global']

# query for average RSSI values
select_operation = 'MEAN(value)'
from_measurement = 'rssi'
where_tag_list = {
    'device_class': ['BLEPacket'],
    'device_id': ['c098e57000cd', 'c098e57000ce'],
}
group_list = ['device_id', 'time(1m) fill(0)']
begin_time = '04-02-2017 00:00:00 US/Eastern'
end_time = '04-02-2017 01:00:00 US/Eastern'
out_filename = 'raw_data/minutelyRSSI'
generate_csv(config, select_operation, from_measurement, where_tag_list, group_list, begin_time, end_time, out_filename)

