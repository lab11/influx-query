#! /usr/bin/env python3

# Download data from InfluxDB and write to a CSV file
# Example configurations are at the bottom of the file

## libraries
import csv
import glob
import os
import re
import sys
import time

try:
    from influxdb import InfluxDBClient
except ImportError:
    print('Could not import influxdb library')
    print('sudo pip3 install influxdb')
    sys.exit(1)

try:
    import configparser
except ImportError:
    print('Could not import configparser library')
    print('sudo pip3 install configparser')
    sys.exit(1)

try:
    import arrow
except ImportError:
    print('Could not import arrow library')
    print('sudo pip3 install arrow')
    sys.exit(1)


## run an influxdb query and write the results to a csv file
def generate_csv (config, select_operation, from_measurement, where_tag_list, group_list, begin_time, end_time, out_filename):

    # convert times influxDB time strings in UTC
    begin_time_statement = arrow.get(begin_time, 'MM-DD-YYYY HH:mm:ss ZZZ').to('UTC').format('YYYY-MM-DD HH:mm:ss')
    end_time_statement = arrow.get(end_time, 'MM-DD-YYYY HH:mm:ss ZZZ').to('UTC').format('YYYY-MM-DD HH:mm:ss')

    # create output directory if necessary
    out_dir = os.path.dirname(out_filename)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # connect to influx database
    client = InfluxDBClient(config['host'], config['port'], config['username'],
            config['password'], config['database'], ssl=True, verify_ssl=True)

    # measurement to take the `value` key from
    if '"' not in from_measurement:
        from_statement = '"' + from_measurement + '"'
    else:
        from_statement = from_measurement

    # determine appropriate tags for the WHERE clause based on tag dict
    where_statement = ''
    first_name = True
    for tag_name in sorted(where_tag_list.keys()):

        # put an AND between each set of tags
        if not where_tag_list[tag_name]:
            continue
        if not first_name:
            where_statement += ' AND '
        first_name = False

        # tag values in parentheses with ORs between them. Note that the tag
        # value MUST use single quotes
        where_statement += '('
        first_value = True
        for tag_value in where_tag_list[tag_name]:
            if not first_value:
                where_statement += ' OR '
            first_value = False
            where_statement += '"' + tag_name + '" = \'' + tag_value + '\''
        where_statement += ')'

    # comma separated list of tags to group by
    group_statements = []
    for tag in group_list:
        time_pattern = re.compile("^time\(.+\)$")
        if time_pattern.match(tag):
            # GROUP BY time statements cannot have double quotes
            group_statements.append(tag)
        else:
            group_statements.append('"' + tag + '"')
    group_statement = ','.join(group_statements)

    # create query and execute it
    query = "SELECT {} FROM {} WHERE {} AND (time >= '{}' AND time < '{}') GROUP BY {}"
    query = query.format(select_operation, from_statement, where_statement, begin_time_statement, end_time_statement, group_statement)
    print("Running query: " + query)
    result = client.query(query)

    # generate a CSV file out of this
    # here we're going to make some assumptions to make our lives easier
    #   1) there is only one FROM measurement type
    #   2) there is at least one GROUP BY tag
    #   3) it's fine to append the GROUP BY tag values to the output filename
    for series in result.raw['series']:
        csv_filename = out_filename + '.csv'
        if 'tags' in series.keys():
            id = '-'.join([series['tags'][key] for key in sorted(series['tags'].keys())])
            id = id.replace(' ', '_')
            csv_filename = out_filename + '-' + id + '.csv'

        print("Writing file: " + csv_filename)
        with open(csv_filename, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['#time', select_operation + ' from ' + series['name']])
            writer.writerows(series['values'])

    print("Finished")


# if not a library, run an example query from influx
if __name__ == "__main__":
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

    # configurations
    select_operation = 'value'
    from_measurement = 'sequence_number'
    where_tag_list = {
        'location': ['802 Fuller'],
        'device_class': ['PowerBlade', 'BLEES'],
        'device_id': ['c098e57000cd', 'c098e57000ce'],
    }
    group_list = ['device_class', 'device_id']
    begin_time = '03-20-2017 00:00:00 US/Eastern'
    end_time = '03-21-2017 00:00:00 US/Eastern'
    out_filename = 'raw_data/sequenceNumber'

    # download PowerBlade and BLEES sequence number data, only takes a second or two to run
    generate_csv(config, select_operation, from_measurement, where_tag_list, group_list, begin_time, end_time, out_filename)

