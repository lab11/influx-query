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
import pandas as pd

try:
    from influxdb import DataFrameClient
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
def generate_csv (config, select_operation, measurement_list, tag_list, begin_time, end_time, group_list, out_filename):

    # convert times influxDB time strings in UTC
    begin_time_statement = arrow.get(begin_time, 'MM-DD-YYYY HH:mm:ss ZZZ').to('UTC').format('YYYY-MM-DD HH:mm:ss')
    end_time_statement = arrow.get(end_time, 'MM-DD-YYYY HH:mm:ss ZZZ').to('UTC').format('YYYY-MM-DD HH:mm:ss')

    # create output directory if necessary
    out_dir = os.path.dirname(out_filename)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # connect to influx database
    client = DataFrameClient(config['host'], config['port'], config['username'],
            config['password'], config['database'], ssl=True, verify_ssl=True)

    # comma separated list of measurements to take the `value` key from
    measurement_statement = ','.join(['"' + measurement + '"' for measurement in measurement_list])

    # determine appropriate tags for the WHERE clause based on tag dict
    tag_statement = ''
    first_name = True
    for tag_name in sorted(tag_list.keys()):

        # put an AND between each set of tags
        if not tag_list[tag_name]:
            continue
        if not first_name:
            tag_statement += ' AND '
        first_name = False

        # tag values in parentheses with ORs between them. Note that the tag
        # value MUST use single quotes
        tag_statement += '('
        first_value = True
        for tag_value in tag_list[tag_name]:
            if not first_value:
                tag_statement += ' OR '
            first_value = False
            tag_statement += '"' + tag_name + '" = \'' + tag_value + '\''
        tag_statement += ')'

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
    query = query.format(select_operation, measurement_statement, tag_statement, begin_time_statement, end_time_statement, group_statement)
    print("Running query: " + query)
    # setting chunked=True here allows us to retrieve more than the
    # max-row-limit of points in a single query. However, they get
    # returned as separate ResultSet's in the result, so we need to
    # determine if there are multiples, and merge them
    result = client.query(query=query)
    merged_dfs = {}
    for key in result:
        renamed = result[key].rename(columns={result[key].columns[0]: key[0]})
        if key[1] not in merged_dfs:
            merged_dfs[key[1]] = renamed
        else:
            merged_dfs[key[1]] = merged_dfs[key[1]].join(renamed)

    # generate a CSV file out of this
    # here we're going to make some assumptions to make our lives easier
    #   1) there is only one FROM measurement type
    #   2) there is at least one GROUP BY tag
    #   3) it's fine to append the GROUP BY tag values to the output filename
    for group in merged_dfs:
        group_values = [x[-1] for x in group]
        group_str = '-'.join(group_values)
        csv_filename = out_filename + '-' + group_str + '.csv'
        print("Writing file: " + csv_filename)
        merged_dfs[group].to_csv(csv_filename)

    print("Finished")


# if not a library, run an example query from influx
if __name__ == "__main__":
    # download powerblade sequence number data, only takes a second or two to run

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
    select_operation = '"value"'
    measurement_list = ['sequence_number']
    tag_list = {
        'location': ['802 Fuller'],
        'device_class': ['PowerBlade', 'BLEES'],
        'device_id': ['c098e57000cd', 'c098e57000ce'],
    }
    group_list = ['device_class', 'device_id']
    begin_time = '03-20-2017 00:00:00 US/Eastern'
    end_time = '03-21-2017 00:00:00 US/Eastern'
    out_filename = 'raw_data/sequenceNumber'

    generate_csv(config, select_operation, measurement_list, tag_list, begin_time, end_time, group_list, out_filename)

