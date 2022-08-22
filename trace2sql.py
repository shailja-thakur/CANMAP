import argparse
import redis
import time
import struct
import json
import base64
import sqlite3
from datetime import datetime

parser = argparse.ArgumentParser(description="Loads a trace file and plays it back into redis in JSON format.")
parser.add_argument('file', type=str, help='The file containing the trace to be converted to sqlite.')
parser.add_argument('db', type=str, help='The sqlite file to add the file to as a table.')
parser.add_argument('-b', dest='begin', default=0, type=int, help='Offset into the trace to begin at (seconds). Default 0.')
parser.add_argument('-l', dest='length', default=0, type=int, help='Length of time in trace to play back (seconds), 0 to play to the end. Default 0.')

arguments = parser.parse_args()

db = sqlite3.connect(arguments.db)

def make_set_cols(point):
    cols = ['power_low', 'power_high', 'audio', 'temperature', 'gyro_x', 'gyro_y', 'gyro_z', 'accel_x', 'accel_y', 'accel_z']
    cols = [point + '_' + x + ' INT' for x in cols]
    return ', '.join(cols)

# this is not properly escaped SQL creation, but this script should not need to be concerned with SQL injection
table_name = arguments.file.replace('.', '_').replace('-', '_').replace('/', '_')
db.execute('DROP TABLE IF EXISTS ' + table_name)
db.execute('CREATE TABLE ' + table_name + ' (timestamp INT, ' + make_set_cols('engine') + ', ' + make_set_cols('trans') + ', ' + make_set_cols('abs') + ', can_raw_bytes CHARACTER(200))')

last_kept_msec = 0
first_inserted = True
total_msec = 0

last_msec = 0
last_sec = 0

input_topics = []
atvp_point_map = ['engine', 'trans', 'abs']

time_map = dict()

def get_sql_for(sec, ms, cell, index):
    if cell in time_map[sec][ms]:
        if index < len(time_map[sec][ms][cell]):
            return str(time_map[sec][ms][cell][index]) + ', '
        else:
            return 'null, '
    else:
        return 'null, '


def push_to_topics(messages):
    global last_msec, last_sec, total_msec, first_inserted, last_kept_msec, time_map

    messages_pushed_this_line = 0

    for i in range(len(messages)):
        if messages[i]:	# true if messages[i] is non empty
            message_bytes = bytes.fromhex(messages[i])
            daq_msec, unused, daq_sec = struct.unpack('<HHI', message_bytes[0:8])
            print(input_topics)
            topic_name = input_topics[i][6:]
            print(topic_name)
            topic_num = int(input_topics[i][4:5])
            print(topic_num)
            array_data = []
            if topic_name != 'can':
                last_sec = daq_sec
                last_msec = daq_msec
                array_data = [x[0] for x in struct.iter_unpack('<h', message_bytes[8:])]
                print(array_data)
                if daq_sec not in time_map:
                    time_map[daq_sec] = {daq_msec: dict()}
                    time_map[daq_sec]['start_ms'] = daq_msec
                elif daq_msec not in time_map[daq_sec]:
                    time_map[daq_sec][daq_msec] = dict()

            pushed_a_message = 0

            if topic_name == 'power1':
                
                time_map[daq_sec][daq_msec][atvp_point_map[topic_num] + '_power_low'] = array_data
                pushed_a_message += 1
            elif topic_name == 'power2':
                time_map[daq_sec][daq_msec][atvp_point_map[topic_num] + '_power_high'] = array_data
                pushed_a_message += 1
            elif topic_name == 'audio':
                time_map[daq_sec][daq_msec][atvp_point_map[topic_num] + '_audio'] = array_data
                pushed_a_message += 1
            elif topic_name == 'temperature':
                time_map[daq_sec][daq_msec][atvp_point_map[topic_num] + '_temperature'] = array_data
                pushed_a_message += 1
            elif topic_name == 'gyro':
                time_map[daq_sec][daq_msec][atvp_point_map[topic_num] + '_gyro'] = array_data
                pushed_a_message += 1
            elif topic_name == 'accel':
                time_map[daq_sec][daq_msec][atvp_point_map[topic_num] + '_accel'] = array_data
                pushed_a_message += 1
            elif topic_name == 'can':
                time_map[last_sec][last_msec]['can_raw_bytes'] = message_bytes
                pushed_a_message += 1

            messages_pushed_this_line += pushed_a_message

    
    if len(time_map) > 5:
        # we have more than 5 seconds of data, the oldest second should be full
        old_sec = min(time_map.keys())
        start_sec_ms = int(time_map[old_sec]['start_ms'])

        sorted_ms = sorted([x for x in time_map[old_sec].keys() if x != 'start_ms'], key=lambda x: ((x - start_sec_ms) % 1000))

        if first_inserted:
            first_inserted = False
            last_kept_msec = (sorted_ms[0] - start_sec_ms) % 1000

        query_rows = []

        for ordered_ms in sorted_ms:
            real_ms = (ordered_ms - start_sec_ms) % 1000
            delta_ms = (real_ms - last_kept_msec) % 1000

            total_msec += delta_ms
            last_kept_msec = real_ms

            show_time = total_msec * 1000
            row = '(' + str(show_time) + ', '
            
            for prefix in ['engine', 'trans', 'abs']:
                row += get_sql_for(old_sec, ordered_ms, prefix + '_power_low', 0)
                row += get_sql_for(old_sec, ordered_ms, prefix + '_power_high', 0)
                row += get_sql_for(old_sec, ordered_ms, prefix + '_audio', 0)
                row += get_sql_for(old_sec, ordered_ms, prefix + '_temperature', 0)


                row += get_sql_for(old_sec, ordered_ms, prefix + '_gyro', 0)
                row += get_sql_for(old_sec, ordered_ms, prefix + '_gyro', 1)
                row += get_sql_for(old_sec, ordered_ms, prefix + '_gyro', 2)
                row += get_sql_for(old_sec, ordered_ms, prefix + '_accel', 0)
                row += get_sql_for(old_sec, ordered_ms, prefix + '_accel', 1)
                row += get_sql_for(old_sec, ordered_ms, prefix + '_accel', 2)

            if 'can_raw_bytes' in time_map[old_sec][ordered_ms]: 
                row += '"' + bytes.hex(time_map[old_sec][ordered_ms]['can_raw_bytes']) + '")'
            else:
                row += 'null)'

            query_rows.append(row)

            for x in range(1, 50):
                show_time = (total_msec * 1000) + (x * 20)
                row = '(' + str(show_time) + ', '

                for prefix in ['engine', 'trans', 'abs']:
                    row += get_sql_for(old_sec, ordered_ms, prefix + '_power_low', x)
                    row += get_sql_for(old_sec, ordered_ms, prefix + '_power_high', x)
                    row += get_sql_for(old_sec, ordered_ms, prefix + '_audio', x)
                    row += ('null, ' * 7)

                row += 'null)'
                query_rows.append(row)

        query = 'INSERT INTO ' + table_name + ' VALUES ' + ', '.join(query_rows)
        db.execute(query)
        db.commit()

        # remove the oldest second
        time_map.pop(old_sec, None)
    

    return messages_pushed_this_line

with open(arguments.file, 'r') as trace_file:

    # read the topic names from the file
    input_topics = trace_file.readline().strip().split(",")[1:]

    print("Converting", arguments.file, "to sqlite database", arguments.db)
    print("Original source trace will not be changed in any way.")

    messages_pushed = 0

    first_line = trace_file.readline().strip().split(",")	
    
    # if we start at the beginning, push the first line's messages
    if arguments.begin == 0:		
        messages_pushed += push_to_topics(first_line[1:])

    input_start_time = float(first_line[0])
    start_time_measure = datetime.utcnow()
    counting_start_time = time.time()
    this_offset_time = 0

    # if we do not start at the beginning, skip over lines until the correct start time is reached
    if arguments.begin != 0:
        for line in trace_file:
            split_line = line.strip().split(",")
            this_offset_time = float(split_line[0]) - input_start_time

            if this_offset_time >= arguments.begin:
                messages_pushed += push_to_topics(split_line[1:])
                input_start_time = float(split_line[0])
                break

    counting_start_time = time.time()

    # now output the lines until the end of the file or the given length time has elapsed
    for line in trace_file:
        split_line = line.strip().split(",")
        this_offset_time = float(split_line[0]) - input_start_time

        if arguments.length != 0 and this_offset_time > arguments.begin + arguments.length:
            break

        messages_pushed += push_to_topics(split_line[1:])

    db.execute('CREATE INDEX timestamp_index ON ' + table_name + ' (timestamp ASC)')
    db.commit()

    total_time_measure = datetime.utcnow() - start_time_measure	
    print("Converted", messages_pushed, "messages in", total_time_measure.total_seconds(), "seconds.")

