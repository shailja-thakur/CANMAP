import sys
import struct

if len(sys.argv) < 3:
    print("Usage: python3 decode-combined-trace-to-csv.py combinedtrace.csv topic-1 ... topic-n")
    exit(1)

topics = sys.argv[2:]

def decode_segment(seg):
    
    ms_time = struct.unpack('<h', bytes.fromhex(seg[0:4]))[0]
    s_time = struct.unpack('<i', bytes.fromhex(seg[8:16]))[0]
    samples = []

    for i in range(16, len(seg), 4):
        sample_bytes = bytes.fromhex(seg[i:i+4])

        if len(sample_bytes) != 2:
            print("Error: bad width reading block '" + seg + "'")
            exit(1)

        samples.append(struct.unpack('<h', sample_bytes)[0])

    return (s_time, ms_time, samples)

def decode_can_segment(seg):
    sample = bytes.fromhex(seg).decode('utf-8')

    return (0, 0, sample)

def dump_reading(reading, fp):
    if isinstance(reading[2], str):
        fp.write(str(reading[0]) + "." + format(reading[1], '03') + "000," + reading[2] + '\n')
    else:
        if len(reading[2]) == 3:
            # it's a gyro or accel reading
            fp.write(str(reading[0]) + "." + format(reading[1], '03') + "000," + str(reading[2][0]) + "," + str(reading[2][1]) + "," + str(reading[2][2]) + '\n')
        else:    
            us_per_reading = int(1000 / len(reading[2]))
            for i in range(len(reading[2])):
                us_now = reading[1] * 1000 + i * us_per_reading
                fp.write(str(reading[0]) + "." + format(us_now, '06') + "," + str(reading[2][i]) + '\n')


with open(sys.argv[1]) as f:
    firstline = f.readline()
    topics_in_file = firstline.split(',')
    topic_indices = []
    topic_filenames = []
    all_can = True
    for topic in topics:
        if not topic in topics_in_file:
            print("Error: cannot find", topic, "in file", sys.argv[1])
            exit(1)
        else:
            
            topic_indices.append(topics_in_file.index(topic))
            topic_filenames.append(sys.argv[1].split('/')[-1].split('-')[0] + "-" + topic + ".csv")
            if not ('can' in topic):
                all_can = False

    print(topics_in_file)
    print(topic_indices)
    if all_can:
        print("Error: cannot decode CAN alone.")
        exit(1)

    print("Decoding topics:", ", ".join(topics))

    pre_sync_point = []
    have_reading_all_topics = [False] * len(topics)
    fail_after_lines = 0
    start_ms = 0
    start_s = 0

    for i in range(len(topics)):
        pre_sync_point.append([])
    
    for line in f:
        readings = line.split(',')
        #print(readings)
        for i in range(len(topic_indices)):
            #print(topic_indices[i])
            if readings[topic_indices[i]] != '':
                decoded_segment = None
                if 'can' in topics[i]:
                    print('reading topics')
                    decoded_segment = decode_can_segment(readings[topic_indices[i]])
                else:
                    decoded_segment = decode_segment(readings[topic_indices[i]])
                    start_ms = decoded_segment[1]
                    start_s = decoded_segment[0]

                pre_sync_point[i].append(decoded_segment)
                have_reading_all_topics[i] = True
                
        if not (False in have_reading_all_topics):
            break
        else:
            fail_after_lines = fail_after_lines + 1

        if fail_after_lines > 10000:
            print("Missing readings on:", topics[have_reading_all_topics.index(False)])
            exit(1)


    print("Writing to:", ", ".join(topic_filenames))
    output_files = [open(filename, 'w') for filename in topic_filenames]

    print("Starting at", start_s, "seconds,", start_ms, "ms")

    for i in range(len(topics)):
        for reading in pre_sync_point[i]: 
            if 'can' in topics[i]:
                dump_reading((start_s, start_ms, reading[2]), output_files[i])
            else:
                if reading[0] >= start_s and reading[1] >= start_ms:
                    dump_reading(reading, output_files[i])

    start_ms = (start_ms + 1) % 1000
    if start_ms == 0:
        start_s = start_s + 1

    current_ms = start_ms
    current_s = start_s

    for line in f:
        readings = line.split(',')
        #print("line")
        for i in range(len(topic_indices)):
            if readings[topic_indices[i]] != '':
                #print("topic in line")
                if 'can' in topics[i]:
                    decoded_segment = decode_can_segment(readings[topic_indices[i]])
                    dump_reading((current_s, current_ms, decoded_segment[2]), output_files[i])
                else:
                    decoded_segment = decode_segment(readings[topic_indices[i]])
                    current_ms = decoded_segment[1]
                    current_s = decoded_segment[0]
                    dump_reading(decoded_segment, output_files[i])
                

    for outfile in output_files:
        outfile.close()
        
    print("Decoding complete.")
