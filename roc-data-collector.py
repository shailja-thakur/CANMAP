import os
import signal
import subprocess
import redis
import sys
import threading
import json
import struct
import time
import numpy
import glob
import signal
import datetime
import pickle
import os.path

#conn = redis.StrictRedis(host='localhost', port=6379, db=0)

detect_proc = None
resource_proc = None
playback_proc = None
anomaly_check_thread = None

# returns a list of all the files that have anomalies
# of the requested type, and the files that have no
# anomalies as a list of tuples (filename, has_anomaly_boolean)
def collect_inputs(anomaly_type):
    # currently just a stub, here are some examples
    # if type == clipping
    anomaly_files = [(x, True) for x in sorted(glob.glob('traces/*.csv'))]
    no_anomaly_files = [(x, False) for x in sorted(glob.glob('traces-no-anomaly/*.csv'))]
    #return [('traces/1533236345-2486-Long-blast-test-6.csv', True), ('traces-no-anomaly/1533061225-826-no-anomaly.csv', False)]
    anomaly_files.extend(no_anomaly_files)
    return anomaly_files

# starts the target detector for the given type with the given parameter on the given channel
# returns a handle to the detector process
def start_detector_with_param(param, channel, anomaly_type):
    global detect_proc
    # modify this function for each new detector
    
    if anomaly_type == 'clipping':
        detect_proc = subprocess.Popen(['python3 ../processors/clips/ClipDetect.py ' + channel + ' ' + str(param)], shell=True, preexec_fn=os.setsid, close_fds=True)        

    #print("Sending command armed")
    #sys.stdout.flush()
    time.sleep(0.5)
    conn.publish('command', 'armed')
    return detect_proc

# get the minimum and maximum possible values for the given parameter on a detector
def get_detector_bounds(channel, anomaly_type):
    if anomaly_type == 'clipping':
        return (0.005, 0.5)

    print("Error cannot find detector bounds.")
    sys.stdout.flush()

# kills a process with sigint
def kill_process(handle):
    os.killpg(os.getpgid(handle.pid), signal.SIGINT)
    handle.wait()

# handle sigint
def signal_handler(sig, frame):
    conn.publish('please-die', 'now')
    if playback_proc: kill_process(playback_proc)
    if detect_proc: 
        conn.publish('command', 'goodbye')
        detect_proc.wait()
    if resource_proc: kill_process(resource_proc)
    if anomaly_check_thread: anomaly_check_thread.join()
    sys.exit(0)

#signal.signal(signal.SIGINT, signal_handler)

def start_process(testfile):

    detect_proc = subprocess.Popen(['python3 NN.py ' + testfile], shell=True, preexec_fn=os.setsid, close_fds=True)
    print(detect_proc.pid)
    return detect_proc.pid

# starts python script that collects memory and cpu utilization
# receives the pid to be monitored, the monitoring period (in secs), and a filename in which collected data is written as parameters
def start_resource_monitor(filename, testfile, period):
    pid = start_process(testfile)
    global resource_proc
    param = "-f " + filename + " -p " + str(period)
    resource_proc = subprocess.Popen(['python3 resource-monitor.py ' + str(pid) + ' ' + param], shell=True, preexec_fn=os.setsid, close_fds=True)
    return resource_proc

# starts playing back the target file into localhost redis
# return the handle to the playback process
def playback_file(filename):
    global playback_proc
    playback_proc = subprocess.Popen(['bash ../../daqpi/scripts/collectionscripts/playback-all-no-anomalies.sh ' + filename], shell=True, preexec_fn=os.setsid, close_fds=True)
    return playback_proc

# thread proc to check if that an anomaly
is_anomaly_found = False
latencies = []

def anomaly_thread_proc():
    global is_anomaly_found

    is_anomaly_found = False
    latencies = []

    pubsub = conn.pubsub()
    pubsub.subscribe('anomaly')
    pubsub.subscribe('please-die')


    pubsub.subscribe('info')
    pubsub.subscribe('atvp1-status')

    current_time = -1.0

    for message in pubsub.listen():
        if message['type'] != "message":
            continue

        if message['channel'].decode('ascii') == 'please-die':
            return

        if message['channel'].decode('ascii') == 'atvp1-status':
            ms, s = struct.unpack('<II', message['data'][0:8])
            current_time = float(s) + (float(ms) / 1000.0)
            continue

        if message['channel'].decode('ascii') == 'info' and current_time > 0:
            latencies.append((time.time(), current_time - float(message['data'].decode('ascii').split(' ')[4])))
            continue

        if message['channel'].decode('ascii') == 'anomaly':
            #print("Message on anomaly channel.")
            #sys.stdout.flush()
            anomaly = json.loads(message['data'].decode('ascii'))
            if anomaly['anomaly'] == 1:
                is_anomaly_found = True


# runs the detector on the all the input files with the given parameter
# returns the confusion matrix as a tuple (tp, fp, tn, fn)
memoization_table = dict()

def run_full_set_with_param(anomaly_type, channel, param, check_resources):
    global anomaly_check_thread
    global memoization_table

    if anomaly_type in memoization_table:
        if channel in memoization_table[anomaly_type]:
            if param in memoization_table[anomaly_type][channel]:
                print("Set result from memo.")
                tp, fp, tn, fn = memoization_table[anomaly_type][channel][param]
                print("Threshold at", param, "gives tp:", tp, "fp:", fp, "tn:", tn, "fn:", fn)
                print("")
                print("=====================================================================")
                print("")
                sys.stdout.flush()
                return memoization_table[anomaly_type][channel][param]
        else:
            memoization_table[anomaly_type][channel] = dict()
    else:
        memoization_table[anomaly_type] = dict()
        memoization_table[anomaly_type][channel] = dict()


    input_files = collect_inputs(anomaly_type)

    tp = 0
    fp = 0
    tn = 0
    fn = 0

    for input_file in input_files:
        print("Starting file", input_file)
        sys.stdout.flush()
        # start the detector
        #print("Starting detector.")
        #sys.stdout.flush()
        detector_handle = start_detector_with_param(param, channel, anomaly_type)
        
        # monitor resources every 0.5 seconds
        filename = input_file[0] + '-resources-' + channel + '-' + str(param)
        if check_resources:
            monitor_handle = start_resource_monitor(detector_handle.pid, filename + 'memcpu.txt', 0.5)

        #print("Starting anomaly thread.")
        #sys.stdout.flush()
        anomaly_check_thread = threading.Thread(target=anomaly_thread_proc)
        anomaly_check_thread.start()

        # playback the file
        #print("Starting playback.")
        #sys.stdout.flush()
        playback_handle = playback_file(input_file[0])
        playback_handle.wait()

        #print("Stopping anomaly thread.")
        #sys.stdout.flush()
        conn.publish('please-die', 'now')
        anomaly_check_thread.join()

        if is_anomaly_found:    # if an anomaly was detected
            if input_file[1]:       # if the file had an anomaly
                tp += 1                 # this is a true positive
            else:                   # else the file had no anomaly
                fp += 1                 # this is a false positive (we found an anomaly where there wasn't one)
        else:                   # if no anomaly was detected
            if input_file[1]:       # if the file had an anomaly
                fn += 1                 # this is a false negative (we didn't find an anomaly but we should have)
            else:                   # else the file had no anomaly
                tn += 1                 # this is a true negative

        if check_resources:
            kill_process(monitor_handle)  
            with open(filename + 'latency.txt', 'w') as f:
                for time, lat in latencies:
                    f.writeline(str(time) + ',' + str(lat))
                f.flush() 

        #print("Killing detector.")
        #sys.stdout.flush()
        conn.publish('command', "goodbye")
        detector_handle.wait()
        #print("Detector dead.")
        #sys.stdout.flush()

    print("Threshold at", param, "gives tp:", tp, "fp:", fp, "tn:", tn, "fn:", fn)
    print("")
    print("=====================================================================")
    print("")
    sys.stdout.flush()

    memoization_table[anomaly_type][channel][param] = (tp, fp, tn, fn)
    with open('memotable.pickle', 'wb') as m:
        pickle.dump(memoization_table, m)

    return (tp, fp, tn, fn)


# actually finds the extrema
def binary_search(anomaly_type, channel):

    detector_bounds = get_detector_bounds(channel, anomaly_type)

    # first try the parameter at min_bound
    tp, fp, tn, fn = run_full_set_with_param(anomaly_type, channel, detector_bounds[0], False)

    print("At lower bound:", tp, fp, tn, fn)
    sys.stdout.flush()

    # we are running under the assuption that the parameter is a non-negative real number
    # and the number of anomalies changes with it monotonically (increasing or decreasing)

    anomaly_count_at_zero = tp + fp
    anomalies_at_zero = True
    if anomaly_count_at_zero == 0:
        anomalies_at_zero = False

    print("Anomalies at lower bound:", anomaly_count_at_zero, '(', anomalies_at_zero, ')')
    sys.stdout.flush()

    # double the parameter every time until we find one so high we get
    # either no anomalies (if we had anomalies at zero), or all anomalies
    # (if we had no anomalies at zero)
    maximum_parameter = detector_bounds[0]

    total_number_of_files = len(collect_inputs(anomaly_type))
    anomalies_at_max = False
    
    safety_counter = 0
    while safety_counter < 10:
        tp, fp, tn, fn = run_full_set_with_param(anomaly_type, channel, maximum_parameter, False)

        if tp + fp == 0 and anomalies_at_zero:
            break

        if tp + fp == total_number_of_files and not anomalies_at_zero:
            anomalies_at_max = True
            break

        maximum_parameter += (detector_bounds[0] + detector_bounds[1]) / 10.0
        safety_counter += 1

    # if we didn't find no or all anomalies yet, there was an error
    if safety_counter >= 10:
        print("Error! Param reached max value without finding extrema.")
        sys.stdout.flush()
        return

    if anomaly_count_at_zero == total_number_of_files and anomalies_at_max:
        print("Error! Detector reports anomalies on all inputs for all parameter values.")
        sys.stdout.flush()
        return

    if anomalies_at_zero and anomalies_at_max:
        print("Error! Detector never reports zero anomalies on any input file for any parameter value.")
        sys.stdout.flush()
        return

    print("Binary searching in range 0 to", maximum_parameter)
    sys.stdout.flush()

    # now we binary seach between the extrema to find the point where we
    # transition from 0 anomalies found to 1 or more anomalies found in all the input files
    # if we had anomalies at zero we search up while we still have anomalies
    # if we had no anomalies at zero we search down while we still have anomalies
    one_anomaly_threshold_parameter_min = 0
    one_anomaly_threshold_parameter_max = maximum_parameter

    for i in range(10):
        parameter_to_check = (one_anomaly_threshold_parameter_min + one_anomaly_threshold_parameter_max) / 2.0
        print("Checking parameter", parameter_to_check)
        sys.stdout.flush()
        tp, fp, tn, fn = run_full_set_with_param(anomaly_type, channel, parameter_to_check, False)

        search_upwards_next = False

        if tp + fp > 0 and anomalies_at_zero:
            search_upwards_next = True
        if tp + fp == 0 and not anomalies_at_zero:
            search_upwards_next = True

        if search_upwards_next:
            one_anomaly_threshold_parameter_min = parameter_to_check
        else:
            one_anomaly_threshold_parameter_max = parameter_to_check

    # find which one of the extrema has zero anomalies
    tp, fp, tn, fn = run_full_set_with_param(anomaly_type, channel, one_anomaly_threshold_parameter_min, False)

    one_anomaly_threshold = 0.0
    if tp + fp == 0:
        one_anomaly_threshold = one_anomaly_threshold_parameter_min
        print("Zero anomalies extrema:", one_anomaly_threshold)
        sys.stdout.flush()
    else:
        tp, fp, tn, fn = run_full_set_with_param(anomaly_type, channel, one_anomaly_threshold_parameter_max, False)
        if tp + fp == 0:
            one_anomaly_threshold = one_anomaly_threshold_parameter_max
            print("Zero anomalies extrema:", one_anomaly_threshold)
            sys.stdout.flush()
        else:
            print("Error! could not find point with zero anomalies.")
            sys.stdout.flush()

    # we found the threshold

    # next we search between the extrema to find the point where we transition from 
    # some number of anomlies to all anomalies
    # if there is all the anomalies at zero, then we search up while we still have the max anomalies
    # if there is less than max anomalies at zero, then we search down while we still have the max anomalies
    all_anomaly_threshold_parameter_min = 0
    all_anomaly_threshold_parameter_max = maximum_parameter

    for i in range(10):
        parameter_to_check = (all_anomaly_threshold_parameter_min + all_anomaly_threshold_parameter_max) / 2.0
        print("Checking parameter", parameter_to_check)
        sys.stdout.flush()
        tp, fp, tn, fn = run_full_set_with_param(anomaly_type, channel, parameter_to_check, False)

        search_upwards_next = False

        if tp + fp == total_number_of_files and anomaly_count_at_zero == total_number_of_files:
            search_upwards_next = True
        if tp + fp < total_number_of_files and anomaly_count_at_zero != total_number_of_files:
            search_upwards_next = True

        if search_upwards_next:
            all_anomaly_threshold_parameter_min = parameter_to_check
        else:
            all_anomaly_threshold_parameter_max = parameter_to_check

    # find which one of the extrema has all anomalies
    tp, fp, tn, fn = run_full_set_with_param(anomaly_type, channel, all_anomaly_threshold_parameter_min, False)

    all_anomaly_threshold = 0.0
    if tp + fp == total_number_of_files:
        all_anomaly_threshold = all_anomaly_threshold_parameter_min
        print("All anomalies extrema:", all_anomaly_threshold)
        sys.stdout.flush()
    else:
        tp, fp, tn, fn = run_full_set_with_param(anomaly_type, channel, all_anomaly_threshold_parameter_max, False)
        if tp + fp == total_number_of_files:
            all_anomaly_threshold = all_anomaly_threshold_parameter_max
            print("All anomalies extrema:", all_anomaly_threshold)
            sys.stdout.flush()
        else:
            print("Error! could not find point with all anomalies.")
            sys.stdout.flush()

    print("Zero anomalies:", one_anomaly_threshold, "All anomalies:", all_anomaly_threshold)
    sys.stdout.flush()


def get_curve_data(anomaly_type, channel, amin, amax):

    step = (amax - amin) / 10
    for i in numpy.arange(amin, amax, step):
        tp, fp, tn, fn = run_full_set_with_param(anomaly_type, channel, i, True)
        print('for param at', i, 'we have tp:', tp, 'fp:', fp, 'tn:', tn, 'fn:', fn)
    

if __name__ == '__main__':
    print("Starting at", datetime.datetime.now())
   
    filename=sys.argv[1]
    testfile = sys.argv[2]
    period = 0.5
    #if os.path.isfile('memotable.pickle'):
    #    with open('memotable.pickle', 'rb') as m:
    #        memoization_table = pickle.load(m)

    #binary_search(sys.argv[1], sys.argv[2])
    start_resource_monitor(filename, testfile, 0.5)
    print("Finishing at", datetime.datetime.now())
