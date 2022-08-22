#!/usr/bin/python3

import argparse
import time
import psutil
#import redis

parser = argparse.ArgumentParser(description="Collects resource utilization of a selection of processes.")
parser.add_argument('target_pids', nargs='+', type=int, help='The PIDs to monitor.')
parser.add_argument('-mp', '--period', dest='monitor_period', type=float, default=5.0, help='Period of sample collection.')
parser.add_argument('-f', '--output-file', dest='output_file', type=str, default='stats-'+str(time.time())+'.txt', help='Output file name.')
#parser.add_argument('-r', '--redis-host', dest='redis_host', type=str, default='localhost', help='Address of the redis instance.')
#parser.add_argument('-p', '--redis-port', dest='redis_port', type=int, default=6379, help='Port of the redis instance.')

arguments = parser.parse_args()

processes = [psutil.Process(pid) for pid in arguments.target_pids]

#rs = redis.StrictRedis(host=arguments.redis_host, port=arguments.redis_port, db=0)

with open(arguments.output_file, 'w') as f:

    f.write('time,global_cpu_percent,global_mem_percent,' + ','.join([str(x)+'_cpu,'+str(x)+'_mem' for x in arguments.target_pids]) + '\n')

    last_sleep_time = time.time()

    while True:
        stats = []

        if(psutil.pid_exists(arguments.target_pids[0]) == False):
            f.close()
            break

        stats.append(time.time())

        cpu_time = psutil.cpu_times_percent()
        stats.append(cpu_time.user + cpu_time.system)

        stats.append(psutil.virtual_memory().percent)

        for proc in processes:
            stats.append(proc.cpu_percent())
            stats.append(proc.memory_info().rss)

        f.write(','.join([str(x) for x in stats]) + '\n')
        f.flush()

        #rs.publish('command', 'info')

        time_to_sleep_to = last_sleep_time + arguments.monitor_period
        remaining_in_sleep = time_to_sleep_to - time.time()
        time.sleep(remaining_in_sleep)
        last_sleep_time += arguments.monitor_period
        

            
