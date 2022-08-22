import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.signal import resample as resample
import sqlite3
import glob
import os
import sys

#date = sys.argv[1]
ecus = {'engine':'atvp2', 'trans':'atvp3', 'abs':'atvp4'}
can_names = {'engine':'Engine #1', 'trans':'Transmission #1', 'abs':'Brakes - System Controller'}
direc = sys.argv[1]
name = sys.argv[2]
channel = sys.argv[3]
ecu = sys.argv[4]
f = direc + name

#for f in glob.glob(direc + '/*-binaryredislog.db'):
timestamp = name.split('-')[0]
#if not os.path.isfile("/media/gdls/gdls-data/workspace/visit4-trace/" + "/fingerprints-*"+ os.path.basename(f)+".npy"):
      
power = pd.DataFrame(np.load((direc + 'normalized_signals/'+ '-'.join(['normalized', timestamp, ecus.get(ecu), channel]) + ".npy")))

c = [ 'v','time']
power.columns  = c

conn = sqlite3.connect(f)
cur = conn.cursor()
print(power.head())
# ECU-ID map
can = pd.read_sql_query("select timestamp, id, pgn, sa_name, databyte1, databyte2, databyte3, databyte4, databyte6, databyte7, databyte8 from 'decoded_test';", conn)
messageIDs = pd.read_sql_query("select distinct(id) from decoded_test where sa_name like '"+ can_names.get(ecu) +"';", conn)
# Can Messages after filtering the duplicate messages
cans = can.drop_duplicates(subset=['timestamp','databyte1', 'databyte2', 'databyte3', 'databyte4', 'databyte6', 'databyte7', 'databyte8'], keep='first')

print('IDs : ', messageIDs.values)
messageIDs = engine.values.flatten().tolist()
print(cans.head(10))

if len(cans.index[cans['id'].isin(messageIDs)]) > 20000:
    can_indices = cans.index[cans['id'].isin(messageIDs)][0:19000]
else:
    can_indices = cans.index[cans['id'].isin(messageIDs)]

times = cans['timestamp'].values
windows = [t - s for s, t in zip(times, times[1:])]
windows = np.sort(windows)
wmean = np.mean(windows)
wstd = np.std(windows)
window = wmean + wstd

traces = []
IDS =  []
f_handle = direc + "fingerprints/" + '-'.join(['fingerprints', 'windowed',ecu, channel, os.path.basename(f).split('.')[0] ]) + '.csv'
for idx in can_indices:
        can_id = can.iloc[idx]["id"]
        time = can.iloc[idx]["timestamp"]
        start = time - (window/2)
        end = time + (window/2)
        #print(time)
        if can_id in messageIDs:
            
            start = power.index[(power['time'] - start).abs().idxmin()]
            end = power.index[(power['time'] - end).abs().idxmin()]          
            #print(start_low, end_low)
            trace = power.loc[start: end]['v']
            
        if (not np.any(np.isnan(trace))):
        
                traces.append(trace.values)
                IDS.append(can_id)
                #print(len(trace_high.values), len(trace_low.values))
        if (len(traces) >= 100) or (can_indices[-1] == idx):
            
            df_trace = pd.DataFrame(traces)
            df_id = pd.DataFrame(IDS)
            df_trace = pd.concat([df_trace, df_id], axis=1)
            print(df_trace.head())
            df_trace.to_csv(f_handle, header=False, index=False, mode='a')
            print("Written Fingerprint-ID samples to ", "fingerprints")
            traces = []
            IDS = []
            
