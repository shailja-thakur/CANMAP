import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.signal import resample as resample
import sqlite3
import glob
import os
import sys
import random

#date = sys.argv[1]
ecus = {'engine':'atvp2', 'trans':'atvp3', 'abs':'atvp4'}
can_names = {'engine':'Engine #1', 'trans':'Transmission #1', 'abs':'Brakes - System Controller'}
direc = sys.argv[1]
name = sys.argv[2]
channel =  sys.argv[3]
ecu = sys.argv[4]
f = direc + name

#for f in glob.glob(direc + '/*-binaryredislog.db'):
timestamp = name.split('-')[0]

power = pd.DataFrame(np.load((direc + 'normalized_signals/'+ '-'.join(['normalized', timestamp, ecus.get(ecu), channel]) + ".npy")))

#trans_power = pd.DataFrame(np.load((direc + 'normalized_signals/' + '-'.join(['normalized', timestamp, 'atvp2', channel]) + ".npy")))
#atvp3_power2 = pd.read_csv("/home/s7thakur/scripts/" + timestamp + "-atvp3-power2.csv")

#abs_power = pd.DataFrame(np.load((direc + 'normalized_signals/' + '-'.join(['normalized', timestamp, 'atvp2', channel]) + ".npy")))


c = [ 'v','time']
power.columns  = c
#engine_power2.columns  = c

#trans_power.columns  = c
#trans_power2.columns = c

#abs_power.columns = c
#abs_power2.columns  = c

conn = sqlite3.connect(f)
cur = conn.cursor()
print(power.head())
#print(abs_power.head())
#print(trans_power.head())
# ECU-ID map
can = pd.read_sql_query("select timestamp, id, pgn, sa_name, databyte1, databyte2, databyte3, databyte4, databyte6, databyte7, databyte8 from 'decoded_test';", conn)
messageIDs = pd.read_sql_query("select distinct(id) from decoded_test where sa_name like '" + can_names.get(ecu) + "' ;", conn)
print('sql query: ', "select distinct(id) from decoded_test where sa_name like '" + can_names.get(ecu) + "' ;")

# Can Messages after filtering the duplicate messages
cans = can.drop_duplicates(subset=['timestamp','databyte1', 'databyte2', 'databyte3', 'databyte4', 'databyte6', 'databyte7', 'databyte8'], keep='first')

print('IDs : ', messageIDs.values)
#print('Trans IDs : ', trans.values)
#print('ABS IDs : ', ABS.values)

mapper = {}
messageIDs = messageIDs.values.flatten().tolist()
#mapper['trans'] = trans.values.flatten().tolist()
#mapper['abs'] = ABS.values.flatten().tolist()
print(cans.head(10))

if len(cans.index[cans['id'].isin(messageIDs)]) > 20000:
    can_indices = cans.index[cans['id'].isin(messageIDs)][0:19000]
else:
    can_indices = cans.index[cans['id'].isin(messageIDs)]

#if len(cans.index[cans['id'].isin(mapper['trans'])]) > 20000:
#    can_trans = cans.index[cans['id'].isin(mapper['trans'])][0:19000]
#else:
#    can_trans = cans.index[cans['id'].isin(mapper['trans'])]


#can_indices = []
#can_indices.extend(can_trans.values)
#can_indices.extend(can_abs.values)
#print(can_indices)
#can_indices = np.hstack([can_engine, can_trans, can_abs])
#can_indices = pd.DataFrame(can_indices)
#can_indices = can_indices.sample(frac=1)
#print(can_indices.values)
#can_indices = cans.index[cans['id'].isin(sum(mapper.values(), []))]
#print(np.unique(cans.loc[can_indices, 'id']))
before = 50
after = 250
traces = []
IDS =  []
timestamp=[]
f_handle = direc + "fingerprints/" + '-'.join(['fingerprints',ecu, channel, os.path.basename(f).split('.')[0] ]) + '.csv'
for idx in can_indices:
        can_id = can.iloc[idx]["id"]
        time = can.iloc[idx]["timestamp"]
        #print(time)
        #print(can_id)
        if can_id in messageIDs:

            start = power.index[(power['time'] - time).abs().idxmin()]
            trace = power.loc[int(start - before) : int(start + after)]['v']
           
        #if can_id in mapper.get('trans'):
        #    start = trans_power.index[(trans_power['time'] - time).abs().idxmin()]
        #    trace = trans_power.iloc[int(start - before) : int(start + after)]['v']
           
        #if can_id in mapper.get('abs'):
        #    start = abs_power.index[(abs_power['time'] - time).abs().idxmin()]
        #    trace = abs_power.iloc[int(start - before) : int(start + after)]['v']
            
        if (not np.any(np.isnan(trace))):
                
                #traces_high.append(trace_high.values)
                traces.append(trace.values)
                IDS.append(can_id)
                timestamp.append(time)
                
        if (len(traces) >= 100) or (can_indices[-1] == idx):
            df_trace = pd.DataFrame(traces)
            df_id = pd.DataFrame(IDS)
            df_time = pd.DataFrame(timestamp)
            df_trace = pd.concat([df_trace, df_id, df_time], axis=1)
            df_trace.to_csv(f_handle, header=None, index=False, mode='a')
            print("Written Fingerprint-ID samples to ", "fingerprints")
            traces= []
            IDS = []
            timestamp=[]
