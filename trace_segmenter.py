import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.signal import resample as resample
import sqlite3
import glob
import sys
import os

date = sys.argv[1]
before = sys.argv[2]
after = sys.argv[3]
chunkssize = sys.argv[4]
base_path = '/media/gdls/gdls-data/workspace'
db = '/media/gdls/gdls-data/visit3'
file_path = os.path.join(base_path, date, 'normalized_signals', '*power2.csv')

ecus = {'atvp2':'Engine #1', 'atvp3':'Transmission #1', 'atvp4':'Brakes - System Controller'}
print(file_path)
for f in glob.glob(file_path):
    timestamp = f.split('/')[7].split('-')[0]

    fingerprints_path = os.path.join(base_path, date, 'fingerprints', '-'.join([timestamp[10:], 'visit3.csv']))
    db_path = os.path.join(db , date , '-'.join([timestamp[10:] , "binaryredislog.db"]))

    #print(fingerprints_path)
    #print(db_path)
    if not os.path.isfile(fingerprints_path):
      print('Generating Fingerprints for :', fingerprints_path)
      conn = sqlite3.connect(db_path)
      cur = conn.cursor()
      can = pd.read_sql_query("select timestamp, id, pgn, sa_name, databyte1, databyte2, databyte3, databyte4, databyte6, databyte7, databyte8 from 'decoded_test';", conn)
      # Can Messages after filtering the duplicate messages
      cans = can.drop_duplicates(subset=['timestamp','databyte1', 'databyte2', 'databyte3', 'databyte4', 'databyte6', 'databyte7', 'databyte8'], keep='first')
      cans = cans.reset_index()
      # create the fingerprints for the ecus at the time 
      traces_high = []
      traces_low = []
      IDS =  []

      for ecu in ecus.keys():

        normalized_signals = os.path.join(base_path, date, 'normalized_signals', '-'.join([timestamp, ecu, 'power1.csv']))
        print(normalized_signals)
        #print(timestamp[10:])

        power = pd.read_csv(normalized_signals, chunksize=int(chunkssize))
        #atvp2_power2 = pd.read_csv("/home/s7thakur/scripts/" + timestamp + "-atvp2-power2.csv")

       
        #atvp2_power2.columns  = ['time', 'v']
        # ECU-ID map
        ids = pd.read_sql_query("select distinct(id) from decoded_test where sa_name like '"+ ecus.get(ecu) + "';", conn)
        #print(ids.values)
        #print('Message IDs for '+ ecu + ':' + ids.values)
        meta_begin = 0      
        for chunk in power:
            chunk.columns = ['v', 'time']    
            chunk = chunk.reset_index()
            #print(chunk.head())
            meta_idx = cans.index[(cans['timestamp']-chunk.iloc[-1]['time']).abs().idxmin()]
            #print('meta index', meta_idx)
            meta_buff = cans.iloc[meta_begin:meta_idx+1]
            meta_buff = meta_buff.reset_index()          
            meta_begin = meta_idx+1
            #print(cans.iloc[meta_idx]['timestamp'], chunk.iloc[-1]['time'])
            for idx in meta_buff.index:
              id = meta_buff.iloc[idx]["id"]
              time = meta_buff.iloc[idx]["timestamp"]
              start = chunk.index[(chunk['time'] - time).abs().idxmin()]
              trace_low =chunk.loc[int(start - int(before)) : int(start + int(after))]['v']
              #print(id, chunk.iloc[start]['time'], time)
              #print(len(trace_low))

              if (not np.any(np.isnan(trace_low))) and (trace_low.size == int(int(after) + int(before) + 1)):
                  traces_low.append(trace_low.values)
                  IDS.append(id)

      df_low = pd.DataFrame(traces_low)
      df_id = pd.DataFrame(IDS)
      print(df_low.shape, df_id.shape)
      df_low = pd.concat([df_low, df_id], axis=1)
      df_low.to_csv(fingerprints_path, index=False, header=None)
      print("Written Fingerprint-ID samples to ", fingerprints_path)



