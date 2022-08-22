import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.signal import resample as resample
import sqlite3
import glob
import os
import sys

#date = sys.argv[1]
ecus = ['atvp2', 'atvp3', 'atvp4']
for f in glob.glob('/media/gdls/gdls-data/workspace/visit4-trace/*-binaryredislog.db'):
    timestamp = f.split('/')[6].split('-')[0]
    if not os.path.isfile("/media/gdls/gdls-data/workspace/visit4-trace/" + "/fingerprints-*"+ os.path.basename(f)+".npy"):
      
      print(timestamp)
      engine_power1 = pd.DataFrame(np.load(('/media/gdls/gdls-data/workspace/visit4-trace/normalized_signals/normalized-' + timestamp +"-atvp2-power1.npy")))
      #atvp2_power2 = pd.read_csv("/home/s7thakur/scripts/" + timestamp + "-atvp2-power2.csv")
      engine_power2 = pd.DataFrame(np.load(('/media/gdls/gdls-data/workspace/visit4-trace/normalized_signals/normalized-' + timestamp +"-atvp2-power2.npy")))

      trans_power1 = pd.DataFrame(np.load(('/media/gdls/gdls-data/workspace/visit4-trace/normalized_signals/normalized-' + timestamp + "-atvp3-power1.npy")))
      #atvp3_power2 = pd.read_csv("/home/s7thakur/scripts/" + timestamp + "-atvp3-power2.csv")

      trans_power2 = pd.DataFrame(np.load(('/media/gdls/gdls-data/workspace/visit4-trace/normalized_signals/normalized-' + timestamp + "-atvp3-power2.npy")))

      abs_power1 = pd.DataFrame(np.load(('/media/gdls/gdls-data/workspace/visit4-trace/normalized_signals/normalized-' + timestamp + "-atvp4-power1.npy")))
      #atvp4_power2 = pd.read_csv("/home/s7thakur/scripts/" + timestamp + "-atvp4-power2.csv")
      abs_power2 = pd.DataFrame(np.load(('/media/gdls/gdls-data/workspace/visit4-trace/normalized_signals/normalized-' + timestamp + "-atvp4-power2.npy")))

      c = [ 'v','time']
      engine_power1.columns  = c
      engine_power2.columns  = c

      trans_power1.columns  = c
      trans_power2.columns = c

      abs_power1.columns = c
      abs_power2.columns  = c

      conn = sqlite3.connect(f)
      cur = conn.cursor()
      print(engine_power1.head())
      print(abs_power1.head())
      print(trans_power1.head())
      # ECU-ID map
      can = pd.read_sql_query("select timestamp, id, pgn, sa_name, databyte1, databyte2, databyte3, databyte4, databyte6, databyte7, databyte8 from 'decoded_test';", conn)
      engine = pd.read_sql_query("select distinct(id) from decoded_test where sa_name like 'Engine #1';", conn)
      trans =  pd.read_sql_query("select distinct(id) from decoded_test where sa_name like 'Transmission #1';", conn)
      ABS =  pd.read_sql_query("select distinct(id) from decoded_test where sa_name like 'Brakes - System Controller';", conn)
      # Can Messages after filtering the duplicate messages
      cans = can.drop_duplicates(subset=['timestamp','databyte1', 'databyte2', 'databyte3', 'databyte4', 'databyte6', 'databyte7', 'databyte8'], keep='first')

      print('Engine IDs : ', engine.values)
      print('Trans IDs : ', trans.values)
      print('ABS IDs : ', ABS.values)

      mapper = {}
      mapper['engine'] = engine.values.flatten().tolist()
      mapper['trans'] = trans.values.flatten().tolist()
      mapper['abs'] = ABS.values.flatten().tolist()
      print(cans.head(10))
      can_indices = cans.index[cans['id'].isin(sum(mapper.values(), []))]
      print(can_indices)
      before = 50
      after = 250
      traces_high = []
      traces_low = []
      IDS =  []
      for idx in cans.index:
        can_id = can.iloc[idx]["id"]
        time = can.iloc[idx]["timestamp"]
        #print(time)
        if can_id in mapper.get('engine'):

            start_low = engine_power1.index[(engine_power1['time'] - time).abs().idxmin()]
            trace_low = engine_power1.loc[int(start_low - before) : int(start_low + after)]['v']
            start_high = engine_power2.index[(engine_power2['time'] - time).abs().idxmin()]
            trace_high = engine_power2.iloc[int(start_high - before) : int(start_high + after)]['v']
            print(start_low)
        if can_id in mapper.get('trans'):
            start_low = trans_power1.index[(trans_power1['time'] - time).abs().idxmin()]
            trace_low = trans_power1.iloc[int(start_low - before) : int(start_low + after)]['v']
            start_high = trans_power2.index[(trans_power2['time'] - time).abs().idxmin()]
            trace_high = trans_power2.iloc[int(start_high - before) : int(start_high + after)]['v']
            #print(len(trace_low), len(trace_high))
            print(start_low)
        if can_id in mapper.get('abs'):
            start_low = abs_power1.index[(abs_power1['time'] - time).abs().idxmin()]
            trace_low = abs_power1.iloc[int(start_low - before) : int(start_low + after)]['v']
            start_high = abs_power2.index[(abs_power2['time'] - time).abs().idxmin()]
            trace_high = abs_power2.iloc[int(start_high - before) : int(start_high + after)]['v']
            print(len(trace_low), len(trace_high))
            print(start_low)
        #if (not np.any(np.isnan(trace_low))) and (not np.any(np.isnan(trace_high))):
        #
        #        traces_high.append(trace_high.values)
        #        traces_low.append(trace_low.values)
        #        IDS.append(can_id)
        #if len(traces_low) == 5:
        #    break


      # traces_high = (traces_high - np.mean(traces_high, axis=1))/np.std(traces_high, axis=1)
      # traces_low_n = (traces_low - np.mean(traces_low, axis=1))/np.std(traces_low, axis=1) 

      df_high = pd.DataFrame(traces_high)
      df_low = pd.DataFrame(traces_low)
      df_id = pd.DataFrame(IDS)
      df_low = pd.concat([df_low, df_id], axis=1)
      df_high = pd.concat([df_high, df_id], axis=1)
      print(df_low.head())
      
      #df.to_csv("/media/gdls/gdls-data/workspace/new_10_100_traces_events_gdls_visit2.csv", index=False, header=None, mode='a')
      np.save(df_low.values, "/media/gdls/gdls-data/workspace/visit4-trace/fingerprints/" +"fingerprints-power1-"+ os.path.basename(f), index=False, header=None)
      np.save(df_high.values, "/media/gdls/gdls-data/workspace/visit4-trace/fingerprints/" +"fingerprints-power2-"+ os.path.basename(f), index=False, header=None)
      print("Written Fingerprint-ID samples to ", "fingerprints-"+ od.path.basename(f))
                


