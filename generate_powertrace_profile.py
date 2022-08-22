import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.signal import resample as resample
import sqlite3
import glob
import os
import sys

date = sys.argv[1]
ecus = ['atvp2', 'atvp3', 'atvp4']
for f in glob.glob('/media/gdls/gdls-data/workspace/'+ date + '/normalized_signals/*power1.csv'):
    timestamp = f.split('/')[7].split('-')[0]
    if not os.path.isfile("/media/gdls/gdls-data/workspace/" + date + "/fingerprints/" +date + "-" + timestamp + "-visit3.csv"):
      
      print(timestamp[10:])
      engine_power1 = pd.read_csv('/media/gdls/gdls-data/workspace/'+ date + '/normalized_signals/' + timestamp +"-atvp2-power1.csv")
      #atvp2_power2 = pd.read_csv("/home/s7thakur/scripts/" + timestamp + "-atvp2-power2.csv")

      trans_power1 = pd.read_csv('/media/gdls/gdls-data/workspace/'+ date + '/normalized_signals/' + timestamp + "-atvp3-power1.csv")
      #atvp3_power2 = pd.read_csv("/home/s7thakur/scripts/" + timestamp + "-atvp3-power2.csv")

      abs_power1 = pd.read_csv('/media/gdls/gdls-data/workspace/'+ date + '/normalized_signals/' + timestamp + "-atvp4-power1.csv")
      #atvp4_power2 = pd.read_csv("/home/s7thakur/scripts/" + timestamp + "-atvp4-power2.csv")

      engine_power1.columns  = ['time', 'v']
      #atvp2_power2.columns  = ['time', 'v']

      trans_power1.columns  = ['time', 'v']
      #atvp3_power2.columns = ['time', 'v']

      abs_power1.columns = ['time', 'v']
      #atvp4_power2.columns  = ['time', 'v']

      conn = sqlite3.connect("/media/gdls/gdls-data/visit3/" + date + "/" + timestamp[10:] + "-binaryredislog.db")
      cur = conn.cursor()

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
      mapper['engine'] = engine.values
      mapper['trans'] = trans.values
      mapper['abs'] = ABS.values


      before = 49
      after = 250
      traces_high = []
      traces_low = []
      IDS =  []
      for idx in cans.index:
        id = can.iloc[idx]["id"]
        time = can.iloc[idx]["timestamp"]
        
        if id in mapper.get('engine'):
            start = engine_power1.index[(engine_power1['time'] - time).abs().idxmin()]
            trace_low = engine_power1.loc[int(start - before) : int(start + after)]['v']
        
    #             trace_high = atvp2_power2.iloc[int(idx - before) : int(indx + after)]['v']
        if id in mapper.get('trans'):
            start = trans_power1.index[(trans_power1['time'] - time).abs().idxmin()]
            trace_low = trans_power1.iloc[int(start - before) : int(start + after)]['v']
    #             trace_high = atvp3_power2.iloc[int(idx - before) : int(indx + after)]['v']
        if id in mapper.get('abs'):
            start = abs_power1.index[(abs_power1['time'] - time).abs().idxmin()]
            trace_low = abs_power1.iloc[int(start - before) : int(start + after)]['v']
    #             trace_high = atvp4_power2.iloc[int(idx - before) : int(indx + after)]['v']
        if (not np.any(np.isnan(trace_low))):
        
    #                 traces_high.append(trace_high)
                traces_low.append(trace_low)
                IDS.append(id)


      # traces_high = (traces_high - np.mean(traces_high, axis=1))/np.std(traces_high, axis=1)
      # traces_low_n = (traces_low - np.mean(traces_low, axis=1))/np.std(traces_low, axis=1) 

      # df_high = pd.DataFrame(traces_high)
      df_low = pd.DataFrame(traces_low)

      df_id = pd.DataFrame(ids)
      df_low = pd.concat([df_low, df_id], axis=1)
      # df_high = pd.concat([df_high, df_id], axis=1)

      # df.to_csv("/media/gdls/gdls-data/workspace/new_10_100_traces_events_gdls_visit2.csv", index=False, header=None, mode='a')
      df_low.to_csv("/media/gdls/gdls-data/workspace/" + date +"/fingerprints/" +date + "-" + timestamp + "-visit3.csv", index=False, header=None)
      print("Written Fingerprint-ID samples to ", timestamp + "-visit3.csv" )
                


