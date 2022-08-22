import os
import sys
import pandas as pd
from collections import Iterable
import numpy as np
import math
from sklearn import preprocessing

fileno = sys.argv[1]
module = sys.argv[2]

filepath_events = '.'.join(['-'.join([fileno, 'atvp2', 'can']), 'csv'])
filepath_traces = '.'.join(['-'.join([fileno, module, 'power2']), 'csv'])
filepath_seg = "/media/gdls/gdls-data/workspace/mapping/"+fileno+"-"+module + "-"+"segments.csv"
filepath_labels = "/media/gdls/gdls-data/workspace/mapping/"+fileno+"-"+module + "-"+"labels.csv"

# offsets to the event
pre_start = 0.000001
post_start = 0.00099

def max_no_samples(traces):
    return max(len(t) for t in traces)

def l2_norm(traces):
    traces  = preprocessing.normalize(traces, norm='l2')
#     norm2 = normalize(traces[:,np.newaxis], axis=0).ravel()
    return traces

def padding(segments):
    sampled_traces = []
    max_sample_count = max_no_samples(segments)
    print('Maximum smaples count',max_sample_count)
    #print(np.array(segments).shape)
    for segment in segments:
#         print(len(segment))
        trace = segment
#         trace = l2_norm(np.asarray(segment))
        if (max_sample_count - len(segment))>0:
            pad = np.zeros(max_sample_count - len(segment))
            trace = np.append(trace, pad)
        sampled_traces.append(list(flatten([trace, len(segment)])))
    print(np.array(sampled_traces).shape)
    
    return sampled_traces, max_sample_count


def map_trace_id(df_events, df_traces):
    # frame length in ms calculated using bus speed, message rate
    # frame length as estimated suing the transmission window
    # for each decoded transmission, fetch the power trace for the corresponding module id
    segments = []
    ids = []
    starts = []
    ends = []
    count = 0
    
    for idx in df_events.index:

        start = math.fabs(df_events.iloc[idx]["time"] - pre_start)
        end = df_events.iloc[idx]["time"] + post_start
        start_index= df_traces.index[(df_traces['time'] - start).abs().argmin()]
        end_index = df_traces.index[(df_traces['time'] - end).abs().argmin()]

        #print(start_index, end_index)
        P_segment = df_traces.loc[start_index: end_index]["val"].values
        
        if P_segment.size > 0 and p_segment:
            segments.append(P_segment)
            ids.append(df_events.iloc[idx]['id'])
            starts.append(start)
            ends.append(end)
        count = count + 1
	
      	if count == 20000:
		break
     print("Saving  power segments to the file: ", filepath_seg)
     #print(segments)
     padded_segments, n = padding(segments)
     norm_segments = l2_norm(padded_segments)
     print(norm_segments.shape)
     columns = list(flatten([['v_'+str(i) for i in range(n)], 'n_samples']))
     df_trace = pd.DataFrame(norm_segments, columns=columns)
     df_label = pd.DataFrame(ids, columns=["id"])
     df_label["start"] = starts
     df_label["end"] = ends
     df_label.to_csv(filepath_labels, header=False, index=False)
     df_trace.to_csv(filepath_seg, header=False, index=False)
     count = 0
     segments.clear()
     ids.clear()
     starts.clear()
     ends.clear()

def flatten(items):
    """Yield items from any nested iterable; see REF."""
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x


if __name__ == "__main__":
    #fileno = sys.argv[1]
    #module = sys.argv[2]
    #filepath_events = '.'.join(['-'.join([fileno, module, 'can']), 'csv'])
    #filepath_traces = '.'.join(['-'.join([fileno, module, 'power2']), 'csv'])
    print('Path to events and traces are as follows:')
    print(filepath_events, filepath_traces)
    #df_events = pd.read_csv(filepath_events)
    #events = np.genfromtxt(filepath_events, delimiter=(17, 1, 17))
    #df_events = pd.DataFrame(events)
    #traces = np.genfromtxt(filepath_traces, delimiter=(17,1, 4))
    #df_traces = pd.DataFrame(traces)
    #df_traces = df_traces.drop(df_traces.columns[[1]], axis=1)
    #df_events = df_events.drop(df_events.columns[[1]], axis=1)
    df_events = pd.read_csv(filepath_events)
    time = df_events.iloc[:,0]
    maps = map(lambda x:int(x.split("    ")[1].split(" ")[1].split('=')[1], 16), df_events.iloc[:,1])
    ids = list(maps)
    df_events = pd.DataFrame({"time":time, "id":ids})
    df_traces = pd.read_csv(filepath_traces)
    #cols = ['time1','time2']
    #df_events.columns = cols
    df_traces.columns = ['time','val']
    print(df_traces.head())
    print(df_events.head())
    map_trace_id(df_events, df_traces)

