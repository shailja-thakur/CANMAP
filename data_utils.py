import math
from collections import Iterable
from sklearn import preprocessing
import pandas as pd
import numpy as np

def save_data(df, log_dir, filename, prefix):

    if not os.path.exists(log_dir):
        os.makedir(log_dir)
    df_meta.to_csv('.'.join([filename, prefix]), index=False)

def max_no_samples(traces):
    return max(len(t) for t in traces)
def min_no_samples(traces):
    return min(len(t) for t in traces)

def l2_norm(traces):
    traces  = preprocessing.normalize(traces, norm='l2')
#     norm2 = normalize(traces[:,np.newaxis], axis=0).ravel()
    return traces

def padding(segments):
    sampled_traces = []
    max_sample_count = max_no_samples(segments)
    print('Maximum smaples count',max_sample_count)
    print(np.array(segments).shape)
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

def flatten(items):
    """Yield items from any nested iterable; see REF."""
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x

def filename(date, number, prefix, channel=None, meta=False,):

    name = '-'.join([date, number])
    name = '--'.join([name, '10MSPS--400mV--CAN-and-powertrace--channel'])
    if channel:
        name = '-'.join([name, channel])
    if meta:
        name = '-'.join([name, 'meta'])
    name = '.'.join([name, prefix])
    return name

