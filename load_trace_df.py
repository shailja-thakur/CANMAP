import sys
import pandas as pd
import numpy as np
from pandas import DataFrame
import os
file_path = "/media/gdls/gdls-data/gdls-building10-test-track/2017-11-23/digitizer-data"
file_name = sys.argv[1]

df = DataFrame(np.fromfile(os.path.join(file_path, file_name)))
print("--->",df.head())

# test-track

#file_path = '/media/gdls/gdls-data/gdls-rhr-parking-lot/2017-07-25'
#file_name = 'experiment-1.4-1501008558-redislog.csv'

df.to_csv("/media/gdls/gdls-data/workspace/1511448286-traces.csv")
