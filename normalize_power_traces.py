import pandas as pd
import numpy as np
import sys
import os
from data_utils import l2_norm
import inspect
from sklearn import preprocessing
import glob

SCALE = 2^15
def norm(df, filename_norm):
    print(df.head()) 
    #traces = (df[[1]] - df[[1]].mean())/df[[1]].std()
    traces = df[[1]] / 32768.0
    df_norm = pd.DataFrame(traces)
    df_norm.columns = ['v']
    df_norm['timestamp'] = df[[0]]
    print("normalized signal ",df_norm.head())
    print("writing")
    np.save(filename_norm, df_norm.as_matrix())
    #df_norm.to_csv(filename_norm, index=False)
    print("Done!")
    print('Written norm traces to - > ', filename_norm)


if __name__ == "__main__":

    
    #date = sys.argv[1]
    #print(date)
    ecus = ['atvp2', 'atvp3', 'atvp4']
    for f in glob.glob("/media/gdls/gdls-data/workspace/visit4-trace/"+"*-power2.csv" ):
        try:
                
                print(f)
                #for f in glob.glob(filepath+'/channels_text/*'):
                df = pd.read_csv(f, header=None)
                filename_norm = "/media/gdls/gdls-data/workspace/visit4-trace" + "/normalized_signals/" + 'normalized-' + os.path.basename(f).split('.')[0] + '.npy'
                norm(df, filename_norm)
        except Exception as e:
                print(e)
