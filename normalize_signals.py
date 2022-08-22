import pandas as pd
import numpy as np
import sys
import os
from data_utils import l2_norm
import inspect
from sklearn import preprocessing
import glob

def norm(df, filename_norm):
    time = df.iloc[:,0]
    df = df.iloc[:,1:]
    print(df.head())
    traces = (df - df.mean())/df.std()
    traces[len(df.columns)+1] = time
    df_norm = pd.DataFrame(traces)
    
    print("normalized signal ",df_norm.head())
    print("writing")
    df_norm.to_csv(filename_norm, mode='a', index=False, header=False)
   
    print('Written norm traces to - > ', filename_norm)


if __name__ == "__main__":


    date = sys.argv[1]
    signal = sys.argv[2]
    print(date)
    for f in glob.glob("/media/gdls/gdls-data/workspace/"+ date +"/decoded_signals/"+"*"+ signal + ".csv" ):
        try:
                filename = f.split("/")[7]
                print(filename)
                #for f in glob.glob(filepath+'/channels_text/*'):
                df = pd.read_csv(f, header=None)
                filename_norm = "/media/gdls/gdls-data/workspace/"+ date + "/normalized_other_signals/" + 'normalized_' + signal +'.csv'
                norm(df, filename_norm)
       
        except Exception as e:
                print(e)
    print("Done!")
