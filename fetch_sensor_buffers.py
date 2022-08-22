import pandas as pd 
import numpy as np 
import math
import sys
from sklearn.preprocessing import normalize

def norm(x):
    return (x - min(x))/(max(x) - min(x))

def prepare_buffers(df, savepath, length):

    start = 0
    offset = int(length) 
    steps = math.floor(len(df)/100)
    print("Number of steps: ", steps)
    print("Sample length: ", df.size)
    buffs = []
    labels = []
    i = 0
    #x = df['v'].values
    #normalized = (x - min(x))/(max(x) - min(x))
    #df.iloc[:,1] = (df.iloc[:,1] - df.iloc[:,1].min())/(df.iloc[:,1].max() - df.iloc[:,1].min())
    #df['v'] = normalized
    #print('min = {}, max = {}'.format(min(df.iloc[:,1]), max(df.iloc[:,1])))
    while (i <= steps):

        if (start+offset) > df.size:
            break
      
        buff = df.iloc[start: start+offset, 1].values
        start = start+offset
        if  np.all(np.isfinite(buff)):
            buffs.append(buff)
            i = i + 1
    
    
    print("Buffers ")
    df_buffs = pd.DataFrame(buffs)
    df_buffs = df_buffs.dropna(how='any', axis=0)
    #buffs = buffs / np.linalg.norm(buffs, axis=-1)[:, np.newaxis]
    print(df_buffs.head())
    df_buffs.to_csv(savepath, index=False)


if __name__ == '__main__':

    filepath = sys.argv[1]
    savepath = sys.argv[2]
    length = sys.argv[3]
    df = pd.read_csv(filepath, sep=",", header=None)
    #df.columns = ['time', 'v']
    #labels = df.iloc[:, 0].values
    #df_labels = pd.DataFrame(labels)
    #df_labels.to_csv("/media/gdls/gdls-data/workspace/1511448286-power2-labels.csv")
    prepare_buffers(df, savepath, length)
