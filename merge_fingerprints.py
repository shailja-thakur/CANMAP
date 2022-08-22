import pandas as pd
import numpy as np
import os
import glob
import sys
import itertools
import operator
path = sys.argv[1]
channel = sys.argv[2]
def map_ECU_ID_visit4(labels):

    engine =  [217056256,217056000,419356672,419361280,419361024,419360768,419356416,419360256,419360512,419362560,419362304,419355392,419351040,419348736,419357696,419348480,150922496]
    trans  =  [217055747,418383107,201326595,419362819,417988611,419351043]
    ABS = [419348235, 201326603, 418382091]

    labels = pd.DataFrame(labels.values, columns=['id'])
    #labels = labels['id'].astype(int)
    print(labels.head())
    #print(labels.index[labels['id'].isin(engine)])
    labels.loc[labels.index[labels['id'].isin(engine)], 'id'] = 1
    labels.loc[labels.index[labels['id'].isin(trans)], 'id'] = 2
    labels.loc[labels.index[labels['id'].isin(ABS)], 'id'] = 3
    return labels['id'].values


def merge_ecus(path):
    df_f =  pd.DataFrame()
    print(path)
    for f in glob.glob(path+ '*'+ channel +'*.csv'):
        print(f)
        df = pd.read_csv(f)
        df.columns = [i for i in range(0, df.shape[1])]
        df = df.T.drop_duplicates().T
        df = df.dropna()
        print(df.head())
    
        labels = map_ECU_ID_visit4(df.iloc[:,-1])
        print(labels)
        
        #print(len(labels), len(df.iloc[:,-1]))
        df = df.iloc[:,0:300]
        
        df = pd.concat([df, pd.DataFrame(labels)], axis=1)
        print(df.head())
        df_f = pd.concat([df, df_f], axis=0)
    #print(len(df_f))
    np.save(path + np.join(['canmap', 'visit4',channel, 'fingerprints.npy']), df_f.values)


def merge_ecus_windowed(path):
    df_f =  pd.DataFrame()
    print(path)
    for f in glob.glob(path+ '*' + channel +'*.csv'):
        print(f)
        df = pd.read_csv(f)
        df.columns = [i for i in range(0, df.shape[1])]
        df = df.T.drop_duplicates().T
        df = df.dropna()
        print(df.head())

        labels = map_ECU_ID_visit4(df.iloc[:,-1])
        print(labels)

        #print(len(labels), len(df.iloc[:,-1]))
        df = df.iloc[:,0:300]
        #print(df.head())
        df = pd.concat([df, pd.DataFrame(labels)], axis=1)
        print(df.head())
        df_f = pd.concat([df, df_f], axis=0)
    #print(len(df_f))
    np.save(path +np.join(['canmap', 'windowed','visit4',channel, 'fingerprints.npy']), df_f.values)


def merge_power_channels(path):
     
    
    files = glob.glob(path + '*.csv')
    times = [os.path.basename(f).split('-')[3] for f in files]
    times = np.unique(times)
    ecus = ['engine', 'trans','abs']
    merge_concat = pd.DataFrame()
    merge_crossed = pd.DataFrame()
    #files = list(itertools.product(*power))
    for t  in times:
        for e in ecus:
            power1 = pd.read_csv('-'.join(['fingerprints',e,'power1', t, 'binaryredislog.csv']), header=None)
            power2 = pd.read_csv('-'.join(['fingerprints', e, 'power2', t, 'binaryredislog.csv']), header=None)
            print('-'.join(['fingerprints',e,'power1', t, 'binaryredislog.csv'], '-'.join(['fingerprints',e,'power2', t, 'binaryredislog.csv'])))
            time1 = power1.iloc[:,-1]
            time2 = power2.iloc[:,-1]
            IDs = power1.iloc[:,-2]
            
            power1 = power1.iloc[:,0:300]
            power2 = power2.iloc[:,0:300]
            #print('Individual channels:')
            #print(power1.head())
            #print(power2.head())
            power1 = power1.reset_index(drop=True)
            power2 = power2.reset_index(drop=True)         
            #print(power1.head())
            power1.columns = [i for i in np.arange(0, 2*power1.shape[1], 2)]
            power2.columns = [i for i in np.arange(1, 2*power2.shape[1], 2)]
            power1['time'] = time1
            power2['time'] = time2
            
            df = pd.merge(power1, power2, on='time')
            time = df['time']
            
            df_concat = df
            df_concat['ID'] = IDs
           
            print('Merged channels cocncatenated :-')
            print(df_concat.head())
            df_cross = df[[i for i in range(0, 2*power1.shape[1]-2)]]
            df_cross['ID'] = IDs
            print('Merged channels crossed :-')
            print(df_cross.head())
            #df3 = df2[[i for i in range(0, 2*df2.shape[1])]]
            
            merge_crossed = pd.concat([merge_crossed, df_cross], axis=0)
            merge_concat = pd.concat([merge_concat, df_concat], axis=0)

    print(merge_crossed.shape, merge_concat.shape)
    # remove missing data, Nans, duplicates
    merge_crossed = merge_crossed.T.drop_duplicates().T
    merge_crossed = merge_crossed.dropna()

    merge_concat = merge_concat.T.drop_duplicates().T
    merge_concat = merge_concat.dropna()
    np.save('canmap-visit4-fingerprints-power1-power2-crossed.npy',merge_crossed)
    np.save('canmap-visit4-fingerprints-power1-power2-concat.npy', merge_concat)

merge(path)
#merge_power_channels(path)
    
