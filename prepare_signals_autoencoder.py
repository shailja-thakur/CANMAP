import pandas as pd
import numpy as np
import glob
import os
import sys

atvp = sys.argv[1]
direc = sys.argv[2]
signal = sys.argv[3]
#date = "2018-05-31"
#direc = "/media/gdls/gdls-data/workspace"
files = os.path.join(direc, '2018-05-*',  atvp + '_' + signal + '.csv')
merged_file = os.path.join(direc, 'signlas_training','_'.join(['train','test',atvp, signal,'.csv']))
print(glob.glob(files))
print(merged_file)

df_merged = pd.DataFrame()
for f in glob.glob(files):
    print(f)
    chunks = pd.read_csv(f, chunksize=100000, header=None)
    for chunk in chunks:
        df_merged = pd.concat([df_merged, chunk], axis=0)
    print(df_merged.shape)

#permutate the rows of the dataset
df_merged = df_merged.sample(frac=1).reset_index(drop=True)
print('Writing the prepared dataset...')
df_merged.to_csv(merged_file, index=False, header=None)
print("Done!")

