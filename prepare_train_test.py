import pandas as pd
import numpy as np
import glob
import os
import sys

date = sys.argv[1]
direc = sys.argv[2]
#date = "2018-05-31"
#direc = "/media/gdls/gdls-data/workspace"
files = os.path.join(direc, date, 'fingerprints','*power2-visit3.csv')
merged_file = os.path.join(direc, date, '_'.join(['train','test',date,'visit3.csv']))
print(files)
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
