import pandas as pd
import numpy as np


df_decoded = pd.read_excel("/media/gdls/gdls-data/workspace/J1939DA_201704.xls", sheet_name="SPNs & PGNs")

temp = df_decoded.iloc[3: df_decoded.size]
cols = df_decoded.iloc[2].values

df_decoded = temp
df_decoded.columns = cols

print(df_decoded.head())

df_decoded.to_csv('/media/gdls/gdls-data/workspace/SPNs_and_PGNs.csv')
