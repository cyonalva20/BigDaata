import pandas as pd
print(pd.read_parquet("lake_real/gold/despacho_central_30min").dtypes)
print(pd.read_parquet("lake_real/silver/b3_reprograma").head(3).T)
print(pd.read_parquet("lake_real/silver/b4_pr06").dtypes)