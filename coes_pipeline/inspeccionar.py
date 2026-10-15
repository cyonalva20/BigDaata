import glob, pandas as pd, pyarrow.parquet as pq
for f in sorted(glob.glob("*/*.parquet")):
    pf = pq.ParquetFile(f)
    print("\n==", f, "| filas:", pf.metadata.num_rows)
    print(pf.schema_arrow)
for f in sorted(glob.glob("*/*.csv")):
    df = pd.read_csv(f, nrows=3, encoding="utf-8-sig")
    print("\n==", f); print(df.dtypes); print(df.head(2).to_string())
