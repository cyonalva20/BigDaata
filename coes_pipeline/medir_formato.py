"""Mide Parquet vs CSV con el MISMO dato (Silver A2): tamano en disco y tiempos de lectura.
Uso: python medir_formato.py lake_real     (escribe en <lake>/logs/medicion_formato.json)"""
import os, sys, time, json, statistics, shutil
from pyspark.sql import SparkSession, functions as F
ROOT = sys.argv[1] if len(sys.argv) > 1 else "lake_real"
TMP = f"{ROOT}/_tmp_formato"; N = 3
spark = (SparkSession.builder.master("local[8]").config("spark.driver.memory", os.environ.get("DRIVER_MEM", "4g"))
         .config("spark.sql.shuffle.partitions", "8").getOrCreate()); spark.sparkContext.setLogLevel("ERROR")
def du(p): return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(p) for f in fs)
def med(fn):
    fn(); ts = []                                              # 1 calentamiento + N medidas
    for _ in range(N): t = time.perf_counter(); fn(); ts.append(time.perf_counter() - t)
    return round(statistics.median(ts), 2)
shutil.rmtree(TMP, ignore_errors=True)
pq = f"{ROOT}/silver/a2_despacho_ejecutado"
df = spark.read.parquet(pq); n = df.count(); r = {"filas": n}
t = time.perf_counter(); df.write.option("header", True).partitionBy("anio", "mes").csv(f"{TMP}/csv"); r["escritura_csv_s"] = round(time.perf_counter() - t, 2)
t = time.perf_counter(); df.write.parquet(f"{TMP}/pq_sin_particion"); r["escritura_parquet_sin_particion_s"] = round(time.perf_counter() - t, 2)
r["bytes"] = {"parquet_particionado_silver": du(pq), "csv_particionado": du(f"{TMP}/csv"), "parquet_sin_particion": du(f"{TMP}/pq_sin_particion")}
P = spark.read.parquet(pq); C = spark.read.schema(df.schema).option("header", True).csv(f"{TMP}/csv"); U = spark.read.parquet(f"{TMP}/pq_sin_particion")
r["lectura_s"] = {
  "una_columna_todo_parquet": med(lambda: P.agg(F.sum("potencia_ejecutada_mw")).collect()),
  "una_columna_todo_csv": med(lambda: C.agg(F.sum("potencia_ejecutada_mw")).collect()),
  "un_mes_parquet_particionado": med(lambda: P.filter("anio = 2024 AND mes = 11").count()),
  "un_mes_csv_particionado": med(lambda: C.filter("anio = 2024 AND mes = 11").count()),
  "un_mes_parquet_sin_particion": med(lambda: U.filter("anio = 2024 AND mes = 11").count())}
r["repeticiones"] = N
os.makedirs(f"{ROOT}/logs", exist_ok=True); json.dump(r, open(f"{ROOT}/logs/medicion_formato.json", "w"), indent=2)
print(json.dumps(r, indent=2)); shutil.rmtree(TMP, ignore_errors=True); spark.stop()
