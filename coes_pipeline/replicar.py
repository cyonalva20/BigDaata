"""Replica A1-A4 K veces SOLO para la prueba de escalabilidad (nunca para analisis ni modelo).
Las claves se hacen unicas con un sufijo _rK en central/barra, para no crear duplicados.
Uso: python replicar.py <LAKE_ORIGEN> <LAKE_DESTINO> <K>"""
import sys
from pyspark.sql import SparkSession, functions as F
src, dst, K = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/"), int(sys.argv[3])
spark = SparkSession.builder.appName(f"replicar-x{K}").config("spark.driver.memory", __import__("os").environ.get("DRIVER_MEM", "4g")).getOrCreate(); spark.sparkContext.setLogLevel("ERROR")
ks = F.explode(F.sequence(F.lit(0), F.lit(K - 1))).alias("k")
for carpeta, col in [("a1_despacho_programado", "central"), ("a2_despacho_ejecutado", "central"),
                     ("a3_costos_variables", "central"), ("a4_costos_marginales", "barra")]:
    df = spark.read.parquet(f"{src}/bronze/{carpeta}").select("*", ks)
    df = df.withColumn(col, F.concat(F.col(col), F.lit("_r"), F.col("k").cast("string")))
    if carpeta.startswith("a4"):
        df = df.withColumn("barra_id", F.concat(F.col("barra_id"), F.lit("_r"), F.col("k").cast("string")))
    df.drop("k").write.mode("overwrite").parquet(f"{dst}/bronze/{carpeta}")
    print(carpeta, "replicado x", K)
# B es pequena: se COPIA BYTE A BYTE (no pasar por Spark: B1.descripcion tiene saltos de linea y se corrompe al reescribir)
import shutil, subprocess, os
for b in ["b1_mantenimientos", "b2_potencia_indisponible", "b3_reprograma", "b4_pr06"]:
    if src.startswith("s3"):
        subprocess.run(["aws", "s3", "sync", f"{src}/bronze/{b}", f"{dst}/bronze/{b}"], check=True)
    else:
        shutil.copytree(f"{src}/bronze/{b}", f"{dst}/bronze/{b}", dirs_exist_ok=True)
spark.stop()
