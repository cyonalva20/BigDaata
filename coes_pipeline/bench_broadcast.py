"""Optimizacion medida: join A2 x A3 con y sin broadcast. Mide mediana de N corridas.
Uso: LAKE_ROOT=lake_test python bench_broadcast.py"""
import os, time, json, statistics
from pyspark.sql import SparkSession, functions as F
ROOT = os.environ.get("LAKE_ROOT", "lake_test").rstrip("/"); N = int(os.environ.get("N_RUNS", "3"))
spark = (SparkSession.builder.appName("bench-broadcast").config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "8"))
         .config("spark.driver.memory", os.environ.get("DRIVER_MEM", "4g")).config("spark.sql.adaptive.enabled", "false").getOrCreate())   # AQE off: aisla el efecto del broadcast
spark.sparkContext.setLogLevel("ERROR")
a2 = spark.read.parquet(f"{ROOT}/silver/a2_despacho_ejecutado").withColumn("fecha_despacho", F.to_date(F.expr("fecha_hora - interval 1 minute")))
a3 = spark.read.parquet(f"{ROOT}/silver/a3_costos_variables").withColumnRenamed("fecha", "fecha_despacho").select("fecha_despacho", "central", "costo_variable_s_kwh")

def medir(nombre, thr, usar_hint):
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", thr)
    j = a2.join(F.broadcast(a3) if usar_hint else a3, ["central", "fecha_despacho"], "left")
    plan = j._jdf.queryExecution().executedPlan().toString()
    estrategia = "BroadcastHashJoin" if "BroadcastHashJoin" in plan else ("SortMergeJoin" if "SortMergeJoin" in plan else "otro")
    j.write.format("noop").mode("overwrite").save()                        # calentamiento (cache de archivos)
    t = []
    for _ in range(N):
        t0 = time.perf_counter(); j.write.format("noop").mode("overwrite").save(); t.append(time.perf_counter() - t0)
    return {"caso": nombre, "estrategia": estrategia, "tiempos_s": [round(x, 2) for x in t], "mediana_s": round(statistics.median(t), 2)}

sin = medir("sin_optimizar", -1, False)           # fuerza SortMergeJoin (shuffle de ambas tablas)
con = medir("con_broadcast", 10 * 1024 * 1024, True)
res = {"filas_a2": a2.count(), "filas_a3": a3.count(), "sin": sin, "con": con,
       "mejora_pct": round(100 * (1 - con["mediana_s"] / sin["mediana_s"]), 1)}
print(json.dumps(res, indent=2))
if not ROOT.startswith("s3"):
    os.makedirs(f"{ROOT}/logs", exist_ok=True); open(f"{ROOT}/logs/bench_broadcast.json", "w").write(json.dumps(res, indent=2))
spark.stop()
