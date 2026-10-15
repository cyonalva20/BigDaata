"""Pipeline COES-SEIN: Bronze -> Silver -> Gold.
Uso:  LAKE_ROOT=lake_test python pipeline.py            (local)
      LAKE_ROOT=s3://mi-bucket python pipeline.py       (AWS; en Glue se usa s3://)
Produce <LAKE_ROOT>/logs/run_<escala>.json con filas, calidad y tiempos."""
import os, sys, json, time, datetime as dt
# En AWS Glue los parametros llegan como "--LAKE_ROOT s3://..."; se pasan a variables de entorno (local sigue igual)
for _i, _a in enumerate(sys.argv):
    if _a.startswith("--") and _i + 1 < len(sys.argv) and not sys.argv[_i + 1].startswith("--"):
        os.environ.setdefault(_a[2:], sys.argv[_i + 1])
EN_GLUE = "JOB_NAME" in os.environ
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = os.environ.get("LAKE_ROOT", "lake_test").rstrip("/")
ESCALA = os.environ.get("ESCALA", "1x")
SHUFFLE = os.environ.get("SHUFFLE_PARTITIONS", "8")
USE_BROADCAST = os.environ.get("BROADCAST", "1") != "0"      # BROADCAST=0 -> join A2xA3 sin hint (SortMergeJoin)
ETAPAS = os.environ.get("ETAPAS", "silver_a,silver_b,gold").split(",")   # p. ej. ETAPAS=gold para re-ejecutar solo Gold
BRONZE, SILVER, GOLD, LOGS = (f"{ROOT}/{z}" for z in ("bronze", "silver", "gold", "logs"))

CORES = os.environ.get("CORES", "*")                          # hilos de Spark local: menos hilos = menos memoria concurrente
_b = SparkSession.builder.appName(f"coes-pipeline-{ESCALA}")
if not EN_GLUE:                                                # en Glue, master y memoria los define el servicio
    _b = _b.master(f"local[{CORES}]").config("spark.driver.memory", os.environ.get("DRIVER_MEM", "4g"))
spark = (_b.config("spark.sql.shuffle.partitions", SHUFFLE)
         .config("spark.sql.parquet.compression.codec", "snappy")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

REPORT = {"entorno": "aws_glue" if EN_GLUE else "local", "escala": ESCALA, "inicio": dt.datetime.now().isoformat(timespec="seconds"),
          "etapas": {}, "calidad": {}}

def etapa(nombre):
    """Decorador: mide el tiempo de cada etapa."""
    def deco(fn):
        def run(*a, **k):
            t0 = time.perf_counter(); out = fn(*a, **k)
            REPORT["etapas"][nombre] = {"segundos": round(time.perf_counter() - t0, 2), **(out or {})}
            print(f"[{nombre}] {REPORT['etapas'][nombre]}"); return out
        return run
    return deco

# ---- Esquemas estrictos (sin tipos genericos) ----
D, TS, S, B = T.DoubleType(), T.TimestampType(), T.StringType(), T.BooleanType()
ESQ = {
 "a1": {"fecha_hora": TS, "central": S, "empresa": S, "potencia_programada_mw": D},
 "a2": {"fecha_hora": TS, "central": S, "empresa": S, "potencia_programada_mw": D,
        "potencia_ejecutada_mw": D, "desviacion_mw": D, "origen": S, "programado_disponible": B},
 "a3": {"fecha": T.DateType(), "grupo_elegido": S, "central": S, "empresa": S, "combustible_elegido": S, "tipo_elegido": S,
        "confianza": S, "potencia_efectiva_mw": D, "cvnc_s_kwh": D, "cvc_s_kwh": D, "costo_variable_s_kwh": D},
 "a4": {"fecha_hora": TS, "barra_id": S, "barra": S, "costo_marginal_s_kwh": D},
}
def tipar(df, esq):
    faltan = set(esq) - set(df.columns)
    assert not faltan, f"Columnas faltantes en la fuente: {faltan}"
    return df.select([F.col(c).cast(t).alias(c) for c, t in esq.items()])

def con_particion(df, col_fecha):
    """anio/mes por DIA DE DESPACHO: el bloque 00:00 pertenece al dia anterior."""
    dia = F.to_date(F.expr(f"{col_fecha} - interval 1 minute")) if col_fecha == "fecha_hora" else F.to_date(col_fecha)
    return df.withColumn("anio", F.year(dia)).withColumn("mes", F.month(dia))

def escribir(df, ruta):
    (df.write.mode("overwrite").partitionBy("anio", "mes").parquet(ruta))
    return spark.read.parquet(ruta).count()

# ---- BRONZE -> SILVER (A) ----
@etapa("silver_a")
def silver_a():
    out = {}
    fuentes = {"a1": ("a1_despacho_programado", "fecha_hora", ["fecha_hora", "central"], "potencia_programada_mw"),
               "a2": ("a2_despacho_ejecutado", "fecha_hora", ["fecha_hora", "central"], "potencia_ejecutada_mw"),
               "a3": ("a3_costos_variables", "fecha", ["fecha", "central"], "costo_variable_s_kwh"),
               "a4": ("a4_costos_marginales", "fecha_hora", ["fecha_hora", "barra_id"], "costo_marginal_s_kwh")}
    for k, (carpeta, colf, clave, critica) in fuentes.items():
        df = tipar(spark.read.parquet(f"{BRONZE}/{carpeta}"), ESQ[k])
        n_in = df.count()
        n_fecha_nula = df.filter(F.col(colf).isNull()).count()
        assert n_fecha_nula == 0, f"{k}: {n_fecha_nula} filas con {colf} ilegible tras el cast; revisar el formato"
        dup = df.groupBy(*clave).count().filter("count > 1").count()
        nulos = df.filter(F.col(critica).isNull()).count()
        neg = df.filter(F.col(critica) < 0).count()
        n_out = escribir(con_particion(df, colf), f"{SILVER}/{carpeta}")
        REPORT["calidad"][k] = {"filas_in": n_in, "filas_silver": n_out, "claves_duplicadas": dup,
                                "nulos_columna_critica": nulos, "pct_completitud": round(100 * (1 - nulos / n_in), 3),
                                "negativos": neg}
        if k == "a2":
            REPORT["calidad"]["a2"]["por_origen"] = {r["origen"]: r["count"] for r in df.groupBy("origen").count().collect()}
        out[k] = n_out
    a2 = spark.read.parquet(f"{SILVER}/a2_despacho_ejecutado")
    r = a2.select(F.countDistinct(F.to_date(F.expr("fecha_hora - interval 1 minute"))).alias("dias"),
                  F.countDistinct(F.when(F.col("programado_disponible"), F.to_date(F.expr("fecha_hora - interval 1 minute")))).alias("dias_con_programa")).first()
    REPORT["calidad"]["cobertura_a2"] = {"dias": r.dias, "dias_con_programa": r.dias_con_programa}
    return out

# ---- BRONZE -> SILVER (B: CSV utf-8-sig -> Parquet) ----
CSV_B = {
 "b1_mantenimientos": ("fecha_inicio", {"fecha_inicio": TS, "fecha_fin": TS, "ubicacion": S, "equipo": S, "tipo_equipo": S,
                                       "tipo_mantenimiento": S, "prog": S, "n_filas_origen": T.IntegerType(), "duracion_horas": D}),
 "b2_potencia_indisponible": ("fecha_hora", {"fecha_hora": TS, "instalacion": S, "tipo_instalacion": S,
                                            "tipo_mantenimiento": S, "empresa": S, "potencia_indisponible_mw": D}),
 "b3_reprograma": ("fecha", {"fecha": T.DateType(), "letra_reprograma": S, "motivo": S, "motivo_categoria": S, "costo_total_soles": D,
                             "fecha_hora_inicio": TS, "fecha_hora_fin": TS, "n_bloques": T.IntegerType(), "horas_cubiertas": D}),
 "b4_pr06": ("fecha_hora", {"fecha_hora": TS, "id_dia": S, "id_reprograma": S, "id_fuente": S,
                            "n_archivos_con_rdo1": T.IntegerType(), "label_rdo": T.IntegerType()}),
}
@etapa("silver_b")
def silver_b():
    out = {}
    for carpeta, (colf, tipos) in CSV_B.items():
        ruta = f"{BRONZE}/{carpeta}"
        df = (spark.read.option("header", True).option("encoding", "UTF-8")
              .option("multiLine", True).option("escape", '"').csv(ruta))
        df = df.toDF(*[c.lstrip("\ufeff").strip() for c in df.columns])
        faltan = set(tipos) - set(df.columns)
        assert not faltan, f"{carpeta}: columnas faltantes {faltan}"
        n_in = df.count()
        casted = [F.col(c).cast(t).alias(c) for c, t in tipos.items()]
        resto = [F.col(c) for c in df.columns if c not in tipos]
        df = df.select(*casted, *resto)
        n_mal = df.filter(F.col(colf).isNull()).count()
        out[carpeta] = escribir(con_particion(df.filter(F.col(colf).isNotNull()), colf), f"{SILVER}/{carpeta}")
        REPORT["calidad"][carpeta] = {"filas_in": n_in, "filas_silver": out[carpeta], "fecha_ilegible_rechazadas": n_mal}
    b4 = spark.read.parquet(f"{SILVER}/b4_pr06")
    REPORT["calidad"]["b4"] = {"bloques": b4.count(), "claves_duplicadas": b4.groupBy("fecha_hora").count().filter("count>1").count(),
                               "label_rdo_1": b4.filter("label_rdo = 1").count()}
    return out

# ---- SILVER -> GOLD ----
@etapa("gold")
def gold():
    a2 = spark.read.parquet(f"{SILVER}/a2_despacho_ejecutado")
    a3 = spark.read.parquet(f"{SILVER}/a3_costos_variables").select(
        F.col("fecha").alias("fecha_despacho"), "central", "combustible_elegido", "confianza", "costo_variable_s_kwh")
    a2 = a2.withColumn("fecha_despacho", F.to_date(F.expr("fecha_hora - interval 1 minute")))
    if not USE_BROADCAST:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    g = a2.join(F.broadcast(a3) if USE_BROADCAST else a3, ["central", "fecha_despacho"], "left")
    plan = g._jdf.queryExecution().executedPlan().toString()
    estrategia = "BroadcastHashJoin" if "BroadcastHashJoin" in plan else ("SortMergeJoin" if "SortMergeJoin" in plan else "otro")
    cen = a3.select("central").distinct().withColumn("central_en_a3", F.lit(True))
    g = g.join(F.broadcast(cen), "central", "left").withColumn("central_en_a3", F.coalesce("central_en_a3", F.lit(False)))
    g = g.withColumn("sin_costo_variable", F.col("costo_variable_s_kwh").isNull())
    g = g.withColumn("costo_estado", F.when(F.col("costo_variable_s_kwh").isNotNull(), "con_costo")
                                      .when(F.col("central_en_a3"), "fecha_sin_costo").otherwise("central_sin_costo_en_a3"))
    n_a2 = a2.count(); n_g = escribir(g.drop("fecha_despacho"), f"{GOLD}/despacho_central_30min")
    REPORT["calidad"]["gold_despacho"] = {"filas_a2": n_a2, "filas_gold": n_g, "join_multiplico_filas": n_g != n_a2,
                                          "estrategia_join": estrategia, "filas_sin_costo": g.filter("sin_costo_variable").count(),
                                          "por_costo_estado": {r["costo_estado"]: r["count"] for r in g.groupBy("costo_estado").count().collect()}}
    a4 = spark.read.parquet(f"{SILVER}/a4_costos_marginales")
    p = (a4.withColumn("cero_como_faltante", F.col("costo_marginal_s_kwh") == 0)
           .withColumn("costo_marginal_limpio", F.when(F.col("costo_marginal_s_kwh") == 0, None).otherwise(F.col("costo_marginal_s_kwh"))))
    n_p = escribir(p, f"{GOLD}/precios_barra_30min")
    REPORT["calidad"]["gold_precios"] = {"filas": n_p, "ceros_tratados_como_faltantes": p.filter("cero_como_faltante").count()}
    return {"despacho": n_g, "precios": n_p}

def guardar_reporte():
    REPORT["fin"] = dt.datetime.now().isoformat(timespec="seconds")
    ruta = f"{ROOT}/logs/run_{ESCALA}.json"
    if ROOT.startswith("s3"):
        spark.createDataFrame([(json.dumps(REPORT, indent=2),)], ["j"]).coalesce(1).write.mode("overwrite").text(ruta + ".d")
    else:
        os.makedirs(f"{ROOT}/logs", exist_ok=True); open(ruta, "w").write(json.dumps(REPORT, indent=2))

if __name__ == "__main__":
    t0 = time.perf_counter()
    REPORT["broadcast"] = USE_BROADCAST; REPORT["etapas_solicitadas"] = list(ETAPAS)
    try:
        for nombre, fn in (("silver_a", silver_a), ("silver_b", silver_b), ("gold", gold)):
            if nombre in ETAPAS:
                fn(); REPORT["total_segundos"] = round(time.perf_counter() - t0, 2); guardar_reporte()
    except Exception as e:
        REPORT["error"] = f"{type(e).__name__}: {str(e)[:300]}"; REPORT["total_segundos"] = round(time.perf_counter() - t0, 2)
        guardar_reporte(); print("FALLO:", REPORT["error"]); raise
    print(json.dumps(REPORT, indent=2)); spark.stop()
