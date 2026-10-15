#!/usr/bin/env python3
"""
modelo_base_spark.py  -  Subsección 4.2 (Modelo base / Technical Baseline), PySpark MLlib.
 
Entrada : dataset_analitico_eda.parquet (salida de generar_dataset_eda.py)
Salida  : carpeta evidencias_modelo/ (métricas, curvas, importancias, diagnósticos)
 
Decisiones de diseño (declararlas en el informe):
  * Target = RDO en el FUTURO, usando solo información conocida en t (sin fuga):
      - "ventana": 1 si hay RDO en algún bloque t+4..t+8 (2 a 4 h)  <- objetivo SMART
      - "t4"     : 1 si hay RDO exactamente en t+4 (2 h)            <- sensibilidad
    Las ventanas se calculan por RANGO de tiempo, así los huecos de datos no desplazan bloques.
  * NO se usan como predictores: label_rdo (actual o rezagada: su momento de disponibilidad no
    está verificado), alfa/beta/gamma/chm (fuga), desviacion_neta_mw (nula en 25 % de bloques).
  * Partición temporal con embargo de 4 h (el label mira hasta 4 h adelante):
        train < 2023-07-01 | validación 2023-07-01..2023-12-31 | test >= 2024-01-01
    Umbral de decisión elegido en VALIDACIÓN (nunca en test).
  * Modelos: Regresión Logística ElasticNet y Árbol de Decisión (maxDepth=5) con pesos de clase,
    más dos referencias triviales (siempre positivo; tasa histórica por bloque horario).
"""
import os, json, sys
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, OneHotEncoder
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
 
# ------------------------------------------------------------------ configuración
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)          # evita el error de versiones distintas de Python
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
PARQUET = os.environ.get("PARQUET", "dataset_analitico_eda.parquet")
TARGET_COL = os.environ.get("TARGET_COL", "label_rdo")      # "label_rdo" | "evento_termico_nop" (presencia) | "inicio_termico_nop" (inicio de racha)
SALIDA = os.environ.get("SALIDA", "evidencias_modelo" + ("" if TARGET_COL == "label_rdo" else "_" + TARGET_COL))
FECHA_VALID, FECHA_TEST = "2023-07-01", "2024-01-01"
H_MIN, H_MAX = 4, 8                    # bloques de 30 min  -> 2 h a 4 h
EMBARGO = pd.Timedelta(hours=H_MAX / 2)
SEED, BOOT = 42, 500
TARGET_PRINCIPAL = "ventana"           # "ventana" (objetivo SMART) o "t4"
os.makedirs(SALIDA, exist_ok=True)
 
spark = (SparkSession.builder.appName("modelo_base_rdo").master("local[*]")
         .config("spark.sql.shuffle.partitions", "8").config("spark.driver.memory", "2g")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
 
# ------------------------------------------------------------------ 1. datos y features
need = ["fecha_hora", TARGET_COL, "generacion_total_mw", "generacion_termica_costosa_mw",
        "forzado_activo_b1", "forzado_inicio_3h_previas_b1"]
try:
    df = spark.read.parquet(PARQUET)
except Exception:   # parquet escrito por pandas con timestamps en nanosegundos: Spark 3.x no los lee
    print("Aviso: Spark no pudo leer el parquet (timestamps ns); se carga con pandas y se convierte a DataFrame de Spark.")
    pdf = pd.read_parquet(PARQUET, columns=need)
    pdf["fecha_hora"] = pdf["fecha_hora"].astype("datetime64[us]")
    for c in ["forzado_activo_b1", "forzado_inicio_3h_previas_b1"]:
        pdf[c] = pdf[c].astype(str)
    df = spark.createDataFrame(pdf)
falta = [c for c in need if c not in df.columns]
if falta:
    sys.exit(f"Faltan columnas en {PARQUET}: {falta}")
 
 
def a_bin(c):
    s = F.lower(F.col(c).cast("string"))
    return (F.when(s.isin("true", "1", "1.0"), 1.0).when(s.isin("false", "0", "0.0"), 0.0))
 
 
df = (df.select("fecha_hora", F.col(TARGET_COL).cast("double").alias("rdo"),
                F.col("generacion_total_mw").cast("double").alias("gen"),
                F.col("generacion_termica_costosa_mw").cast("double").alias("term_cara"),
                a_bin("forzado_activo_b1").alias("f_activo"),
                a_bin("forzado_inicio_3h_previas_b1").alias("f_3h"))
        .withColumn("t_idx", (F.unix_timestamp("fecha_hora") / 1800).cast("long")))
 
t_ini = F.col("fecha_hora") - F.expr("INTERVAL 1 MINUTE")        # convención: fecha_hora = FIN de bloque
df = (df.withColumn("bloque", F.hour(t_ini) * 2 + F.floor(F.minute(t_ini) / 30) + 1)   # 1..48
        .withColumn("dow", F.dayofweek(t_ini))
        .withColumn("mes", F.month(t_ini).cast("double"))                                # estacionalidad anual (hidrología)                                          # 1..7
        .withColumn("dia", F.to_date(t_ini)))
 
w = Window.orderBy("t_idx")
def en_rango(col, a, b, fn):  # función agregada sobre bloques t+a .. t+b (por tiempo, no por fila)
    return fn(col).over(w.rangeBetween(a, b))
 
df = (df.withColumn("gen_l1", en_rango("gen", -1, -1, F.max))
        .withColumn("gen_l4", en_rango("gen", -4, -4, F.max))
        .withColumn("rampa_1", F.col("gen") - F.col("gen_l1"))          # ΔDemanda(t)-(t-1) (generación como proxy)
        .withColumn("rampa_4", F.col("gen") - F.col("gen_l4"))          # cambio en 2 h
        .withColumn("term_share", F.col("term_cara") / F.col("gen"))
        .withColumn("bl_sin", F.sin(2 * np.pi * F.col("bloque") / 48))
        .withColumn("bl_cos", F.cos(2 * np.pi * F.col("bloque") / 48))
        .withColumn("dow_sin", F.sin(2 * np.pi * F.col("dow") / 7))
        .withColumn("dow_cos", F.cos(2 * np.pi * F.col("dow") / 7))
        .withColumn("mes_sin", F.sin(2 * np.pi * F.col("mes") / 12))
        .withColumn("mes_cos", F.cos(2 * np.pi * F.col("mes") / 12))
        .withColumn("finde", F.when(F.col("dow").isin(1, 7), 1.0).otherwise(0.0)))
 
# targets (futuros, por rango de tiempo; exigen ventana completa)
df = (df.withColumn("y_ventana_raw", en_rango("rdo", H_MIN, H_MAX, F.max))
        .withColumn("n_ventana", en_rango("rdo", H_MIN, H_MAX, F.count))
        .withColumn("y_ventana", F.when(F.col("n_ventana") == (H_MAX - H_MIN + 1), F.col("y_ventana_raw")))
        .withColumn("y_t4", en_rango("rdo", H_MIN, H_MIN, F.max)))
df = df.withColumn("bloque_idx", (F.col("bloque") - 1).cast("double"))
NUM = ["gen", "rampa_1", "rampa_4", "term_cara", "term_share", "finde", "f_activo", "f_3h"]
LR_FEATS = NUM + ["dow_sin", "dow_cos", "mes_sin", "mes_cos", "bloque_oh"]       # bloque horario como one-hot (perfil horario no senoidal)
DT_FEATS = NUM + ["bloque", "dow", "mes"]                         # el árbol parte directamente el bloque 1..48
FEATURES = NUM + ["dow_sin", "dow_cos", "mes_sin", "mes_cos", "bloque", "dow", "mes", "bloque_idx"]
df = df.dropna(subset=FEATURES).cache()
print(f"Filas con features completas: {df.count():,}")
 
# ------------------------------------------------------------------ 2. utilidades de evaluación
def auc_np(y, s):
    r = pd.Series(s).rank(method="average").to_numpy()
    n1 = y.sum(); n0 = len(y) - n1
    return np.nan if n1 == 0 or n0 == 0 else (r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)
 
def prf(y, s, thr):
    p = (s >= thr).astype(int)
    tp = int(((p == 1) & (y == 1)).sum()); fp = int(((p == 1) & (y == 0)).sum()); fn = int(((p == 0) & (y == 1)).sum())
    tn = int(((p == 0) & (y == 0)).sum())
    pr = tp / (tp + fp) if tp + fp else 0.0; rc = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * pr * rc / (pr + rc) if pr + rc else 0.0
    return pr, rc, f1, (tn, fp, fn, tp)
 
def mejor_umbral(y, s):
    grid = np.unique(np.round(np.r_[np.linspace(0.02, 0.98, 97), np.quantile(s, [.1, .25, .5, .75, .9])], 4))
    f = [prf(y, s, g)[2] for g in grid]
    return float(grid[int(np.argmax(f))])
 
def pr_auc_np(y, s):          # average precision
    o = np.argsort(-s); y = y[o]; tp = np.cumsum(y); k = np.arange(1, len(y) + 1)
    return float((y * tp / k).sum() / max(y.sum(), 1))
 
def curvas(y, s):
    o = np.argsort(-s); ys = y[o]; tp = np.cumsum(ys); fp = np.cumsum(1 - ys)
    return fp / max(fp[-1], 1), tp / max(tp[-1], 1), tp / np.arange(1, len(ys) + 1)
 
# ------------------------------------------------------------------ 3. loop por target
resultados, importancias, franjas, detalle_pred = [], [], [], {}
for TARGET in ["ventana", "t4"]:
    ycol = f"y_{TARGET}"
    d = df.filter(F.col(ycol).isNotNull()).withColumn("y", F.col(ycol))
    # Partición temporal con embargo
    v0, t0 = pd.Timestamp(FECHA_VALID), pd.Timestamp(FECHA_TEST)
    train = d.filter(F.col("fecha_hora") < F.lit((v0 - EMBARGO).to_pydatetime()))
    valid = d.filter((F.col("fecha_hora") >= F.lit(v0.to_pydatetime())) & (F.col("fecha_hora") < F.lit((t0 - EMBARGO).to_pydatetime())))
    test = d.filter(F.col("fecha_hora") >= F.lit(t0.to_pydatetime()))
    n_tr, n_va, n_te = train.count(), valid.count(), test.count()
    prev = {k: float(x.agg(F.avg("y")).first()[0]) for k, x in [("train", train), ("valid", valid), ("test", test)]}
    print(f"\n{'='*70}\nTARGET = {TARGET} | n train/valid/test = {n_tr:,}/{n_va:,}/{n_te:,} | prevalencia "
          f"{prev['train']:.3f}/{prev['valid']:.3f}/{prev['test']:.3f}\n{'='*70}")
 
    # pesos de clase balanceados (solo con la tasa de TRAIN)
    p1 = prev["train"]
    train = train.withColumn("w", F.when(F.col("y") == 1, 0.5 / p1).otherwise(0.5 / (1 - p1)))
    modelos = {
        "Regresión logística (ElasticNet)": (LR_FEATS, LogisticRegression(labelCol="y", featuresCol="features", weightCol="w",
                                             elasticNetParam=0.5, regParam=0.01, maxIter=200, standardization=True)),
        "Árbol de decisión (maxDepth=5)": (DT_FEATS, DecisionTreeClassifier(labelCol="y", featuresCol="features", weightCol="w",
                                           maxDepth=5, minInstancesPerNode=50, seed=SEED)),
    }
    tr_pd = train.select("bloque", "y").toPandas()                       # también sirve a la referencia por bloque
    frec_bl = tr_pd["bloque"].value_counts(normalize=True).sort_index()
    ev_roc = BinaryClassificationEvaluator(labelCol="y", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    ev_pr = BinaryClassificationEvaluator(labelCol="y", rawPredictionCol="rawPrediction", metricName="areaUnderPR")
 
    def a_pandas(x, cols=("fecha_hora", "dia", "bloque", "y", "p")):
        return (x.withColumn("p", vector_to_array("probability")[1]).select(*cols).toPandas()
                 .sort_values("fecha_hora").reset_index(drop=True))
 
    pred = {}
    for nombre, (feats, clf) in modelos.items():
        etapas = ([OneHotEncoder(inputCols=["bloque_idx"], outputCols=["bloque_oh"], dropLast=False)]
                  if "bloque_oh" in feats else [])
        m = Pipeline(stages=etapas + [VectorAssembler(inputCols=feats, outputCol="features"), clf]).fit(train)
        pv, pt = m.transform(valid), m.transform(test)
        pred[nombre] = (a_pandas(pv), a_pandas(pt), ev_roc.evaluate(pt), ev_pr.evaluate(pt))
        est = m.stages[-1]
        if nombre.startswith("Regresión"):
            num2 = NUM + ["dow_sin", "dow_cos", "mes_sin", "mes_cos"]
            sd = train.select([F.stddev(c).alias(c) for c in num2]).first().asDict()
            coefs = est.coefficients.toArray()
            for f_, c_ in zip(num2, coefs[:len(num2)]):
                importancias.append(dict(target=TARGET, modelo=nombre, variable=f_, coef=c_, importancia=abs(c_ * sd[f_])))
            oh = coefs[len(num2):]                                          # 48 coeficientes del bloque horario
            sd_oh = np.sqrt(frec_bl.to_numpy() * (1 - frec_bl.to_numpy()))
            importancias.append(dict(target=TARGET, modelo=nombre, variable="bloque horario (one-hot, suma)", coef=np.nan,
                                     importancia=float(np.abs(oh * sd_oh).sum())))
        else:
            for f_, c_ in zip(DT_FEATS, est.featureImportances.toArray()):
                importancias.append(dict(target=TARGET, modelo=nombre, variable=f_, coef=np.nan, importancia=c_))
 
    # referencias triviales
    tasa_bl = tr_pd.groupby("bloque")["y"].mean()
    ref_bl = lambda x: x.assign(p=x["bloque"].map(tasa_bl).fillna(p1))
    base = next(iter(pred.values()))
    pred["Referencia: tasa histórica por bloque horario"] = (ref_bl(base[0]), ref_bl(base[1]), None, None)
    pred["Referencia: siempre positivo"] = (base[0].assign(p=1.0), base[1].assign(p=1.0), None, None)
 
    rng = np.random.default_rng(SEED)
    for nombre, (pv, pt, roc_sp, pr_sp) in pred.items():
        yv, sv, yt, st = pv["y"].to_numpy(), pv["p"].to_numpy(), pt["y"].to_numpy(), pt["p"].to_numpy()
        thr = 0.5 if "siempre" in nombre else mejor_umbral(yv, sv)       # umbral elegido en VALIDACIÓN
        pr_, rc_, f1_, cm = prf(yt, st, thr)
        auc = auc_np(yt, st) if "siempre" not in nombre else 0.5
        # IC95 % por bootstrap de días (los bloques de un día están correlacionados)
        dias = pt.groupby("dia").indices; claves = list(dias.keys()); aucs, f1s = [], []
        for _ in range(BOOT):
            ix = np.concatenate([dias[claves[i]] for i in rng.integers(0, len(claves), len(claves))])
            aucs.append(auc_np(yt[ix], st[ix]) if "siempre" not in nombre else 0.5); f1s.append(prf(yt[ix], st[ix], thr)[2])
        resultados.append(dict(target=TARGET, modelo=nombre, n_test=len(yt), prevalencia_test=yt.mean(),
                               auc_roc=auc, auc_ic_lo=np.nanpercentile(aucs, 2.5), auc_ic_hi=np.nanpercentile(aucs, 97.5),
                               pr_auc=(pr_sp if pr_sp is not None else pr_auc_np(yt, st) if "siempre" not in nombre else yt.mean()),
                               umbral=thr, precision=pr_, recall=rc_, f1=f1_, f1_ic_lo=np.percentile(f1s, 2.5),
                               f1_ic_hi=np.percentile(f1s, 97.5), tn=cm[0], fp=cm[1], fn=cm[2], tp=cm[3]))
        detalle_pred[(TARGET, nombre)] = (yt, st, thr)
        if nombre.startswith(("Regresión", "Árbol")) and TARGET == TARGET_PRINCIPAL:
            b = pt["bloque"].to_numpy()
            for fr, (a_, z_) in {"madrugada (00:30-06:00)": (1, 12), "mañana (06:30-12:00)": (13, 24),
                                 "tarde (12:30-18:00)": (25, 36), "noche (18:30-24:00)": (37, 48)}.items():
                k = (b >= a_) & (b <= z_)
                pr2, rc2, f2, _ = prf(yt[k], st[k], thr)
                franjas.append(dict(target=TARGET, modelo=nombre, franja=fr, n=int(k.sum()), prevalencia=yt[k].mean(),
                                    auc=auc_np(yt[k], st[k]), precision=pr2, recall=rc2, f1=f2))
 
# ------------------------------------------------------------------ 4. salidas
R = pd.DataFrame(resultados)
R.to_csv(f"{SALIDA}/metricas_baseline.csv", index=False)
pd.DataFrame(importancias).to_csv(f"{SALIDA}/importancia_variables.csv", index=False)
pd.DataFrame(franjas).to_csv(f"{SALIDA}/diagnostico_por_franja.csv", index=False)
cols = ["modelo", "prevalencia_test", "auc_roc", "auc_ic_lo", "auc_ic_hi", "pr_auc", "precision", "recall", "f1", "umbral"]
pd.set_option("display.width", 220, "display.max_columns", 30)
for T in ["ventana", "t4"]:
    print(f"\n### MÉTRICAS EN TEST (2024) | target = {T}" + ("  <- PRINCIPAL" if T == TARGET_PRINCIPAL else "  (sensibilidad)"))
    print(R[R.target == T][cols].round(3).to_string(index=False))
    for _, r in R[(R.target == T) & R.modelo.str.startswith(("Regresión", "Árbol"))].iterrows():
        print(f"  Matriz de confusión {r.modelo}: TN={r.tn} FP={r.fp} FN={r.fn} TP={r.tp}")
print("\n### IMPORTANCIA DE VARIABLES (target principal)")
I = pd.DataFrame(importancias); I = I[I.target == TARGET_PRINCIPAL]
for m_, g in I.groupby("modelo"):
    print(f"\n{m_}:\n", g.sort_values("importancia", ascending=False)[["variable", "coef", "importancia"]].round(4).to_string(index=False))
print("\n### DIAGNÓSTICO POR FRANJA HORARIA (target principal)")
print(pd.DataFrame(franjas).round(3).to_string(index=False))
 
# curvas ROC / PR del target principal
fig, ax = plt.subplots(1, 2, figsize=(11, 4.4))
for (T, nombre), (yt, st, thr) in detalle_pred.items():
    if T != TARGET_PRINCIPAL or "siempre" in nombre:
        continue
    fpr, tpr, prec = curvas(yt, st)
    ax[0].plot(fpr, tpr, label=nombre.replace("Referencia: ", "Ref.: "), ls="--" if "Referencia" in nombre else "-")
    rec = tpr; ax[1].plot(rec, prec, label=nombre.replace("Referencia: ", "Ref.: "), ls="--" if "Referencia" in nombre else "-")
ax[0].plot([0, 1], [0, 1], color="gray", lw=.8); ax[0].set(xlabel="Tasa de falsos positivos", ylabel="Tasa de verdaderos positivos", title="Curva ROC (test 2024)")
prev_t = R[(R.target == TARGET_PRINCIPAL)].prevalencia_test.iloc[0]
ax[1].axhline(prev_t, color="gray", lw=.8); ax[1].set(xlabel="Recall", ylabel="Precision", title="Curva Precision–Recall (test 2024)")
ax[0].legend(fontsize=7, loc="lower right"); ax[1].legend(fontsize=7, loc="upper right"); plt.tight_layout(); plt.savefig(f"{SALIDA}/curvas_roc_pr.png", dpi=300); plt.close()
 
# importancia
fig, ax = plt.subplots(1, 2, figsize=(12, 4.6), sharey=False)
for a, (m_, g) in zip(ax, I.groupby("modelo")):
    g = g.sort_values("importancia"); a.barh(g.variable, g.importancia); a.set_title(m_, fontsize=9)
    a.set_xlabel("|coef × desv. est.|" if m_.startswith("Regresión") else "Importancia (Gini)")
plt.tight_layout(); plt.savefig(f"{SALIDA}/importancia_variables.png", dpi=300); plt.close()
 
json.dump(dict(spark=spark.version, python=sys.version.split()[0], target_principal=TARGET_PRINCIPAL, features=FEATURES,
               split=dict(valid_desde=FECHA_VALID, test_desde=FECHA_TEST, embargo_horas=EMBARGO.total_seconds() / 3600),
               horizonte_bloques=[H_MIN, H_MAX], semilla=SEED, bootstrap=BOOT),
          open(f"{SALIDA}/config_corrida.json", "w"), indent=2, ensure_ascii=False)
print(f"\n✓ Evidencias en '{SALIDA}/'")
spark.stop()