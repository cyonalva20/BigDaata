#!/usr/bin/env python3
"""
generar_dataset_eda.py  (versión corregida)
Paso 1: agregación Gold despacho (A) a nivel SEIN-bloque + join con B (solo columnas seguras).
Paso 2: cifras de control y estadística univariada, con verificaciones explícitas.
Ejecutar desde la carpeta coes_pipeline.
"""
import os
import numpy as np
import pandas as pd
 
# Cifras de control documentadas en la Sección 3
N_ESPERADO, POS_ESPERADO = 50_396, 13_146
BOOT = 2000
RNG = np.random.default_rng(42)
 
 
def a_binario(s: pd.Series) -> pd.Series:
    """'True'/'False'/bool/0/1 -> Int8 (NaN se conserva; NO se rellena con 0)."""
    m = {"true": 1, "1": 1, "1.0": 1, "false": 0, "0": 0, "0.0": 0}
    return s.astype(str).str.strip().str.lower().map(m).astype("Int8")
 
 
print("=" * 70, "\nPASO 1: DATASET ANALÍTICO (A + B)\n", "=" * 70, sep="")
 
# ---------------------------------------------------------------- A (Gold)
ruta_a = os.path.join("lake_real", "gold", "despacho_central_30min")
cols_a = ["fecha_hora", "potencia_ejecutada_mw", "desviacion_mw", "programado_disponible",
          "costo_estado", "costo_variable_s_kwh", "combustible_elegido"]
a = pd.read_parquet(ruta_a, columns=cols_a)
a["fecha_hora"] = pd.to_datetime(a["fecha_hora"])
print(f"[A] filas: {len(a):,}")
 
# Umbral de "térmica costosa" EMPÍRICO (P90 del costo variable informado), no arbitrario
con_costo = a["costo_estado"].eq("con_costo") & a["costo_variable_s_kwh"].notna()
umbral = a.loc[con_costo, "costo_variable_s_kwh"].quantile(0.90)
print(f"[A] umbral térmica costosa = P90 costo_variable = {umbral:.4f} S/kWh")
print("[A] combustibles (para decidir si conviene filtrar por combustible):")
print(a["combustible_elegido"].value_counts(dropna=False).head(15).to_string())
 
a["es_cara"] = con_costo & (a["costo_variable_s_kwh"] >= umbral)
# desviación solo donde hubo programa; en días sin programa queda NaN (no 0)
a["dev"] = a["desviacion_mw"].where(a["programado_disponible"])
a["dev_abs"] = a["dev"].abs()
a["ej_cara"] = a["potencia_ejecutada_mw"].where(a["es_cara"], 0.0)
 
g = a.groupby("fecha_hora", sort=True)
agg = g.agg(generacion_total_mw=("potencia_ejecutada_mw", "sum"),
            desv_sum=("dev", "sum"), desv_n=("dev", "count"),
            desviacion_abs_mw=("dev_abs", "sum"),
            generacion_termica_costosa_mw=("ej_cara", "sum"),
            programado_disponible=("programado_disponible", "any")).reset_index()
agg["desviacion_neta_mw"] = agg["desv_sum"].where(agg["desv_n"] > 0)      # NaN si no hubo programa
agg["desviacion_abs_mw"] = agg["desviacion_abs_mw"].where(agg["desv_n"] > 0)
agg = agg.drop(columns=["desv_sum", "desv_n"])
print(f"[A] bloques agregados: {len(agg):,}")
 
# ---------------------------------------------------------------- B (Silver preferente)
ruta_b_silver = os.path.join("lake_real", "silver", "b4_pr06")
ruta_b_csv = os.path.join("salida", "B_dataset_maestro.csv")
if os.path.isdir(ruta_b_silver):
    b = pd.read_parquet(ruta_b_silver); origen_b = ruta_b_silver
else:
    b = pd.read_csv(ruta_b_csv, low_memory=False); origen_b = ruta_b_csv
print(f"[B] origen: {origen_b} | filas: {len(b):,}")
b["fecha_hora"] = pd.to_datetime(b["fecha_hora"])
assert b["fecha_hora"].is_unique, "B tiene fecha_hora duplicada"
 
# SOLO columnas seguras: alfa, beta, gamma, chm (y sus *_primer_archivo) quedan fuera por fuga temporal
seguras = ["fecha_hora", "label_rdo", "forzado_activo_b1", "forzado_inicio_3h_previas_b1"]
b = b[seguras].copy()
b["label_rdo"] = b["label_rdo"].astype(int)
for c in ["forzado_activo_b1", "forzado_inicio_3h_previas_b1"]:
    b[c] = a_binario(b[c])
 
# ---------------------------------------------------------------- JOIN
m = agg.merge(b, on="fecha_hora", how="inner", validate="one_to_one")
sin_a = len(b) - len(m)
print(f"[JOIN] filas B sin pareja en A: {sin_a:,} | filas finales: {len(m):,}")
 
# Bloque con convención FIN de bloque: 00:30 -> 1 ... 00:00 -> 48; el 00:00 pertenece al día anterior
t = m["fecha_hora"] - pd.Timedelta(minutes=1)
m["bloque_fin"] = t.dt.hour * 2 + t.dt.minute // 30 + 1          # 1..48
m["hora_fin"] = m["bloque_fin"] / 2                               # 0.5 .. 24.0
m["dia_operativo"] = pd.to_datetime(t.dt.date)
m["anio"] = m["dia_operativo"].dt.year
m["dow"] = m["dia_operativo"].dt.dayofweek
 
m.to_parquet("dataset_analitico_eda.parquet", index=False)
print(f"✓ dataset_analitico_eda.parquet ({len(m):,} filas)")
 
# ================================================================ PASO 2
print("\n" + "=" * 70, "\nPASO 2: CIFRAS DE CONTROL\n", "=" * 70, sep="")
n, pos = len(m), int(m["label_rdo"].sum())
print(f"Filas: {n:,}  (esperado {N_ESPERADO:,}) -> {'OK' if n == N_ESPERADO else 'REVISAR'}")
print(f"Positivos: {pos:,} = {pos/n:.2%}  (esperado {POS_ESPERADO:,}) -> {'OK' if pos == POS_ESPERADO else 'REVISAR'}")
print(f"Rango: {m['fecha_hora'].min()} -> {m['fecha_hora'].max()} | días operativos: {m['dia_operativo'].nunique():,}")
print(f"Bloques sin programa (desviación NaN): {m['desviacion_neta_mw'].isna().sum():,}")
 
print("\nTasa RDO por año operativo:")
print(m.groupby("anio")["label_rdo"].agg(tasa="mean", positivos="sum", bloques="size").round(4).to_string())
 
print("\nTasa RDO (%) por bloque (fin de bloque):")
tb = (m.groupby("bloque_fin")["label_rdo"].mean() * 100).round(1)
print(tb.to_string())
print(f"Máximo: bloque {tb.idxmax()} ({tb.max():.1f}%) | Mínimo: bloque {tb.idxmin()} ({tb.min():.1f}%)")
 
# ---- B1 vs RDO
print("\nEVENTOS FORZADOS (B1) vs RDO")
for col in ["forzado_inicio_3h_previas_b1", "forzado_activo_b1"]:
    d = m.dropna(subset=[col])
    tab = d.groupby(col)["label_rdo"].agg(tasa="mean", positivos="sum", bloques="size")
    print(f"\n{col} (nulos excluidos: {m[col].isna().sum()})")
    print(tab.round(4).to_string())
 
col = "forzado_inicio_3h_previas_b1"
d = m.dropna(subset=[col]).copy()
x, y = d[col].astype(int).to_numpy(), d["label_rdo"].to_numpy()
phi = np.corrcoef(x, y)[0, 1]                       # Pearson entre dos binarias = phi
try:
    from scipy.stats import chi2_contingency
    chi2, p, _, _ = chi2_contingency(pd.crosstab(x, y))
    print(f"\nPearson/phi = {phi:.4f} | chi2 = {chi2:.2f} | p (por bloque, optimista) = {p:.3g}")
except ImportError:
    print(f"\nPearson/phi = {phi:.4f} (instala scipy para chi2)")
 
# Los bloques de un mismo día están correlacionados -> bootstrap por día operativo
dia = d.assign(x=x, y=y).groupby(["dia_operativo", "x"])["y"].agg(k="sum", n="size").unstack("x").fillna(0)
k0, n0, k1, n1 = (dia[("k", 0)].to_numpy(), dia[("n", 0)].to_numpy(), dia[("k", 1)].to_numpy(), dia[("n", 1)].to_numpy())
idx = RNG.integers(0, len(dia), size=(BOOT, len(dia)))
r0, r1 = k0[idx].sum(1) / n0[idx].sum(1), k1[idx].sum(1) / n1[idx].sum(1)
rr = r1 / r0
print(f"Razón de riesgo (con/sin falla 3h) = {k1.sum()/n1.sum()/(k0.sum()/n0.sum()):.3f} | "
      f"IC95% bootstrap por día = [{np.percentile(rr,2.5):.3f}, {np.percentile(rr,97.5):.3f}]")
print(f"Diferencia (pp) = {(k1.sum()/n1.sum()-k0.sum()/n0.sum())*100:.2f}")
print("\nFIN. Compara los 'OK/REVISAR' antes de usar las cifras en el informe.")