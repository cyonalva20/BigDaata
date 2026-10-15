#!/usr/bin/env python3
"""
diagnostico_target_termico.py - ¿El evento 'térmica cara no programada' significa lo mismo en todo el período?
Revisa: (1) de qué tipo de fila sale el evento (programada nula / cero; origen), (2) cuántas centrales caras aparecen
en A1 cada mes (¿cambió el formato?), (3) prevalencia de tres definiciones por partición y por mes.
Ejecutar desde coes_pipeline. Pegar la salida completa.
"""
import os
import numpy as np
import pandas as pd
 
UMBRAL_MW = 5.0
COMB_CAROS = ["D2", "R500", "NLH_RFG", "FX_RFG", "CARB"]
SALIDA = "evidencias_modelo"; os.makedirs(SALIDA, exist_ok=True)
 
cols = ["fecha_hora", "central", "potencia_programada_mw", "potencia_ejecutada_mw", "programado_disponible",
        "combustible_elegido", "origen"]
a = pd.read_parquet(os.path.join("lake_real", "gold", "despacho_central_30min"), columns=cols)
a = a.reset_index(drop=True)
a["fecha_hora"] = pd.to_datetime(a["fecha_hora"])
c = a[a.combustible_elegido.isin(COMB_CAROS) & a.programado_disponible].copy()
t = c.fecha_hora - pd.Timedelta(minutes=1)
c["anio"], c["mes"] = t.dt.year, t.dt.to_period("M").astype(str)
ev = c[c.potencia_ejecutada_mw > UMBRAL_MW].copy()
ev["tipo"] = np.select([ev.potencia_programada_mw.isna(), ev.potencia_programada_mw == 0],
                       ["programada NULA", "programada = 0"], "programada > 0")
 
print("=== 1. Filas con ejecutada > umbral (centrales caras, días con programa), por año y tipo ===")
print(pd.crosstab(ev.anio, ev.tipo).to_string())
print("\nPor 'origen' (¿la central aparece en el programa?):")
print(pd.crosstab(ev.anio, ev.origen.fillna("(nulo)")).to_string())
 
print("\n=== 2. ¿Cambió cuántas centrales caras figuran en A1 cada mes? ===")
m = pd.DataFrame({
    "centrales_en_A1": c[c.potencia_programada_mw.notna()].groupby("mes").central.nunique(),
    "centrales_ejecutando": ev.groupby("mes").central.nunique(),
    "filas_nulas_ejec": ev[ev.tipo == "programada NULA"].groupby("mes").size(),
    "filas_cero_ejec": ev[ev.tipo == "programada = 0"].groupby("mes").size(),
    "filas_prog_pos_ejec": ev[ev.tipo == "programada > 0"].groupby("mes").size(),
}).reindex(sorted(c.mes.unique())).fillna(0).astype(int)      # compatible con pandas antiguos (sin groupby.apply)
print(m.to_string()); m.to_csv(f"{SALIDA}/termico_por_mes.csv")
 
print("\n=== 3. Prevalencia de bloque con evento, tres definiciones ===")
# refinada: la fila nula solo cuenta si la central SÍ figura en el programa ese mes (es una central programable)
en_a1 = c.groupby(["central", "mes"]).potencia_programada_mw.transform(lambda s: s.notna().any())
c["amplia"] = (c.potencia_ejecutada_mw > UMBRAL_MW) & (c.potencia_programada_mw.isna() | (c.potencia_programada_mw == 0))
c["estricta"] = (c.potencia_ejecutada_mw > UMBRAL_MW) & (c.potencia_programada_mw == 0)
c["refinada"] = c["estricta"] | ((c.potencia_ejecutada_mw > UMBRAL_MW) & c.potencia_programada_mw.isna() & en_a1)
bl = c.groupby("fecha_hora")[["amplia", "estricta", "refinada"]].any()
con_prog = a.groupby("fecha_hora").programado_disponible.any()
bl = bl.reindex(con_prog[con_prog].index).fillna(False)
bt = bl.index - pd.Timedelta(minutes=1)
part = np.where(bl.index >= "2024-01-01", "3_test_2024", np.where(bl.index >= "2023-07-01", "2_valid_2023H2", "1_train"))
print(bl.groupby(part).mean().round(4).to_string())
print("\nPor año operativo:"); print(bl.groupby(bt.year).mean().round(4).to_string())
print("\nBloques con evento por definición y partición:"); print(bl.groupby(part).sum().to_string())
# episodios por central (rachas) para la definición refinada
r = c[c.refinada].sort_values(["central", "fecha_hora"])
nuevo = r.groupby("central").fecha_hora.diff() != pd.Timedelta(minutes=30)
print(f"\nEpisodios (definición refinada): {int(nuevo.sum()):,} | centrales distintas: {r.central.nunique()}")
print("Top 8 centrales por bloques-evento (refinada):", r.central.value_counts().head(8).to_dict())
print(f"\n✓ Detalle mensual en '{SALIDA}/termico_por_mes.csv'")