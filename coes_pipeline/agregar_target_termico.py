#!/usr/bin/env python3
"""
agregar_target_termico.py (v2) - Construye el target de la opción B y lo agrega al dataset analítico.
 
Evento por central y bloque = central térmica CARA (COMB_CAROS) con ejecutada > UMBRAL_MW, en un día con programa (A1),
  y SIN programa: programada = 0 explícita, o programada nula SI la central figura en A1 ese mes (central programable).
Columnas nuevas (por bloque):
  evento_termico_nop : 1 si hay >= 1 central con evento (definición 'refinada'); 0 si no; NaN si no se puede saber
                       (día sin programa A1, o fecha sin tabla de costos A3: sin combustible no se puede identificar la central)
  inicio_termico_nop : 1 si el bloque tiene evento y el bloque anterior no (INICIO de una racha no programada)
  evento_amplio / evento_estricto : sensibilidades (programada nula o 0 / solo programada = 0)
  n_centrales_nop, mwh_termico_nop, costo_variable_nop_soles : exposición económica (costo VARIABLE, no sobrecosto)
Entrada : dataset_analitico_eda.parquet + lake_real/gold/despacho_central_30min
Salida  : dataset_analitico_termico.parquet
"""
import os
import numpy as np
import pandas as pd
 
UMBRAL_MW = 5.0
COMB_CAROS = ["D2", "R500", "NLH_RFG", "FX_RFG", "CARB"]
FECHA_VALID, FECHA_TEST = "2023-07-01", "2024-01-01"
 
base = pd.read_parquet("dataset_analitico_eda.parquet")
base["fecha_hora"] = pd.to_datetime(base["fecha_hora"])
a = pd.read_parquet(os.path.join("lake_real", "gold", "despacho_central_30min"),
                    columns=["fecha_hora", "central", "potencia_programada_mw", "potencia_ejecutada_mw",
                             "programado_disponible", "combustible_elegido", "costo_variable_s_kwh", "costo_estado"])
a = a.reset_index(drop=True)
a["fecha_hora"] = pd.to_datetime(a["fecha_hora"])
a["mes"] = (a.fecha_hora - pd.Timedelta(minutes=1)).dt.to_period("M").astype(str)
 
# --- bloques donde SÍ se puede etiquetar: el día tiene programa (A1) Y tiene información de costos/combustible (A3)
#     (un día sin NINGUNA fila 'con_costo' es un hueco de A3: sin combustible no se puede identificar la térmica cara)
a["dia"] = (a.fecha_hora - pd.Timedelta(minutes=1)).dt.date
dia_con_a3 = a.assign(cc=a.costo_estado.eq("con_costo")).groupby("dia").cc.any()
bq = a.groupby("fecha_hora").agg(con_prog=("programado_disponible", "any"), dia=("dia", "first"))
bq["sin_a3"] = ~bq.dia.map(dia_con_a3).fillna(False).astype(bool)
etiquetable = bq.con_prog & ~bq.sin_a3
n_sin = int((bq.con_prog & bq.sin_a3).sum())
print(f"Bloques con programa: {int(bq.con_prog.sum()):,} | de ellos en días SIN información A3: {n_sin:,} "
      f"({n_sin / max(int(bq.con_prog.sum()), 1):.1%}) -> quedan SIN etiqueta (NaN), no como negativos")
print("Días sin A3 por mes:", bq[bq.con_prog & bq.sin_a3].groupby(pd.to_datetime(bq[bq.con_prog & bq.sin_a3].dia).dt.to_period("M")).dia.nunique().to_dict())
assert etiquetable.sum() / max(int(bq.con_prog.sum()), 1) > 0.5, "Casi ningún bloque es etiquetable: revisa costo_estado (valores: %s)" % a.costo_estado.unique()[:5]
 
# --- centrales caras programables: figuran en A1 (programada no nula) ese mes
cara = a[a.combustible_elegido.isin(COMB_CAROS) & a.programado_disponible].copy()
en_a1 = cara[cara.potencia_programada_mw.notna()].groupby(["central", "mes"]).size().rename("n_a1").reset_index()
cara = cara.merge(en_a1, on=["central", "mes"], how="left")
cara["en_a1_mes"] = cara.n_a1.notna()
ej = cara.potencia_ejecutada_mw > UMBRAL_MW
cara["estricta"] = ej & (cara.potencia_programada_mw == 0)
cara["amplia"] = ej & (cara.potencia_programada_mw.isna() | (cara.potencia_programada_mw == 0))
cara["refinada"] = cara.estricta | (ej & cara.potencia_programada_mw.isna() & cara.en_a1_mes)
cara["mwh"] = np.where(cara.refinada, cara.potencia_ejecutada_mw * 0.5, 0.0)
cara["soles"] = np.where(cara.refinada, cara.potencia_ejecutada_mw * 0.5 * 1000 * cara.costo_variable_s_kwh, np.nan)
g = cara.groupby("fecha_hora")
ev = pd.DataFrame({"evento_termico_nop": g.refinada.any(), "evento_amplio": g.amplia.any(), "evento_estricto": g.estricta.any(),
                   "n_centrales_nop": g.refinada.sum(), "mwh_termico_nop": g.mwh.sum(),
                   "costo_variable_nop_soles": g.soles.sum(min_count=1)}).reindex(bq.index)
ev[["evento_termico_nop", "evento_amplio", "evento_estricto"]] = ev[["evento_termico_nop", "evento_amplio", "evento_estricto"]].fillna(False)
ev[["n_centrales_nop", "mwh_termico_nop", "costo_variable_nop_soles"]] = ev[["n_centrales_nop", "mwh_termico_nop", "costo_variable_nop_soles"]].fillna(0)
 
# inicio de racha: evento ahora y no en el bloque anterior (por tiempo, solo si el anterior es etiquetable)
prev = ev.evento_termico_nop.reindex(ev.index - pd.Timedelta(minutes=30)).to_numpy()
prev_ok = etiquetable.reindex(ev.index - pd.Timedelta(minutes=30)).fillna(False).to_numpy()
ev["inicio_termico_nop"] = ev.evento_termico_nop & ~prev.astype(bool) & prev_ok
 
out = base.merge(ev.reset_index(), on="fecha_hora", how="left", validate="one_to_one")
ok = out.fecha_hora.map(etiquetable).fillna(False).astype(bool)
for col in ["evento_termico_nop", "evento_amplio", "evento_estricto", "inicio_termico_nop"]:
    out[col] = out[col].astype(float).where(ok)
for col in ["n_centrales_nop", "mwh_termico_nop", "costo_variable_nop_soles"]:
    out[col] = out[col].where(ok)
out.to_parquet("dataset_analitico_termico.parquet", index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
 
print(f"\nFilas: {len(out):,} | con etiqueta: {ok.sum():,} ({ok.mean():.1%})")
t = out.fecha_hora - pd.Timedelta(minutes=1)
out["part"] = np.where(out.fecha_hora >= FECHA_TEST, "3_test (2024)", np.where(out.fecha_hora >= FECHA_VALID, "2_validación (2023-H2)", "1_entrenamiento"))
cols = ["evento_termico_nop", "inicio_termico_nop", "evento_amplio", "evento_estricto"]
print("\nPrevalencia por bloque y partición:")
print(out.groupby("part").agg(bloques=("fecha_hora", "size"), con_etiqueta=("evento_termico_nop", "count"),
                              **{c: (c, "mean") for c in cols}).round(4).to_string())
print("\nPor año operativo:")
print(out.groupby(t.dt.year).agg(con_etiqueta=("evento_termico_nop", "count"), evento=("evento_termico_nop", "mean"),
                                 inicio=("inicio_termico_nop", "mean"), inicios=("inicio_termico_nop", "sum")).round(4).to_string())
print("\nEXPOSICIÓN ECONÓMICA (costo VARIABLE de la energía térmica cara no programada; NO es sobrecosto), días etiquetables:")
print(out.groupby(t.dt.year).agg(mwh=("mwh_termico_nop", "sum"), soles_millones=("costo_variable_nop_soles", lambda x: x.sum() / 1e6),
                                 bloques=("evento_termico_nop", "count")).round(2).to_string())
print("\nCorrelación (phi) evento térmico vs RDO:", round(out[["evento_termico_nop", "label_rdo"]].dropna().corr().iloc[0, 1], 4))
print("✓ dataset_analitico_termico.parquet")