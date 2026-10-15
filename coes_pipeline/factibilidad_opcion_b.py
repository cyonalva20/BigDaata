#!/usr/bin/env python3
"""
factibilidad_opcion_b.py - ¿Se puede construir un target de 'arranque térmico NO programado' con A2/A1 (Gold)?
Solo mide: prevalencia, número de eventos, concentración por central, cobertura. NO entrena nada.
Ejecutar desde coes_pipeline. Tarda unos minutos (lee 6,5 M filas con pandas).
Evento (por central y bloque) = ejecutada > UMBRAL_MW  y  programada ausente o 0  y  el día SÍ tiene programa,
                               y la central es térmica 'cara' (definición A: lista de combustibles; B: costo >= P75).
"""
import os
import numpy as np
import pandas as pd
 
UMBRAL_MW = 5.0
COMB_CAROS = ["D2", "R500", "NLH_RFG", "FX_RFG", "CARB"]       # ajusta tras ver la tabla de combustibles
ruta = os.path.join("lake_real", "gold", "despacho_central_30min")
cols = ["fecha_hora", "central", "potencia_programada_mw", "potencia_ejecutada_mw", "programado_disponible",
        "combustible_elegido", "costo_variable_s_kwh", "costo_estado"]
a = pd.read_parquet(ruta, columns=cols)
a["fecha_hora"] = pd.to_datetime(a["fecha_hora"])
print(f"Filas: {len(a):,} | bloques únicos: {a.fecha_hora.nunique():,}")
 
print("\n=== 1. ¿Qué significa 'sin programa'? (filas con ejecutada > umbral y día con programa) ===")
base = a[(a.potencia_ejecutada_mw > UMBRAL_MW) & a.programado_disponible]
print(f"programada nula : {base.potencia_programada_mw.isna().sum():,}")
print(f"programada == 0 : {(base.potencia_programada_mw == 0).sum():,}")
print(f"programada  > 0 : {(base.potencia_programada_mw > 0).sum():,}")
 
print("\n=== 2. Combustibles (filas ejecutadas > umbral, día con programa, sin programa o programa 0) ===")
sinprog = base[base.potencia_programada_mw.fillna(0) == 0]
print(sinprog.combustible_elegido.fillna("(sin combustible / no está en A3)").value_counts().head(15).to_string())
 
def evaluar(nombre, mask_termica):
    e = a[mask_termica & a.programado_disponible].copy()
    e["ev"] = (e.potencia_ejecutada_mw > UMBRAL_MW) & (e.potencia_programada_mw.fillna(0) == 0)
    blq = e.groupby("fecha_hora").ev.any()                       # bloque con >=1 arranque no programado
    nb_prog = a.loc[a.programado_disponible, "fecha_hora"].nunique()
    print(f"\n--- Definición {nombre} ---")
    print(f"Centrales candidatas: {e.central.nunique()} | filas candidatas: {len(e):,}")
    print(f"Bloques con evento: {int(blq.sum()):,} de {nb_prog:,} bloques con programa ({blq.sum() / nb_prog:.2%})")
    ev = e[e.ev].sort_values(["central", "fecha_hora"])
    if ev.empty:
        print("Sin eventos."); return
    nuevo = (ev.groupby("central").fecha_hora.diff() != pd.Timedelta(minutes=30))
    print(f"Episodios (arranques no programados): {int(nuevo.sum()):,} | días con evento: {ev.fecha_hora.dt.date.nunique():,}")
    top = ev.groupby("central").size().sort_values(ascending=False)
    print(f"Concentración: la central nº1 aporta {top.iloc[0] / top.sum():.1%} de los bloques-evento; top 3 = {top.head(3).sum() / top.sum():.1%}")
    print("Top 5 centrales:", top.head(5).to_dict())
    h = blq.groupby(((blq.index - pd.Timedelta(minutes=1)).hour * 2 + (blq.index - pd.Timedelta(minutes=1)).minute // 30 + 1)).mean()
    print("Tasa por bloque (fin 1..48) mín/máx:", f"{h.min():.2%} / {h.max():.2%}", "| máx en bloque", int(h.idxmax()))
    print("Por año:", blq.groupby((blq.index - pd.Timedelta(minutes=1)).year).mean().round(4).to_dict())
    full = blq.astype(float).sort_index().asfreq("30min")        # ventana t+4..t+8 por tiempo
    fut = pd.concat([full.shift(-k) for k in range(4, 9)], axis=1)
    y = fut.max(axis=1).where(fut.notna().all(axis=1))
    print(f"Prevalencia target 'ventana' (t+4..t+8): {y.mean():.2%} sobre {y.notna().sum():,} bloques")
    ok = 0.03 <= blq.mean() <= 0.40 and nuevo.sum() >= 300 and top.iloc[0] / top.sum() < 0.5
    print("Criterio GO (prevalencia 3–40 %, ≥300 episodios, central nº1 < 50 %):", "CUMPLE" if ok else "NO CUMPLE")
 
evaluar("A (lista de combustibles caros)", a.combustible_elegido.isin(COMB_CAROS))
termica = a.combustible_elegido.notna() & a.costo_estado.eq("con_costo") & a.costo_variable_s_kwh.notna()
p75 = a.loc[termica, "costo_variable_s_kwh"].quantile(0.75)
evaluar(f"B (térmica con costo >= P75 = {p75:.3f} S/kWh)", termica & (a.costo_variable_s_kwh >= p75))
print("\nFIN. Pega esta salida completa.")