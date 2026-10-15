#!/usr/bin/env python3
"""
revisar_maestro.py - Resumen de calidad de B_dataset_maestro.csv (no modifica nada).

Uso:   py revisar_maestro.py salida\\B_dataset_maestro.csv
Opcional: py revisar_maestro.py salida\\B_dataset_maestro.csv salida\\B3_reprograma_diario.csv
Pega aqui todo lo que imprima.
"""
import sys
from pathlib import Path

import pandas as pd

ruta = Path(sys.argv[1] if len(sys.argv) > 1 else "salida/B_dataset_maestro.csv")
ruta_b3 = Path(sys.argv[2]) if len(sys.argv) > 2 else ruta.parent / "B3_reprograma_diario.csv"

m = pd.read_csv(ruta, low_memory=False)
m["ts"] = pd.to_datetime(m["fecha_hora"], errors="coerce")
print(f"== MAESTRO: {len(m):,} filas x {m.shape[1]} columnas | fecha_hora ilegibles: {int(m['ts'].isna().sum())}")
print(f"   rango: {m['ts'].min()}  ->  {m['ts'].max()}")

# --- cobertura de bloques y dias (bloque = fin de intervalo de 30 min: dia D va de D 00:30 a D+1 00:00)
dia_bloque = (m["ts"] - pd.Timedelta(minutes=30)).dt.normalize()
esperados_dias = pd.date_range("2021-12-01", "2024-11-30", freq="D")
por_dia = dia_bloque.value_counts()
faltan = [d for d in esperados_dias if d not in por_dia.index]
incompletos = por_dia[(por_dia != 48) & por_dia.index.isin(esperados_dias)]
print(f"\n== COBERTURA: esperados {len(esperados_dias) * 48:,} bloques ({len(esperados_dias)} dias x 48)")
print(f"   dias SIN ningun bloque: {len(faltan)}  | dias con != 48 bloques: {len(incompletos)}")
if faltan:
    anio = pd.Series([d.year for d in faltan]).value_counts().sort_index().to_dict()
    print(f"   dias faltantes por anio: {anio}")
    print("   primeros 12: " + ", ".join(d.strftime('%Y-%m-%d') for d in faltan[:12]))
print(f"   fecha_hora duplicadas: {int(m['fecha_hora'].duplicated().sum())}")

# --- B3: dias con reprograma que no estan en el maestro
if ruta_b3.exists():
    b3 = pd.read_csv(ruta_b3)
    dias_b3 = set(pd.to_datetime(b3["fecha"]).dt.normalize())
    solo_b3 = sorted(d for d in dias_b3 if d not in por_dia.index)
    letras = b3.groupby("fecha")["letra_reprograma"].count()
    print(f"\n== B3: {len(dias_b3)} dias con reprogramas; de ellos {len(solo_b3)} NO estan en el maestro (sin indices PR06)")
    if solo_b3:
        print(f"   primeros: {', '.join(d.strftime('%Y-%m-%d') for d in solo_b3[:8])}")
        solo_a = letras[[d.strftime('%Y-%m-%d') for d in solo_b3]].value_counts().sort_index().to_dict()
        print(f"   n de reprogramas en esos dias (letras:dias): {solo_a}")

# --- label
y = m["label_rdo"]
print(f"\n== LABEL: RDO=1 en {int(y.sum()):,} de {len(m):,} bloques ({y.mean():.1%}) | valores distintos: {sorted(y.dropna().unique().tolist())}")
d = pd.DataFrame({"anio": m["ts"].dt.year, "rdo": y})
print("   tasa RDO por anio: " + ", ".join(f"{a}: {t:.1%}" for a, t in d.groupby("anio")["rdo"].mean().items()))
por_hora = m.groupby(m["ts"].dt.hour)["label_rdo"].mean()
print(f"   hora con mas RDO: {int(por_hora.idxmax()):02d}h ({por_hora.max():.0%}) | con menos: {int(por_hora.idxmin()):02d}h ({por_hora.min():.0%})")
dias_rdo = m.assign(dia=dia_bloque).groupby("dia")["label_rdo"].sum()
print(f"   dias sin ningun RDO=1: {int((dias_rdo == 0).sum())} de {len(dias_rdo)} | dias con TODOS los bloques en RDO=1: {int((dias_rdo == 48).sum())}")
print(f"   bloques RDO=1 sin id_reprograma: {int(((y == 1) & m['id_reprograma'].isna()).sum())} | ids distintos: {m['id_reprograma'].nunique()}")
if "id_fuente" in m:
    print("   id_fuente: " + str(m.loc[y == 1, "id_fuente"].value_counts(dropna=False).to_dict()))

# --- vacios por columna
vac = m.drop(columns=["ts"]).isna().mean().sort_values(ascending=False)
vac = vac[vac > 0]
print("\n== COLUMNAS CON VACIOS (%): " + (", ".join(f"{c}: {v:.0%}" for c, v in vac.items()) if len(vac) else "ninguna"))
for c in ("n_forzados_activos_b1", "impacto_n_reprogramas_dia", "impacto_costo_soles_primera_letra"):
    if c in m:
        s = m[c]
        print(f"   {c}: min {s.min():.4g} | mediana {s.median():.4g} | max {s.max():.4g}")
