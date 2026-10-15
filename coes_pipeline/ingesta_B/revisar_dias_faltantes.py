#!/usr/bin/env python3
"""
revisar_dias_faltantes.py - Por que faltan dias en el maestro y como se reparte el RDO por hora.
No modifica nada.

Uso:
  py revisar_dias_faltantes.py salida\\B_dataset_maestro.csv salida\\B3_reprograma_diario.csv C:\\IA\\datos_coes\\descarga_log.csv
(el tercer archivo es opcional). Pega aqui todo lo que imprima.
"""
import sys
from pathlib import Path

import pandas as pd

ruta_m = Path(sys.argv[1] if len(sys.argv) > 1 else "salida/B_dataset_maestro.csv")
ruta_b3 = Path(sys.argv[2] if len(sys.argv) > 2 else ruta_m.parent / "B3_reprograma_diario.csv")
ruta_log = Path(sys.argv[3] if len(sys.argv) > 3 else r"C:\IA\datos_coes\descarga_log.csv")

m = pd.read_csv(ruta_m, low_memory=False)
m["ts"] = pd.to_datetime(m["fecha_hora"])
m["dia"] = (m["ts"] - pd.Timedelta(minutes=30)).dt.normalize()
dias_m = set(m["dia"].unique())

b3 = pd.read_csv(ruta_b3)
b3["dia"] = pd.to_datetime(b3["fecha"])
b3["falta"] = ~b3["dia"].isin(dias_m)
faltan = sorted(b3.loc[b3["falta"], "dia"].unique())
print(f"== {len(faltan)} dias con reprograma (B3) y SIN indices PR06 en el maestro\n")

# 1) motivo de los reprogramas de esos dias vs los demas
if "motivo_categoria" in b3:
    t = pd.crosstab(b3["falta"].map({True: "dias SIN indices", False: "dias CON indices"}),
                    b3["motivo_categoria"].fillna("(vacio)"), normalize="index").round(2) * 100
    print("== Motivo de los reprogramas (% por grupo de dias):")
    print(t.T.sort_values(t.index[0], ascending=False).head(8).to_string())
    print()
n_rep = b3.groupby("dia").size()
print("== Reprogramas por dia -> dias SIN indices: " + str(n_rep.reindex(faltan).value_counts().sort_index().to_dict())
      + " | dias CON indices: " + str(n_rep[~n_rep.index.isin(faltan)].value_counts().sort_index().to_dict()))
dow = pd.Series([d.day_name()[:3] for d in faltan]).value_counts().to_dict()
print(f"== Dia de la semana de los dias sin indices: {dow}\n")

# 2) que paso en la descarga
if ruta_log.exists():
    lg = pd.read_csv(ruta_log)
    lg["dia"] = pd.to_datetime(lg["fecha"])
    idx = lg[lg["archivo"].str.startswith("indices") & lg["dia"].isin(faltan)]
    print(f"== Descarga de indices en esos dias (log): {idx['estado'].value_counts().to_dict()}")
    print("   ejemplos (fecha, letra, archivo, estado):")
    for d in faltan[:3]:
        for _, r in lg[(lg["dia"] == d)].head(8).iterrows():
            print(f"     {r['fecha']} {r['letra']} {r['archivo']} {r['estado']}")
    print()
else:
    print(f"(sin log de descarga en {ruta_log}; pasalo como 3er argumento si quieres ese detalle)\n")

# 3) RDO por hora
print("== % de bloques RDO=1 por hora del dia (hora = fin del bloque):")
h = m.groupby(m["ts"].dt.hour)["label_rdo"].mean().mul(100).round(0).astype(int)
print("   " + " ".join(f"{k:02d}h:{v:>2}" for k, v in h.items() if k < 12))
print("   " + " ".join(f"{k:02d}h:{v:>2}" for k, v in h.items() if k >= 12))

# 4) dias con bloques distintos de 48
cnt = m.groupby("dia").size()
raro = cnt[(cnt != 48) & (cnt.index < "2024-11-30")]
print(f"\n== Dias con bloques != 48 (sin contar el ultimo dia de la ventana): {len(raro)}")
for d, n in raro.items():
    print(f"   {d:%Y-%m-%d}: {n} bloques")
