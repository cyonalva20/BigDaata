"""Diagnostico de A2 sin ejecutada y del join A2 x A3 (no modifica nada).
Uso: python diagnostico_join.py lake_real"""
import sys, pandas as pd
root = sys.argv[1] if len(sys.argv) > 1 else "lake_real"
a2 = pd.read_parquet(f"{root}/silver/a2_despacho_ejecutado", columns=["fecha_hora", "central", "potencia_ejecutada_mw", "programado_disponible", "origen"])
a3 = pd.read_parquet(f"{root}/silver/a3_costos_variables", columns=["fecha", "central", "costo_variable_s_kwh"])
a2["fecha_despacho"] = (a2.fecha_hora - pd.Timedelta(minutes=1)).dt.normalize()
a3["fecha_despacho"] = pd.to_datetime(a3.fecha)

print("== 1) A2 con potencia_ejecutada_mw nula:", int(a2.potencia_ejecutada_mw.isna().sum()))
nul = a2[a2.potencia_ejecutada_mw.isna()]
print("   por origen:", nul.origen.value_counts().to_dict())
print("   top 10 centrales:", nul.central.value_counts().head(10).to_dict())
print("   dias distintos:", nul.fecha_despacho.nunique(), "| con programado_disponible:", nul.programado_disponible.value_counts().to_dict())

c2, c3 = set(a2.central.unique()), set(a3.central.unique())
print(f"\n== 2) Centrales: A2={len(c2)} | A3={len(c3)} | en ambas={len(c2 & c3)} | solo A2={len(c2 - c3)} | solo A3={len(c3 - c2)}")
print("   ejemplos solo A3 (posible desajuste de nombre):", sorted(c3 - c2)[:15])
print("   ejemplos solo A2:", sorted(c2 - c3)[:15])

k = a2.groupby(["central", "fecha_despacho"], as_index=False).size()
m = k.merge(a3[["central", "fecha_despacho"]].drop_duplicates(), how="left", on=["central", "fecha_despacho"], indicator=True)
m["con_costo"] = m._merge == "both"
tot = len(m)
print(f"\n== 3) Central-dias de A2: {tot:,} | con costo en A3: {int(m.con_costo.sum()):,} ({m.con_costo.mean():.1%})")
sin = m[~m.con_costo]
sin_nunca = sin[~sin.central.isin(c3)]
sin_otras = sin[sin.central.isin(c3)]
print(f"   sin costo porque la central NO existe en A3: {len(sin_nunca):,} ({len(sin_nunca)/tot:.1%})  [{sin_nunca.central.nunique()} centrales]")
print(f"   sin costo aunque la central SI existe en A3 (falta esa fecha): {len(sin_otras):,} ({len(sin_otras)/tot:.1%})")
fechas_a3 = set(a3.fecha_despacho.unique()); todas = pd.date_range("2021-12-01", "2024-11-30")
print("   dias de la ventana sin ninguna fila en A3:", sum(d not in fechas_a3 for d in todas))
print("   top centrales con falta de fechas (existen en A3):", sin_otras.central.value_counts().head(8).to_dict())
a3c = a3.merge(k[["central", "fecha_despacho"]], how="left", on=["central", "fecha_despacho"], indicator=True)
print(f"\n== 4) Filas de A3 sin pareja en A2: {int((a3c._merge=='left_only').sum()):,} de {len(a3):,}")
