"""Genera un MINI dataset con los esquemas de A y B, SOLO para probar que el
pipeline corre. Estos datos son artificiales: NO se usan en el informe ni en el modelo."""
import os, sys, itertools
import numpy as np, pandas as pd

root = sys.argv[1] if len(sys.argv) > 1 else "lake_test"
b = os.path.join(root, "bronze")
rng = np.random.default_rng(0)

# 3 meses a 30 min; fecha_hora = FIN del bloque (00:30 ... 00:00 del dia siguiente)
fh = pd.date_range("2021-12-01 00:30", "2022-02-28 00:00", freq="30min")
centrales = [f"CENTRAL_{i:02d}" for i in range(20)]
empresas = {c: f"EMP_{i%5}" for i, c in enumerate(centrales)}

def w(df, name, fname):
    d = os.path.join(b, name); os.makedirs(d, exist_ok=True)
    df.to_parquet(os.path.join(d, fname), index=False)

# A1 con un hueco de 5 dias (simula los dias faltantes del portal)
base = pd.DataFrame(list(itertools.product(fh, centrales)), columns=["fecha_hora", "central"])
base["empresa"] = base.central.map(empresas)
base["potencia_programada_mw"] = rng.uniform(0, 100, len(base)).round(3)
dia = (base.fecha_hora - pd.Timedelta(minutes=1)).dt.normalize()
hueco = dia.between("2021-12-10", "2021-12-14")
a1 = base[~hueco].copy()
a1["fecha_hora"] = a1.fecha_hora.dt.strftime("%Y-%m-%d %H:%M")
w(a1, "a1_despacho_programado", "a1_despacho_programado.parquet")

# A2 (todos los dias), con desviacion y programado_disponible
a2 = base.copy()
a2["potencia_programada_mw"] = np.where(hueco, np.nan, a2.potencia_programada_mw)
a2["potencia_ejecutada_mw"] = (base.potencia_programada_mw + rng.normal(0, 5, len(base))).clip(lower=0).round(3)
a2["desviacion_mw"] = a2.potencia_ejecutada_mw - a2.potencia_programada_mw
a2["origen"] = "ambas"
a2["programado_disponible"] = ~hueco
a2["fecha_hora"] = a2.fecha_hora.dt.strftime("%Y-%m-%d %H:%M")
w(a2, "a2_despacho_ejecutado", "a2_con_desviacion.parquet")

# A3: un costo por central y fecha
fechas = pd.date_range("2021-12-01", "2022-02-28", freq="D")
a3 = pd.DataFrame(list(itertools.product(fechas, centrales)), columns=["fecha", "central"])
a3["empresa"] = a3.central.map(empresas)
a3["grupo_elegido"] = a3.central + " G1"; a3["combustible_elegido"] = rng.choice(["GAS", "D2", "AGUA"], len(a3))
a3["tipo_elegido"] = "directa"; a3["confianza"] = "alta"; a3["potencia_efectiva_mw"] = 50.0; a3["cvnc_s_kwh"] = 0.01; a3["cvc_s_kwh"] = 0.1; a3["n_opciones"] = 1; a3["cv_min_s_kwh"] = 0.1
a3["fecha"] = a3.fecha.dt.strftime("%Y-%m-%d")
a3["costo_variable_s_kwh"] = rng.uniform(0.01, 0.5, len(a3)).round(4)
w(a3, "a3_costos_variables", "a3_costo_central_fecha.parquet")

# A4 30 min, 6 barras; una con ceros masivos (baja tension) y una con negativos
barras = {1: "ICA 220", 2: "PISCO 10", 3: "LIMA 220", 4: "TRUJILLO 220", 5: "CHIMBOTE 138", 6: "PIURA 60"}
a4 = pd.DataFrame(list(itertools.product(fh, barras)), columns=["fecha_hora", "barra_id"])
a4["fecha_hora"] = a4.fecha_hora.dt.strftime("%Y-%m-%d %H:%M")
a4["barra"] = a4.barra_id.map(barras); a4["barra_id"] = a4.barra_id.astype(str)
a4["costo_marginal_s_kwh"] = rng.uniform(0.05, 0.3, len(a4)).round(5)
a4.loc[(a4.barra_id == "2") & (a4.fecha_hora < "2022-01-15"), "costo_marginal_s_kwh"] = 0.0
a4.loc[a4.sample(10, random_state=1).index, "costo_marginal_s_kwh"] = -0.01
w(a4, "a4_costos_marginales", "a4_costos_marginales_30min.parquet")

# B (CSV, utf-8-sig como los escribe pipeline_persona_B.py) -- columnas reales
def wc(df, name, fname):
    d = os.path.join(b, name); os.makedirs(d, exist_ok=True)
    df.to_csv(os.path.join(d, fname), index=False, encoding="utf-8-sig")

FMT = "%Y-%m-%d %H:%M"
b4 = pd.DataFrame({"fecha_hora": fh.strftime(FMT)})
b4["id_dia"] = (fh - pd.Timedelta(minutes=1)).strftime("%Y%m%d")
b4["label_rdo"] = rng.choice([0, 1], len(b4), p=[0.74, 0.26])
b4["id_reprograma"] = np.where(b4.label_rdo == 1, b4.id_dia + "B", None)
b4["id_fuente"] = "indicesB.xlsx"; b4["n_archivos_con_rdo1"] = b4.label_rdo
b4["alfa"] = 1.0; b4["forzado_activo_b1"] = 0; b4["archivo"] = "indices.xlsx"
wc(b4[["fecha_hora","id_dia","id_reprograma","id_fuente","n_archivos_con_rdo1","label_rdo","alfa","forzado_activo_b1","archivo"]],
   "b4_pr06", "B4_pr06_rdo.csv")

ini = pd.to_datetime("2021-12-01") + pd.to_timedelta(rng.integers(0, 85 * 24, 200), unit="h")
fin = ini + pd.to_timedelta(rng.integers(1, 48, 200), unit="h")
b1 = pd.DataFrame({"fecha_inicio": ini.strftime(FMT), "fecha_fin": fin.strftime(FMT), "ubicacion": "UBIC",
                   "equipo": rng.choice(centrales, 200), "tipo_equipo": "UG", "tipo_mantenimiento": "M",
                   "prog": rng.choice(["PROGRAMADO", "FORZADO"], 200, p=[.9, .1]), "descripcion": "x",
                   "n_filas_origen": 1, "duracion_horas": ((fin - ini).total_seconds() / 3600).round(2)})
b1["instalacion_id"] = b1.ubicacion + " | " + b1.equipo
wc(b1, "b1_mantenimientos", "B1_mantenimientos.csv")

b2 = pd.DataFrame({"fecha_hora": pd.date_range("2024-11-25 00:30", periods=48, freq="30min").strftime(FMT)})
b2 = b2.merge(pd.DataFrame({"instalacion": centrales[:5]}), how="cross")
b2["tipo_instalacion"] = "UG"; b2["tipo_mantenimiento"] = "M"; b2["empresa"] = "E"; b2["potencia_indisponible_mw"] = 1.5
wc(b2, "b2_potencia_indisponible", "B2_potencia_indisponible.csv")

b3 = pd.DataFrame({"fecha": rng.choice(fechas.strftime("%Y-%m-%d"), 40), "letra_reprograma": rng.choice(list("BCD"), 40),
                   "motivo": "TEST", "motivo_categoria": "X", "costo_total_soles": rng.uniform(1e3, 1e5, 40).round(2),
                   "fecha_hora_inicio": "2022-01-10 10:00", "fecha_hora_fin": "2022-01-11 00:00",
                   "n_bloques": 28, "horas_cubiertas": 14.0, "archivo": "Reprog.xlsx"}).drop_duplicates(["fecha","letra_reprograma"])
wc(b3, "b3_reprograma", "B3_reprograma_diario.csv")
print("fixtures en", root, "| A2 filas:", len(a2), "| A4 filas:", len(a4))
