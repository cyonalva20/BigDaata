# -*- coding: utf-8 -*-
"""
4.1 EDA con el objetivo térmico (evento_termico_nop). Lee dataset_analitico_termico.parquet.
Genera: fig_4_1a_perfil_horario.png, fig_4_1b_serie_mensual.png, fig_4_1c_b1_vs_evento.png
        + resumen_eda_termico.txt (todas las cifras que se citan en el texto).
Todo sale de los datos: sin valores de respaldo. Los bloques sin etiqueta (hueco de A1 o A3) se excluyen, no se cuentan como 0.
"""
import os, io, sys
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
 
PARQUET = os.environ.get("PARQUET", "dataset_analitico_termico.parquet")
OUT = os.environ.get("SALIDA", "figuras_eda_termico")
os.makedirs(OUT, exist_ok=True)
rng = np.random.default_rng(42)
buf = io.StringIO()
def P(*a):
    print(*a); print(*a, file=buf)
 
d = pd.read_parquet(PARQUET)
d["fecha_hora"] = pd.to_datetime(d["fecha_hora"])
t = d.fecha_hora - pd.Timedelta(minutes=1)
d["dia"] = t.dt.normalize()
d["bloque"] = (t.dt.hour * 2 + t.dt.minute // 30 + 1).astype(int)       # 1..48 (fin de bloque)
d["mes"] = t.dt.to_period("M")
d["periodo"] = np.where(t >= "2024-01-01", "2024 (test)", "2022-2023 (entren.+valid.)")
lab = d[d.evento_termico_nop.notna()].copy()
P(f"Bloques totales: {len(d):,} | etiquetados: {len(lab):,} ({len(lab)/len(d):.1%}) | prevalencia global: {lab.evento_termico_nop.mean():.4f}")
 
# ---------- Fig 4.1-a: perfil por bloque horario, dos periodos, IC por remuestreo de días
def perfil(sub):
    M = sub.pivot_table(index="dia", columns="bloque", values="evento_termico_nop", aggfunc="mean").reindex(columns=range(1, 49))
    A = M.to_numpy(); nd = A.shape[0]
    est = np.nanmean(A, axis=0)
    bs = np.array([np.nanmean(A[rng.integers(0, nd, nd)], axis=0) for _ in range(500)])
    return est * 100, np.nanpercentile(bs, 2.5, axis=0) * 100, np.nanpercentile(bs, 97.5, axis=0) * 100, nd
fig, ax = plt.subplots(figsize=(10, 4.6))
cols = {"2022-2023 (entren.+valid.)": "#1f4e79", "2024 (test)": "#c0504d"}
res = {}
for per, sub in lab.groupby("periodo"):
    est, lo, hi, nd = perfil(sub); res[per] = est
    ax.plot(range(1, 49), est, color=cols[per], lw=2, label=f"{per} · {nd} días")
    ax.fill_between(range(1, 49), lo, hi, color=cols[per], alpha=.15)
ax.set_xticks([1, 8, 16, 24, 32, 40, 48]); ax.set_xticklabels(["00:30", "04:00", "08:00", "12:00", "16:00", "20:00", "00:00"])
ax.set_xlabel("Hora de fin del bloque de 30 min"); ax.set_ylabel("% de bloques con térmica cara no programada")
ax.set_title("Perfil horario del evento térmico no programado (IC 95 % por remuestreo de días)")
ax.grid(alpha=.3); ax.legend(); fig.tight_layout(); fig.savefig(f"{OUT}/fig_4_1a_perfil_horario.png", dpi=200); plt.close(fig)
for per, est in res.items():
    P(f"[a] {per}: máximo bloque {int(np.nanargmax(est))+1} ({np.nanmax(est):.1f} %), mínimo bloque {int(np.nanargmin(est))+1} ({np.nanmin(est):.1f} %), razón máx/mín {np.nanmax(est)/max(np.nanmin(est),1e-9):.2f}")
 
# ---------- Fig 4.1-b: serie mensual: prevalencia, cobertura de etiqueta y exposición
tot = d.groupby("mes").size()
m = lab.groupby("mes").agg(prev=("evento_termico_nop", "mean"), n=("evento_termico_nop", "size"),
                           soles=("costo_variable_nop_soles", "sum"), mwh=("mwh_termico_nop", "sum"))
m = m.reindex(tot.index); m["cobertura"] = (m.n.fillna(0) / tot)
m["prev"] = m.prev * 100; m["soles_M"] = m.soles / 1e6
idx = [str(p) for p in m.index]; x = np.arange(len(idx))
pobre = (m.cobertura < 0.5).to_numpy()
fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
a1.bar(x, m.prev.fillna(0), color=np.where(pobre, "#bbbbbb", "#1f4e79"))
a1.set_ylabel("% bloques con evento"); a1.set_title("Prevalencia mensual del evento (gris: < 50 % de bloques etiquetables por huecos de A1/A3)")
a2.bar(x, m.soles_M.fillna(0), color=np.where(pobre, "#bbbbbb", "#c0504d"))
a2.set_ylabel("Millones de S/ (costo variable)"); a2.set_title("Exposición económica mensual: costo variable de la energía térmica cara no programada")
a2.set_xticks(x[::3]); a2.set_xticklabels(idx[::3], rotation=45, ha="right")
for a in (a1, a2): a.grid(alpha=.3, axis="y")
fig.tight_layout(); fig.savefig(f"{OUT}/fig_4_1b_serie_mensual.png", dpi=200); plt.close(fig)
P("\n[b] Meses con cobertura < 50 %:", [str(i) for i, p in zip(m.index, pobre) if p])
ok = m[~pobre]
top = ok.soles_M.sort_values(ascending=False).head(5)
P("[b] Top 5 meses por exposición (S/ millones):", {str(k): round(v, 2) for k, v in top.items()})
P(f"[b] Exposición total en meses con cobertura suficiente: S/ {ok.soles_M.sum():.1f} M; los 5 meses mayores concentran {top.sum()/ok.soles_M.sum():.1%}")
yr = lab.groupby(lab.dia.dt.year).agg(prev=("evento_termico_nop", "mean"), mwh=("mwh_termico_nop", "sum"),
                                       soles_M=("costo_variable_nop_soles", lambda s: s.sum() / 1e6), n=("evento_termico_nop", "size"))
P("\n[b] Por año operativo:\n" + yr.round(3).to_string())
mes_num = lab.groupby(lab.mes.dt.month).evento_termico_nop.mean() * 100
P("[b] Prevalencia por mes del año (todos los años, %):", mes_num.round(1).to_dict())
 
# ---------- Episodios (rachas de bloques consecutivos con evento, solo entre bloques etiquetados)
s = lab.set_index("fecha_hora").evento_termico_nop.astype(int).sort_index()
gap = s.index.to_series().diff() != pd.Timedelta(minutes=30)
runid = ((s != s.shift()) | gap).cumsum()
runs = s.groupby(runid).agg(["first", "size"]); ep = runs[runs["first"] == 1]["size"]
P(f"\n[ep] Episodios: {len(ep):,} | duración mediana {ep.median():.0f} bloques ({ep.median()*0.5:.1f} h) | p90 {ep.quantile(.9):.0f} bloques | máx {ep.max()} bloques")
 
# ---------- Fig 4.1-c: ¿lo que se conoce de B1 distingue el evento? (RR con IC por remuestreo de días)
if "forzado_activo_b1" in lab.columns:
    sub = lab.dropna(subset=["forzado_activo_b1"])
    def rr(df):
        g = df.groupby("forzado_activo_b1").evento_termico_nop.mean()
        return g.get(1.0, np.nan), g.get(0.0, np.nan)
    p1, p0 = rr(sub)
    frames = [g for _, g in sub.groupby("dia")]          # un DataFrame por día operativo
    bs = []
    for _ in range(300):
        pick = rng.integers(0, len(frames), len(frames))  # índices enteros: sin problemas de tipos de fecha
        b = pd.concat([frames[i] for i in pick]); a1_, a0_ = rr(b); bs.append(a1_ / a0_ if a0_ else np.nan)
    lo, hi = np.nanpercentile(bs, [2.5, 97.5])
    phi = sub[["forzado_activo_b1", "evento_termico_nop"]].astype(float).corr().iloc[0, 1]
    fig, ax = plt.subplots(figsize=(5.5, 4.2))
    ax.bar(["Sin evento forzado\nactivo en B1", "Con evento forzado\nactivo en B1"], [p0 * 100, p1 * 100], color=["#7f7f7f", "#c0504d"])
    ax.set_ylabel("% de bloques con evento térmico no programado")
    ax.set_title(f"Razón de tasas {p1/p0:.2f} (IC 95 %: {lo:.2f}-{hi:.2f})", fontsize=10); ax.grid(alpha=.3, axis="y")
    fig.tight_layout(); fig.savefig(f"{OUT}/fig_4_1c_b1_vs_evento.png", dpi=200); plt.close(fig)
    P(f"\n[c] P(evento | B1 forzado activo)={p1:.4f}; P(evento | no)={p0:.4f}; RR={p1/p0:.3f} IC95 [{lo:.3f}, {hi:.3f}]; phi={phi:.4f}; bloques con B1 activo: {int((sub.forzado_activo_b1==1).sum()):,}")
else:
    P("[c] No hay forzado_activo_b1 en el parquet; se omite la Fig. 4.1-c")
 
if "label_rdo" in lab.columns:
    sub = lab.dropna(subset=["label_rdo"])
    P(f"\n[rdo] phi(evento térmico, RDO) = {sub[['evento_termico_nop','label_rdo']].corr().iloc[0,1]:.4f} | P(evento|RDO=1)={sub[sub.label_rdo==1].evento_termico_nop.mean():.4f} | P(evento|RDO=0)={sub[sub.label_rdo==0].evento_termico_nop.mean():.4f}")
open(f"{OUT}/resumen_eda_termico.txt", "w", encoding="utf-8").write(buf.getvalue())
print(f"\n✓ Figuras y resumen en '{OUT}/'")