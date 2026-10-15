import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score
)

print("=" * 65)
print("ENTRENAMIENTO Y EVALUACIÓN DEL MODELO BASE (SUBSECCIÓN 4.2)")
print("=" * 65)

# 1. CARGA DEL DATASET ANALÍTICO GENERADO EN 4.1
ruta_data = "dataset_analitico_eda.parquet"
df = pd.read_parquet(ruta_data)
df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
df = df.sort_values("fecha_hora").reset_index(drop=True)

# 2. INGENIERÍA DE CARACTERÍSTICAS TEMPORALES Y OPERATIVAS
df["hora"] = df["fecha_hora"].dt.hour
df["dia_semana"] = df["fecha_hora"].dt.dayofweek
df["mes"] = df["fecha_hora"].dt.month
# Rampa de demanda operativa (diferencia con el bloque anterior de 30 min)
df["rampa_demanda_mw"] = df["generacion_total_mw"].diff().fillna(0)

# Asegurar tipo numérico para el predictor forzado de B1
if "forzado_inicio_3h_previas_b1" in df.columns:
    col_str = df["forzado_inicio_3h_previas_b1"].astype(str).str.strip().str.lower()
    df["forzado_flag"] = col_str.map({"true": 1, "1": 1, "1.0": 1, "false": 0, "0": 0, "0.0": 0}).fillna(0).astype(int)
else:
    df["forzado_flag"] = 0

features = [
    "hora",
    "bloque_30min",
    "dia_semana",
    "mes",
    "generacion_total_mw",
    "rampa_demanda_mw",
    "forzado_flag"
]
target = "label_rdo"

# 3. PARTICIÓN TEMPORAL ESTRICTA (TRAIN: 2021-2023 | TEST: 2024)
fecha_corte = pd.Timestamp("2024-01-01 00:00")
train_mask = df["fecha_hora"] < fecha_corte
test_mask = df["fecha_hora"] >= fecha_corte

X_train = df.loc[train_mask, features]
y_train = df.loc[train_mask, target]

X_test = df.loc[test_mask, features]
y_test = df.loc[test_mask, target]

print(f"\nConjunto de Entrenamiento (2021-12 a 2023-12): {len(X_train):,} bloques")
print(f"  -> Positivos RDO=1: {y_train.sum():,} ({y_train.mean():.2%})")
print(f"Conjunto de Prueba (2024-01 a 2024-11):       {len(X_test):,} bloques")
print(f"  -> Positivos RDO=1: {y_test.sum():,} ({y_test.mean():.2%})")

# 4. ENTRENAMIENTO DEL MODELO BASE (Árbol de Decisión Acotado)
# Se usa max_depth=5 para servir como Baseline interpretable y evitar sobreajuste
modelo_base = DecisionTreeClassifier(
    max_depth=5,
    class_weight="balanced",  # compensar el desbalance natural 26% vs 74%
    random_state=42
)
modelo_base.fit(X_train, y_train)

# 5. PREDICCIÓN Y EVALUACIÓN
y_pred = modelo_base.predict(X_test)
y_proba = modelo_base.predict_proba(X_test)[:, 1]

# Métricas técnicas de la Línea Base
auc_roc = roc_auc_score(y_test, y_proba)
pr_auc = average_precision_score(y_test, y_proba)
f1_rdo = f1_score(y_test, y_pred, pos_label=1)
prec_rdo = precision_score(y_test, y_pred, pos_label=1)
rec_rdo = recall_score(y_test, y_pred, pos_label=1)
cm = confusion_matrix(y_test, y_pred)

print("\n" + "=" * 65)
print("RESULTADOS DE LA LÍNEA BASE TÉCNICA (CONJUNTO DE PRUEBA 2024)")
print("=" * 65)
print(f"AUC-ROC:                      {auc_roc:.4f}")
print(f"PR-AUC (Precision-Recall):    {pr_auc:.4f}")
print(f"F1-Score (Clase RDO=1):       {f1_rdo:.4f}")
print(f"Precisión (Clase RDO=1):      {prec_rdo:.4f}")
print(f"Recall / Sensibilidad (RDO=1):{rec_rdo:.4f}")

print("\nMatriz de Confusión:")
print(f"  [TN: {cm[0,0]:>5} | FP: {cm[0,1]:>5}]")
print(f"  [FN: {cm[1,0]:>5} | TP: {cm[1,1]:>5}]")

# 6. IMPORTANCIA DE VARIABLES DEL MODELO BASE
imp = pd.Series(modelo_base.feature_importances_, index=features).sort_values(ascending=False)
print("\nImportancia de Variables (Feature Importance):")
for f, val in imp.items():
    print(f"  - {f:<22}: {val:.4f}")

# Guardar matriz de confusión como evidencia para el informe
os.makedirs("evidencias_modelo", exist_ok=True)
fig, ax = plt.subplots(figsize=(5, 4))
im = ax.imshow(cm, cmap="Blues", interpolation="nearest")
plt.colorbar(im)
ax.set_xticks([0, 1])
ax.set_yticks([0, 1])
ax.set_xticklabels(["Sin RDO (0)", "Con RDO (1)"], fontweight="bold")
ax.set_yticklabels(["Sin RDO (0)", "Con RDO (1)"], fontweight="bold")
ax.set_xlabel("Predicción del Modelo Base", fontweight="bold")
ax.set_ylabel("Valor Real Histórico", fontweight="bold")

for i in range(2):
    for j in range(2):
        ax.text(j, i, f"{cm[i, j]:,}", ha="center", va="center", color="black" if cm[i, j] < cm.max()/2 else "white", fontweight="bold")

plt.title("Figura 4.2. Matriz de Confusión - Modelo Base (Test 2024)", fontweight="bold", pad=12)
plt.tight_layout()
plt.savefig("evidencias_modelo/fig_4_2_matriz_confusion_baseline.png", dpi=300)
plt.close()

print("\n✓ Evidencia guardada en: evidencias_modelo/fig_4_2_matriz_confusion_baseline.png")
print("=" * 65)