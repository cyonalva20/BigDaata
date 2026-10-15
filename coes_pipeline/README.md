# Pipeline COES-SEIN (Bronze -> Silver -> Gold)

Requisitos: Java 17 u 21, Python 3.10+, `pip install -r requirements.txt`.

## 1. Prueba local con mini dataset (solo valida el codigo; datos artificiales)
    python make_fixtures.py lake_test
    LAKE_ROOT=lake_test python pipeline.py            # genera lake_test/silver, gold, logs/run_1x.json

## 2. Corrida con datos REALES (local)
Copiar los archivos reales a `lake_real/bronze/<fuente>/`:
    a1_despacho_programado/  a2_despacho_ejecutado/  a3_costos_variables/  a4_costos_marginales/
    b1_mantenimientos/  b3_reprograma/  b4_pr06/
    LAKE_ROOT=lake_real ESCALA=1x /usr/bin/time -v python pipeline.py 2> tiempo_1x.txt
    (en tiempo_1x.txt: "Maximum resident set size" = RAM pico; "Elapsed" = tiempo)

## 3. Optimizacion medida (broadcast join)
    LAKE_ROOT=lake_real python bench_broadcast.py     # -> logs/bench_broadcast.json

## 4. Escalabilidad (la replica es SOLO para medir rendimiento)
    python replicar.py lake_real lake_x10 10
    LAKE_ROOT=lake_x10 ESCALA=10x /usr/bin/time -v python pipeline.py 2> tiempo_10x.txt

## 5. AWS: LAKE_ROOT=s3://<bucket> (el mismo codigo; ver Fase 3)

## Memoria
Spark local usa `DRIVER_MEM` (por defecto 4g). Ajustar a ~60-70 % de la RAM libre:  `DRIVER_MEM=8g LAKE_ROOT=lake_real python pipeline.py`

## Que archivo va en cada carpeta de Bronze (UN solo archivo de datos por carpeta)
    a1_despacho_programado/  a1_despacho_programado.parquet
    a2_despacho_ejecutado/   a2_con_desviacion.parquet            (NO el a2_despacho_ejecutado.parquet)
    a3_costos_variables/     a3_costo_central_fecha.parquet       (NO a3_costos_variables.parquet)
    a4_costos_marginales/    a4_costos_marginales_30min.parquet   (NO el de 15 min)
    b1_mantenimientos/       B1_mantenimientos.csv
    b2_potencia_indisponible/ B2_potencia_indisponible.csv
    b3_reprograma/           B3_reprograma_diario.csv
    b4_pr06/                 B4_pr06_rdo.csv                      (NO por_archivo ni diagnosticos)

## Estructura del repositorio
    ingesta_B/   scripts de Persona B (descarga del COES, limpieza en pandas, verificaciones). NO se vuelven a ejecutar: sus CSV ya existen.
    ingesta_A/   [PENDIENTE: scripts de Persona A: a1_*.py, a2_*.py, a3_final.py, cv_scraper.py, a4_*.py]
    pipeline.py  Bronze -> Silver -> Gold (PySpark)   |  bench_broadcast.py  |  replicar.py  |  make_fixtures.py
