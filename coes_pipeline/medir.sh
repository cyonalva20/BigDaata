#!/bin/bash
ESC=${1:-1x}; MEM=${2:-6g}; LAKE=${LAKE:-lake_real}
mkdir -p evidencias
rm -f evidencias/ram_$ESC.log
( while true; do ps -C java -o rss= 2>/dev/null | sort -n | tail -1 >> evidencias/ram_$ESC.log; sleep 1; done ) &
SAMP=$!
S=$(date +%s)
LAKE_ROOT=$LAKE ESCALA=$ESC DRIVER_MEM=$MEM python pipeline.py > evidencias/salida_$ESC.txt 2>&1
RC=$?
E=$(date +%s)
kill $SAMP 2>/dev/null
PICO=$(sort -n evidencias/ram_$ESC.log | tail -1 | awk '{print int($1/1024)}')
cp $LAKE/logs/run_$ESC.json evidencias/run_${ESC}_$(date +%H%M%S).json 2>/dev/null
echo "$(date -Iseconds) escala=$ESC exit=$RC tiempo_total_s=$((E-S)) ram_pico_java_MB=$PICO driver_mem=$MEM" | tee -a evidencias/resumen.txt
