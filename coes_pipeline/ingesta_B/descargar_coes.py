#!/usr/bin/env python3
"""
descargar_coes.py - Baja del portal del COES los archivos DIARIOS de:
  * Reprograma Diario de Operacion  -> Reprog_DDMML.xlsx          (B3: motivo y costo)
  * Indices PR06 (flag RDO)         -> indicesAAAAMMDD_L.xlsx     (B4: label)

No necesita instalar nada (solo Python). Guarda en la estructura que ya lee pipeline_persona_B.py:
  reprograma/AAAA/MM/DD/L/Reprog_DDMML.xlsx
  pr06/AAAA/MM/DD/L/indicesAAAAMMDD_L.xlsx

PRIMERO PRUEBA (3 fechas, ~1 minuto):
  py descargar_coes.py --probar

DESPUES, TODO EL PERIODO (puede tardar 1-2 horas; se puede cortar con Ctrl+C y volver a correr, continua donde quedo):
  py descargar_coes.py --desde 2021-12-01 --hasta 2024-11-30

Patron de direccion (tomado de las descargas reales):
  .../Portal/browser/download?url=Operacion/Programa de Operacion/Reprograma Diario Operacion/
        2024/11_NOVIEMBRE/Dia 25/Reprog 2511B/Reprog_2511B.xlsx      (con tildes en Operacion y Dia)
"""
import argparse
import csv
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

BASE_COES = "https://www.coes.org.pe/Portal/browser/download?url="
RAIZ = "Operación/Programa de Operación/Reprograma Diario Operación"
MESES = {1: ["ENERO"], 2: ["FEBRERO"], 3: ["MARZO"], 4: ["ABRIL"], 5: ["MAYO"], 6: ["JUNIO"],
         7: ["JULIO"], 8: ["AGOSTO"], 9: ["SETIEMBRE", "SEPTIEMBRE"], 10: ["OCTUBRE"],
         11: ["NOVIEMBRE"], 12: ["DICIEMBRE"]}
LETRAS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
UA = "Mozilla/5.0 (compatible; proyecto-academico-PR06)"


class Descargador:
    def __init__(self, base, salida, pausa, verbose):
        self.base, self.salida, self.pausa, self.verbose = base, Path(salida), pausa, verbose
        self.ultimo = 0.0
        self.pref_mes = {}      # mes -> nombre de carpeta de mes que funciono (p. ej. SETIEMBRE)
        self.pref_dia = None    # 'cero' (Dia 05) o 'sin' (Dia 5)
        self.log = []

    # ---------------------------------------------------------------- red
    def _get(self, ruta):
        """Devuelve (estado, bytes|None). estado: 'ok' | 'no_existe' | 'error'."""
        url = self.base + urllib.parse.quote(ruta, safe="/")
        espera = self.pausa - (time.time() - self.ultimo)
        if espera > 0:
            time.sleep(espera)
        estado, datos, detalle = "error", None, ""
        for intento in range(4):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=60) as r:
                    cuerpo = r.read()
                if cuerpo[:2] == b"PK" and len(cuerpo) > 1000:       # un .xlsx real es un zip
                    estado, datos, detalle = "ok", cuerpo, f"{len(cuerpo)} bytes"
                else:
                    estado, detalle = "no_existe", "respuesta no es un xlsx"
                break
            except urllib.error.HTTPError as e:
                if e.code in (400, 404, 500):                        # el portal responde asi a rutas que no existen
                    estado, detalle = "no_existe", f"HTTP {e.code}"
                    break
                detalle = f"HTTP {e.code}"
            except Exception as e:                                   # red, timeout...
                detalle = type(e).__name__
            time.sleep(2 * (intento + 1))
        self.ultimo = time.time()
        if self.verbose:
            print(f"      [{estado:9}] {detalle:22} {ruta}")
        return estado, datos

    # --------------------------------------------------- rutas candidatas
    def _carpetas_dia(self, d):
        meses = list(MESES[d.month])
        if d.month in self.pref_mes:
            meses.sort(key=lambda m: m != self.pref_mes[d.month])
        dias = [("cero", f"Día {d.day:02d}"), ("sin", f"Día {d.day}")]
        if d.day >= 10:
            dias = dias[:1]
        if self.pref_dia:
            dias.sort(key=lambda x: x[0] != self.pref_dia)
        for mes in meses:
            for clave, dia in dias:
                yield mes, clave, f"{RAIZ}/{d.year}/{d.month:02d}_{mes}/{dia}"

    @staticmethod
    def _archivos(d, letra):
        ddmm = f"{d.day:02d}{d.month:02d}"
        return (f"Reprog {ddmm}{letra}",
                f"Reprog_{ddmm}{letra}.xlsx",
                f"indices{d:%Y%m%d}_{letra}.xlsx")

    def _destino(self, tipo, d, letra, nombre):
        return self.salida / tipo / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}" / letra / nombre

    def _guardar(self, destino, datos):
        destino.parent.mkdir(parents=True, exist_ok=True)
        tmp = destino.with_suffix(".tmp")
        tmp.write_bytes(datos)
        tmp.replace(destino)

    def _registrar(self, d, letra, archivo, estado):
        self.log.append((d.isoformat(), letra, archivo, estado))

    # ---------------------------------------------------------- un dia
    def dia(self, d):
        """Baja todas las letras del dia. Devuelve (resumen_texto, completo)."""
        completo, partes = True, []
        base_dia = None  # carpeta del dia que funciono
        for letra in LETRAS:
            carpeta_l, f_reprog, f_idx = self._archivos(d, letra)
            dest_r = self._destino("reprograma", d, letra, f_reprog)
            estado, datos = "ok", None
            if dest_r.exists() and dest_r.stat().st_size > 1000 and base_dia is not None:
                pass  # ya descargado antes
            else:
                candidatos = ([base_dia] if base_dia else [c for c in self._carpetas_dia(d)])
                estado = "no_existe"
                for cand in candidatos:
                    mes, clave, carpeta_dia = cand if base_dia is None else (None, None, base_dia)
                    est, datos = self._get(f"{carpeta_dia}/{carpeta_l}/{f_reprog}")
                    if est == "ok":
                        if base_dia is None:
                            base_dia = carpeta_dia
                            self.pref_mes[d.month], self.pref_dia = mes, clave
                        estado = "ok"
                        break
                    if est == "error":
                        estado = "error"
                        break
                if estado == "ok" and datos:
                    self._guardar(dest_r, datos)
            if estado == "error":
                completo = False
                self._registrar(d, letra, f_reprog, "error_red")
                break
            if estado == "no_existe":
                if letra == "A":      # puede faltar A pero existir B: se prueba B antes de rendirse
                    self._registrar(d, letra, f_reprog, "no_existe")
                    continue
                self._registrar(d, letra, f_reprog, "no_existe")
                break
            self._registrar(d, letra, f_reprog, "ok")
            partes.append(f"{letra}")
            # indices de la misma letra (A normalmente no tiene)
            dest_i = self._destino("pr06", d, letra, f_idx)
            if dest_i.exists() and dest_i.stat().st_size > 1000:
                partes[-1] += "+idx"
                continue
            est, datos = self._get(f"{base_dia}/{carpeta_l}/{f_idx}")
            if est == "ok":
                self._guardar(dest_i, datos)
                partes[-1] += "+idx"
                self._registrar(d, letra, f_idx, "ok")
            elif est == "error":
                completo = False
                self._registrar(d, letra, f_idx, "error_red")
            else:
                self._registrar(d, letra, f_idx, "no_existe")
        return (" ".join(partes) if partes else "sin reprogramas"), completo


def main():
    ap = argparse.ArgumentParser(description="Descarga Reprograma Diario e indices PR06 del COES")
    ap.add_argument("--desde", default="2021-12-01")
    ap.add_argument("--hasta", default="2024-11-30")
    ap.add_argument("--salida", default=".", help="carpeta base (se crean reprograma/ y pr06/ dentro)")
    ap.add_argument("--pausa", type=float, default=0.4, help="segundos entre peticiones")
    ap.add_argument("--probar", action="store_true", help="prueba con 3 fechas y muestra cada direccion")
    ap.add_argument("--rehacer", action="store_true", help="ignorar dias ya marcados como completos")
    ap.add_argument("--base-url", default=BASE_COES, help=argparse.SUPPRESS)
    a = ap.parse_args()

    salida = Path(a.salida)
    salida.mkdir(parents=True, exist_ok=True)
    marcas = salida / "descarga_dias_completos.txt"
    completos = set(marcas.read_text().split()) if marcas.exists() and not a.rehacer else set()

    if a.probar:
        fechas = [date(2024, 11, 25), date(2022, 9, 14), date(2022, 1, 5)]
        print("PRUEBA con 3 fechas: 25/11/2024 (la que ya conoces), 14/09/2022 (mes setiembre) y 05/01/2022 (dia de 1 digito)\n")
    else:
        d0, d1 = (datetime.strptime(x, "%Y-%m-%d").date() for x in (a.desde, a.hasta))
        fechas = [d0 + timedelta(days=i) for i in range((d1 - d0).days + 1)]

    dl = Descargador(a.base_url, salida, a.pausa, verbose=a.probar)
    t0, hechos, saltados, sin_datos, con_error = time.time(), 0, 0, 0, []
    try:
        for i, d in enumerate(fechas, 1):
            if d.isoformat() in completos and not a.probar:
                saltados += 1
                continue
            resumen, completo = dl.dia(d)
            if resumen == "sin reprogramas":
                sin_datos += 1
            if completo:
                completos.add(d.isoformat())
                with marcas.open("a") as f:
                    f.write(d.isoformat() + "\n")
            else:
                con_error.append(d.isoformat())
            hechos += 1
            if a.probar or i % 10 == 0 or i == len(fechas):
                faltan = len(fechas) - i
                print(f"[{i}/{len(fechas)}] {d}: {resumen}" + ("" if completo else "  (ERROR DE RED, se reintentara)")
                      + ("" if a.probar else f"   | quedan {faltan} dias"))
    except KeyboardInterrupt:
        print("\nInterrumpido. Vuelve a correr el mismo comando para continuar donde quedo.")
    finally:
        with (salida / "descarga_log.csv").open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if f.tell() == 0:
                w.writerow(["fecha", "letra", "archivo", "estado"])
            w.writerows(dl.log)

    n_ok = sum(1 for r in dl.log if r[3] == "ok")
    print(f"\nListo en {(time.time() - t0) / 60:.1f} min: {hechos} dias procesados, {saltados} ya estaban completos, "
          f"{sin_datos} sin reprogramas, {n_ok} archivos descargados.")
    if con_error:
        print(f"{len(con_error)} dias con error de red (primeros: {', '.join(con_error[:5])}). Vuelve a correr el comando.")
    if a.probar:
        if n_ok == 0:
            print("\nNO SE DESCARGO NADA. Copia y pega aqui TODO el texto de arriba (las lineas con [no_existe]/[error]) "
                  "para que ajuste las rutas.")
        else:
            print(f"\nLa prueba funciono. Revisa que se hayan creado archivos en {salida.resolve()}\\reprograma y \\pr06.")


if __name__ == "__main__":
    main()
