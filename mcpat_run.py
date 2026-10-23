#!/usr/bin/env python3
"""
mcpat_run.py - Potencia, Energia y EDP con McPAT para las corridas del DSE.

Formulas de la guia del curso:
    Energia = (Total Leakage + Runtime Dynamic) * system.cpu.cpi
    EDP     = Energia * system.cpu.cpi

Ejecutar SIEMPRE desde la raiz de gem5:
    cd ~/gem5
    # Prueba con una sola corrida, comparando el conversor original y el corregido:
    python3 mcpat_run.py --compare dse_runs_pilot/W4_iq16_rob128_fp2_st1_bp10_l1d64kB
    # Todas las corridas de una carpeta (reanudable) -> <carpeta>_power.csv:
    python3 mcpat_run.py --all dse_runs --jobs 6

Por cada corrida deja en su carpeta:
    mcpat_<conv>.xml   entrada generada por el conversor
    mcpat_<conv>.out   salida completa de McPAT
"""
import argparse
import csv
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.abspath(os.getcwd())
MCPAT = os.path.join(ROOT, "mcpat/mcpat")       # misma ruta que usan las diapositivas del curso
TEMPLATE = os.path.join(ROOT, "scripts/McPAT/ARM_A76_2.1GHz.xml")
CONVERTERS = {
    "v25": os.path.join(ROOT, "scripts/McPAT/gem5toMcPAT_cortexA76_v25.py"),
    "orig": os.path.join(ROOT, "scripts/McPAT/gem5toMcPAT_cortexA76.py"),
}
TIMEOUT_S = 1800

FIELDS = ["run", "converter", "cpi", "ipc", "total_leakage_W", "runtime_dynamic_W",
          "power_W", "energy", "edp", "peak_power_W", "area_mm2",
          "convert_s", "mcpat_s", "status"]


def read_stat(stats_path, name):
    with open(stats_path, errors="replace") as f:
        for line in f:
            p = line.split()
            if len(p) >= 2 and p[0] == name:
                return float(p[1])
    return None


def parse_mcpat(out_path):
    """Toma los valores del bloque 'Processor:' (los primeros que imprime McPAT)."""
    with open(out_path, errors="replace") as f:
        txt = f.read()
    i = txt.find("Processor:")
    if i < 0:
        return None
    # Solo el bloque del procesador: termina en la primera linea en blanco. Asi nunca
    # se toma por error un valor de un sub-bloque (cores, L2, NoC...).
    block = txt[i:].split("\n\n", 1)[0]

    def grab(label):
        m = re.search(r"^\s*" + re.escape(label) + r"\s*=\s*([-+0-9.eE]+)", block, re.M)
        return float(m.group(1)) if m else None

    return {"total_leakage_W": grab("Total Leakage"),
            "runtime_dynamic_W": grab("Runtime Dynamic"),
            "peak_power_W": grab("Peak Power"),
            "area_mm2": grab("Area")}


def process(rundir, conv):
    rundir = os.path.abspath(rundir)          # el conversor se ejecuta con cwd=rundir
    run = os.path.basename(os.path.normpath(rundir))
    row = {"run": run, "converter": conv, "status": ""}
    stats = os.path.join(rundir, "stats.txt")
    cfg = os.path.join(rundir, "config.json")
    for p in (stats, cfg):
        if not os.path.isfile(p):
            row["status"] = "NO_" + os.path.basename(p)
            return row
    xml = os.path.join(rundir, f"mcpat_{conv}.xml")
    out = os.path.join(rundir, f"mcpat_{conv}.out")

    # 1) gem5 -> XML. El conversor escribe 'config.xml' en el directorio actual.
    t0 = time.time()
    r = subprocess.run([sys.executable, CONVERTERS[conv], stats, cfg, TEMPLATE],
                       cwd=rundir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       text=True, timeout=TIMEOUT_S)
    row["convert_s"] = f"{time.time() - t0:.1f}"
    gen = os.path.join(rundir, "config.xml")
    if r.returncode != 0 or not os.path.isfile(gen):
        row["status"] = "CONVERT_FAIL"
        with open(os.path.join(rundir, f"mcpat_{conv}_convert.log"), "w") as f:
            f.write(r.stdout)
        return row
    os.replace(gen, xml)
    if conv == "v25" and "ADVERTENCIA" in r.stdout:
        row["status"] = "WARN_ZERO_FIELDS "

    # 2) McPAT
    t0 = time.time()
    with open(out, "w") as f:
        r = subprocess.run([MCPAT, "-infile", xml, "-print_level", "1"],
                           cwd=rundir, stdout=f, stderr=subprocess.STDOUT, timeout=TIMEOUT_S)
    row["mcpat_s"] = f"{time.time() - t0:.1f}"
    res = parse_mcpat(out)
    if r.returncode != 0 or not res or None in (res["total_leakage_W"], res["runtime_dynamic_W"]):
        row["status"] += "MCPAT_FAIL"
        return row

    # 3) Energia y EDP segun la guia
    cpi = read_stat(stats, "system.cpu.cpi")
    ipc = read_stat(stats, "system.cpu.ipc")
    power = res["total_leakage_W"] + res["runtime_dynamic_W"]
    energy = power * cpi
    row.update(res)
    row.update({"cpi": cpi, "ipc": ipc, "power_W": power,
                "energy": energy, "edp": energy * cpi})
    row["status"] += "OK"
    return row


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--compare", metavar="RUNDIR", help="una corrida, conversor original vs corregido")
    g.add_argument("--all", metavar="RUNS_ROOT", help="todas las corridas de una carpeta")
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--converter", choices=list(CONVERTERS), default="v25")
    a = ap.parse_args()

    needed = [MCPAT, TEMPLATE] + ([CONVERTERS["v25"], CONVERTERS["orig"]] if a.compare
                                  else [CONVERTERS[a.converter]])
    for p in needed:
        if not os.path.exists(p):
            sys.exit(f"No encuentro {p}. Ejecuta desde la raiz de gem5 (y compila McPAT).")

    if a.compare:
        rows = [process(a.compare, c) for c in ("orig", "v25")]
        print(f"\nCorrida: {rows[0]['run']}")
        print(f"{'':22s}{'original':>16s}{'corregido v25':>16s}")
        for k in ("total_leakage_W", "runtime_dynamic_W", "power_W", "cpi",
                  "energy", "edp", "area_mm2", "mcpat_s", "status"):
            v = [r.get(k, "") for r in rows]
            v = [f"{x:.4f}" if isinstance(x, float) else str(x) for x in v]
            print(f"{k:22s}{v[0]:>16s}{v[1]:>16s}")
        return

    runs = sorted(d for d in os.listdir(a.all)
                  if os.path.isfile(os.path.join(a.all, d, "stats.txt")))
    out_csv = os.path.normpath(a.all) + "_power.csv"
    done = set()
    if os.path.exists(out_csv):                      # reanudable
        with open(out_csv) as f:
            done = {r["run"] for r in csv.DictReader(f) if r["status"].endswith("OK")}
    pending = [r for r in runs if r not in done]
    print(f"{len(runs)} corridas, {len(pending)} pendientes de McPAT")
    new = not os.path.exists(out_csv)
    with open(out_csv, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        if new:
            w.writeheader()
        with ThreadPoolExecutor(max_workers=a.jobs) as ex:
            futs = {ex.submit(process, os.path.join(a.all, r), a.converter): r for r in pending}
            for i, fut in enumerate(as_completed(futs), 1):
                try:
                    row = fut.result()
                except Exception as e:
                    row = {"run": futs[fut], "converter": a.converter, "status": "ERROR " + repr(e)}
                w.writerow(row)
                f.flush()
                print(f"[{i}/{len(pending)}] {row['status']:12s} {row['run']} "
                      f"P={row.get('power_W', '')} EDP={row.get('edp', '')}", flush=True)
    print(f"Resultados en {out_csv}")


if __name__ == "__main__":
    main()
