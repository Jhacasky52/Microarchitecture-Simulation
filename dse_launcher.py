#!/usr/bin/env python3
"""
dse_launcher.py - Lanzador del DSE (gem5 + CortexA76.py, workload mp3_dec)

Ejecutar SIEMPRE desde la raiz de gem5:
    cd ~/gem5
    python3 dse_launcher.py --mode pilot --dry-run      # ver comandos y estimacion
    python3 dse_launcher.py --mode pilot --jobs 6       # corrida piloto (12 configs)
    python3 dse_launcher.py --mode full  --jobs 6       # DSE completo (reanudable)
    python3 dse_launcher.py --collect                   # consolidar resultados en CSV

Requiere mcpat_run.py en la misma carpeta (~/gem5) y McPAT en ~/gem5/mcpat/mcpat.
Al terminar cada simulacion se ejecuta McPAT con el conversor del profesor
(scripts/McPAT/gem5toMcPAT_cortexA76.py + ARM_A76_2.1GHz.xml), como en las
diapositivas. Usa --no-mcpat para desactivarlo.

Caracteristicas:
  * Reanudable: salta las corridas que ya tienen un stats.txt completo.
  * argv identico en todas las corridas (cada simulacion usa su carpeta como cwd
    y escribe 'out.wav' relativo), para que las diferencias se deban solo a la
    microarquitectura.
  * Verifica en config.ini que cada parametro se aplico de verdad.
  * Borra el .wav (12 MB) al terminar cada corrida.
  * Registra cada resultado en dse_runs/manifest.csv.
  * Reanudable tambien para McPAT: si una simulacion termino pero falta McPAT,
    al relanzar solo se ejecuta McPAT.
"""
import argparse
import configparser
import csv
import itertools
import os
import random
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import mcpat_run                 # conversor del profesor + McPAT (mismo directorio)

# ---------------------------------------------------------------- rutas
ROOT = os.path.abspath(os.getcwd())
GEM5 = os.path.join(ROOT, "build/ARM/gem5.fast")
SCRIPT = os.path.join(ROOT, "scripts/CortexA76_scripts_gem5/CortexA76.py")
BINARY = os.path.join(ROOT, "workloads/mp3_dec/mp3_dec")
MP3_IN = os.path.join(ROOT, "workloads/mp3_dec/mp3dec_testfile.mp3")
OUT_ROOT = os.path.join(ROOT, "dse_runs")
TIMEOUT_S = 3 * 3600          # una corrida normal tarda ~14 min
EST_RUN_S = 820               # para la estimacion de tiempo (piloto: 818 s medio con 6 en paralelo)
MCPAT_CONV = "orig"           # conversor del profesor; "v25" para el corregido

# ---------------------------------------------------------------- espacio de diseno
# Branch predictor (CortexA76.py): 0 BiModeBP, 1 LTAGE, 7 TAGE,
#   8 TAGE_SC_L_64KB, 9 TAGE_SC_L_8KB, 10 TournamentBP (default)
GRID = {
    "W":   [2, 4, 6, 8],          # fetch = decode = rename = commit
    "iq":  [16, 32, 64, 96],      # piloto: 32->64 satura con W=4, pero con W=8 la IQ=64 aun se llena
    "rob": [64, 128, 256],
    "fp":  [2, 3, 4],             # unidades FP/SIMD (FUList3)
    "st":  [1, 2],                # unidades con store (FUList5) = cacheStorePorts
    "bp":  [10, 8],               # piloto: TAGE_SC_L_64KB fue el mejor (-22% fallos, +1.8% IPC)
    "l1d": ["32kB", "64kB"],      # latencia fija en 4 ciclos (supuesto conservador)
}
KEYS = list(GRID.keys())

BASELINE = dict(W=4, iq=16, rob=128, fp=2, st=1, bp=10, l1d="64kB")

PILOT = [
    BASELINE,
    {**BASELINE, "bp": 1},
    {**BASELINE, "bp": 7},
    {**BASELINE, "bp": 8},
    {**BASELINE, "bp": 9},
    dict(W=8, iq=64, rob=256, fp=4, st=2, bp=10, l1d="64kB"),  # maximo
    dict(W=2, iq=16, rob=64,  fp=2, st=1, bp=10, l1d="32kB"),  # minimo
    dict(W=8, iq=16, rob=128, fp=2, st=1, bp=10, l1d="64kB"),  # front-end ancho, back-end chico
    dict(W=4, iq=64, rob=64,  fp=2, st=1, bp=10, l1d="64kB"),  # IQ grande, ROB chico
    dict(W=4, iq=64, rob=256, fp=2, st=1, bp=10, l1d="64kB"),  # interaccion IQ x ROB
    dict(W=4, iq=16, rob=128, fp=4, st=2, bp=10, l1d="64kB"),  # solo FUs
    dict(W=4, iq=32, rob=128, fp=2, st=1, bp=10, l1d="64kB"),  # IQ=32 (comparar con OFAT)
]

BP_NAMES = {0: {"BiModeBP"}, 1: {"LTAGE"}, 2: {"LocalBP"}, 7: {"TAGE"},
            8: {"TAGE_SC_L_64KB", "TAGE_SC_L"}, 9: {"TAGE_SC_L_8KB", "TAGE_SC_L"},
            10: {"TournamentBP"}}

# ---------------------------------------------------------------- stats a extraer
STATS = [
    "simSeconds", "simInsts", "hostSeconds", "hostInstRate",
    "system.cpu.numCycles", "system.cpu.ipc", "system.cpu.cpi",
    "system.cpu.dcache.overallMissRate::total",
    "system.cpu.icache.overallMissRate::total",
    "system.cpu.l2cache.overallMissRate::total",
    "system.cpu.branchPred.mispredicted_0::total",
    "system.cpu.branchPred.condIncorrect",
    "system.cpu.branchPred.BTBHitRatio",
    "system.cpu.rename.IQFullEvents",
    "system.cpu.rename.ROBFullEvents",
    "system.cpu.rename.SQFullEvents",
    "system.cpu.rename.LQFullEvents",
    "system.cpu.iew.lsqFullEvents",
    "system.cpu.fuBusy", "system.cpu.fuBusyRate",
    "system.cpu.statFuBusy::MemWrite",
    "system.cpu.statFuBusy::SimdFloatAdd",
    "system.cpu.statFuBusy::IntAlu",
]
STATS_SET = set(STATS)


# ---------------------------------------------------------------- utilidades
def pow2_at_least(x):
    p = 1
    while p < x:
        p *= 2
    return p


def derive(cfg):
    """Traduce una configuracion a argumentos de CortexA76.py."""
    w = cfg["W"]
    # fetchBufferSize esta en BYTES (4 B por instruccion ARM); potencia de 2, <= linea de 64 B
    fb = min(64, max(16, pow2_at_least(4 * w)))
    return {
        "--fetch_width": w, "--decode_width": w,
        "--rename_width": w, "--commit_width": w,
        "--wb_width": max(4, w),
        "--fb_entries": fb,
        "--fq_entries": max(16, 4 * w),
        "--iq_entries": cfg["iq"],
        "--rob_entries": cfg["rob"],
        "--num_fu_FP_SIMD_ALU": cfg["fp"],
        "--num_fu_write": cfg["st"],
        "--branch_predictor_type": cfg["bp"],
        "--l1d_size": cfg["l1d"],
        # LQ/SQ escalan con el ROB (~ROB/2, como el A76 real: ROB 128, LQ 68).
        # Piloto: con ROB=256 y LQ fija en 68 la LQ se llenaba 3.5M veces.
        "--lq_entries": max(68, cfg["rob"] // 2),
        "--sq_entries": max(72, cfg["rob"] // 2),
    }


def run_name(cfg):
    return (f"W{cfg['W']}_iq{cfg['iq']}_rob{cfg['rob']}_fp{cfg['fp']}"
            f"_st{cfg['st']}_bp{cfg['bp']}_l1d{cfg['l1d']}")


NAME_RE = re.compile(r"W(\d+)_iq(\d+)_rob(\d+)_fp(\d+)_st(\d+)_bp(\d+)_l1d(\w+)$")


def parse_name(name):
    m = NAME_RE.match(name)
    if not m:
        return None
    w, iq, rob, fp, st, bp, l1d = m.groups()
    return dict(W=int(w), iq=int(iq), rob=int(rob), fp=int(fp),
                st=int(st), bp=int(bp), l1d=l1d)


def build_cmd(cfg, rundir):
    cmd = [GEM5, "-d", rundir, SCRIPT,
           f"--cmd={BINARY}",
           f"--cwd={rundir}",                       # cwd del proceso simulado
           f"--options=-w out.wav {MP3_IN}"]        # argv identico en todas las corridas
    cmd += [f"{k}={v}" for k, v in derive(cfg).items()]
    return cmd


def size_bytes(s):
    m = re.fullmatch(r"(\d+)\s*([kKmM]?)B", s)
    return int(m.group(1)) * {"": 1, "k": 1024, "m": 1024 ** 2}[m.group(2).lower()]


def is_done(rundir):
    st = os.path.join(rundir, "stats.txt")
    if not os.path.isfile(st):
        return False
    with open(st, errors="replace") as f:
        txt = f.read()
    return ("End Simulation Statistics" in txt
            and re.search(r"^simInsts\s+[1-9]", txt, re.M) is not None)


def verify(cfg, rundir):
    """Compara lo pedido con lo que gem5 instancio (config.ini)."""
    cp = configparser.ConfigParser(interpolation=None, strict=False, delimiters=("=",))
    cp.optionxform = str
    cp.read(os.path.join(rundir, "config.ini"))
    d = derive(cfg)
    checks = [
        ("system.cpu", "fetchWidth", d["--fetch_width"]),
        ("system.cpu", "decodeWidth", d["--decode_width"]),
        ("system.cpu", "renameWidth", d["--rename_width"]),
        ("system.cpu", "commitWidth", d["--commit_width"]),
        ("system.cpu", "wbWidth", d["--wb_width"]),
        ("system.cpu", "fetchBufferSize", d["--fb_entries"]),
        ("system.cpu", "fetchQueueSize", d["--fq_entries"]),
        ("system.cpu", "numIQEntries", d["--iq_entries"]),
        ("system.cpu", "numROBEntries", d["--rob_entries"]),
        ("system.cpu", "cacheStorePorts", d["--num_fu_write"]),
        ("system.cpu", "LQEntries", d["--lq_entries"]),
        ("system.cpu", "SQEntries", d["--sq_entries"]),
        ("system.cpu.fuPool.FUList3", "count", d["--num_fu_FP_SIMD_ALU"]),
        ("system.cpu.fuPool.FUList5", "count", d["--num_fu_write"]),
        ("system.cpu.dcache", "size", size_bytes(cfg["l1d"])),
    ]
    bad = []
    for sec, key, exp in checks:
        got = cp.get(sec, key, fallback=None)
        if got is None or got.strip() != str(exp):
            bad.append(f"{sec}.{key}: esperado {exp}, obtenido {got}")
    bp_type = cp.get("system.cpu.branchPred", "type", fallback=None)
    if bp_type not in BP_NAMES.get(cfg["bp"], set()):
        bad.append(f"branchPred.type: esperado {BP_NAMES.get(cfg['bp'])}, obtenido {bp_type}")
    return bad


def mcpat_done(rundir):
    out = os.path.join(rundir, f"mcpat_{MCPAT_CONV}.out")
    if not os.path.isfile(out):
        return False
    res = mcpat_run.parse_mcpat(out)
    return bool(res) and None not in (res["total_leakage_W"], res["runtime_dynamic_W"])


def run_mcpat(rundir):
    """Ejecuta conversor + McPAT. Devuelve un texto corto para el manifest."""
    try:
        row = mcpat_run.process(rundir, MCPAT_CONV)
    except Exception as e:
        return f"mcpat=ERROR {e!r}"
    if row.get("status", "").endswith("OK"):
        return f"mcpat=OK P={row['power_W']:.4f}W EDP={row['edp']:.4f} ({row['mcpat_s']}s)"
    return f"mcpat={row.get('status', '?')}"


def run_one(cfg, use_mcpat=True):
    name = run_name(cfg)
    rundir = os.path.join(OUT_ROOT, name)
    if is_done(rundir):
        if use_mcpat and not mcpat_done(rundir):
            t0 = time.time()
            note = run_mcpat(rundir)
            return name, "MCPAT_ONLY", time.time() - t0, note
        return name, "SKIP", 0.0, ""
    if os.path.isdir(rundir):            # corrida incompleta previa: empezar limpio
        shutil.rmtree(rundir)
    os.makedirs(rundir)
    cmd = build_cmd(cfg, rundir)
    with open(os.path.join(rundir, "cmd.txt"), "w") as f:
        f.write(" ".join(cmd) + "\n")

    t0 = time.time()
    try:
        with open(os.path.join(rundir, "gem5.log"), "w") as log:
            r = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT,
                               timeout=TIMEOUT_S, cwd=ROOT)
        rc = r.returncode
    except subprocess.TimeoutExpired:
        return name, "TIMEOUT", time.time() - t0, ""
    wall = time.time() - t0

    wav = os.path.join(rundir, "out.wav")
    if os.path.exists(wav):
        os.remove(wav)

    if not is_done(rundir):
        return name, f"FAIL(rc={rc})", wall, "ver gem5.log"
    bad = verify(cfg, rundir)
    note = "; ".join(bad) + (f" rc={rc}" if rc != 0 else "")
    if use_mcpat:
        note = (note + " " + run_mcpat(rundir)).strip()
    return name, ("OK" if not bad else "PARAM_MISMATCH"), wall, note


def parse_stats(path):
    vals = {}
    with open(path, errors="replace") as f:
        for line in f:
            p = line.split()
            if len(p) >= 2 and p[0] in STATS_SET and p[0] not in vals:
                vals[p[0]] = p[1]
    return {k: vals.get(k, "0") for k in STATS}   # stat ausente = nunca se incremento = 0


POWER_FIELDS = ["total_leakage_W", "runtime_dynamic_W", "power_W", "energy", "edp",
                "area_mm2", "peak_power_W"]


def power_columns(rundir, cpi):
    """Potencia de McPAT y formulas de la guia: E = P*CPI, EDP = E*CPI."""
    out = os.path.join(rundir, f"mcpat_{MCPAT_CONV}.out")
    empty = {k: "" for k in POWER_FIELDS}
    if not os.path.isfile(out):
        return empty
    res = mcpat_run.parse_mcpat(out)
    if not res or None in (res["total_leakage_W"], res["runtime_dynamic_W"]):
        return empty
    p = res["total_leakage_W"] + res["runtime_dynamic_W"]
    e = p * float(cpi)
    return {**res, "power_W": p, "energy": e, "edp": e * float(cpi)}


def collect():
    rows = []
    if not os.path.isdir(OUT_ROOT):
        sys.exit(f"No existe {OUT_ROOT}")
    for name in sorted(os.listdir(OUT_ROOT)):
        cfg = parse_name(name)
        rundir = os.path.join(OUT_ROOT, name)
        if cfg is None or not is_done(rundir):
            continue
        row = {"run": name, **cfg}
        row.update(parse_stats(os.path.join(rundir, "stats.txt")))
        row.update(power_columns(rundir, row["system.cpu.cpi"]))
        rows.append(row)
    out = os.path.join(ROOT, "dse_results.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["run"] + KEYS + STATS + POWER_FIELDS)
        w.writeheader()
        w.writerows(rows)
    print(f"{len(rows)} corridas consolidadas en {out}")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="Lanzador DSE gem5 CortexA76 / mp3_dec")
    ap.add_argument("--mode", choices=["pilot", "full"], default="pilot")
    ap.add_argument("--jobs", type=int, default=5, help="simulaciones en paralelo")
    ap.add_argument("--limit", type=int, default=0, help="maximo de corridas (0 = todas)")
    ap.add_argument("--dry-run", action="store_true", help="solo mostrar comandos")
    ap.add_argument("--collect", action="store_true", help="consolidar stats en CSV")
    ap.add_argument("--no-mcpat", action="store_true", help="no ejecutar McPAT tras cada simulacion")
    ap.add_argument("--collect-every", type=int, default=100,
                    help="actualizar dse_results.csv cada N corridas (0 = solo al final)")
    a = ap.parse_args()
    use_mcpat = not a.no_mcpat

    if a.collect:
        collect()
        return

    needed = [GEM5, SCRIPT, BINARY, MP3_IN]
    if use_mcpat:
        needed += [mcpat_run.MCPAT, mcpat_run.TEMPLATE, mcpat_run.CONVERTERS[MCPAT_CONV]]
    for p in needed:
        if not os.path.exists(p):
            sys.exit(f"No encuentro {p}. Ejecuta el script desde la raiz de gem5.")

    if a.mode == "pilot":
        configs = PILOT
    else:
        configs = [dict(zip(KEYS, v)) for v in itertools.product(*GRID.values())]
        random.Random(0).shuffle(configs)   # orden aleatorio fijo: resultados parciales representativos
    if a.limit:
        configs = configs[:a.limit]

    def needs_work(c):
        rd = os.path.join(OUT_ROOT, run_name(c))
        return not is_done(rd) or (use_mcpat and not mcpat_done(rd))

    pending = [c for c in configs if needs_work(c)]
    n_sim = sum(1 for c in pending if not is_done(os.path.join(OUT_ROOT, run_name(c))))
    est_h = n_sim * (EST_RUN_S + (25 if use_mcpat else 0)) / max(1, a.jobs) / 3600
    print(f"Modo {a.mode}: {len(configs)} configs, {n_sim} simulaciones pendientes, "
          f"{len(pending) - n_sim} solo McPAT, McPAT={'si' if use_mcpat else 'no'}, "
          f"{a.jobs} en paralelo -> ~{est_h:.1f} h")

    if a.dry_run:
        for c in pending[:3]:
            print(" ".join(build_cmd(c, os.path.join(OUT_ROOT, run_name(c)))))
        return

    os.makedirs(OUT_ROOT, exist_ok=True)
    t_start = time.time()
    counts = {}
    manifest = os.path.join(OUT_ROOT, "manifest.csv")
    new_file = not os.path.exists(manifest)
    with open(manifest, "a", newline="") as mf:
        mw = csv.writer(mf)
        if new_file:
            mw.writerow(["timestamp", "run", "status", "wall_s", "note"])
        done = 0
        with ThreadPoolExecutor(max_workers=a.jobs) as ex:
            futs = {ex.submit(run_one, c, use_mcpat): c for c in pending}
            for fut in as_completed(futs):
                done += 1
                try:
                    name, status, wall, note = fut.result()
                except Exception as e:                      # nunca abortar todo el DSE
                    name, status, wall, note = run_name(futs[fut]), "ERROR", 0.0, repr(e)
                mw.writerow([datetime.now().isoformat(timespec="seconds"),
                             name, status, f"{wall:.0f}", note])
                mf.flush()
                print(f"[{done}/{len(pending)}] {status:15s} {name} {wall:6.0f}s {note}",
                      flush=True)
                key = status if "mcpat=OK" in note or a.no_mcpat or status == "SKIP" else status + "+mcpat_fallo"
                counts[key] = counts.get(key, 0) + 1
                if a.collect_every and done % a.collect_every == 0:
                    try:
                        collect()                       # resultados parciales
                    except Exception as e:
                        print(f"collect parcial fallo: {e!r}", flush=True)

    # ---- Al terminar: CSV final y resumen, sin intervencion ----
    collect()
    horas = (time.time() - t_start) / 3600
    resumen = os.path.join(ROOT, "DSE_TERMINADO.txt")
    with open(resumen, "w") as f:
        f.write(f"DSE ({a.mode}) terminado: {datetime.now().isoformat(timespec='seconds')}\n")
        f.write(f"Duracion: {horas:.1f} h con {a.jobs} procesos en paralelo\n")
        f.write(f"Corridas procesadas en esta ejecucion: {done}\n")
        for k, v in sorted(counts.items()):
            f.write(f"  {k}: {v}\n")
        f.write("Resultados: dse_results.csv | Detalle por corrida: dse_runs/manifest.csv\n")
        f.write("Si hay fallos, relanza el mismo comando: solo rehace lo pendiente.\n")
    print(open(resumen).read(), flush=True)


if __name__ == "__main__":
    main()
