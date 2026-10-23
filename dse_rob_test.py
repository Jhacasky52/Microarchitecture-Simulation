import os
import subprocess

# Valores de ROB a explorar (ej. subiendo, por defecto y bajando capacidad)
rob_values = [64, 128, 256]

# Los 10 parámetros/métricas más importantes para analizar en un DSE fuera de orden
metrics_to_extract = [
    "simSeconds",                               # 1. Tiempo total simulado (segundos)
    "simInsts",                                 # 2. Instrucciones totales simuladas
    "system.cpu.ipc",                           # 3. Rendimiento (IPC: Instrucciones por Ciclo)
    "system.cpu.cpi",                           # 4. Eficiencia (CPI: Ciclos por Instrucción)
    "system.cpu.numCycles",                     # 5. Ciclos totales de reloj consumidos
    "system.cpu.dcache.overallMissRate::total", # 6. Tasa de fallos en caché de datos (L1D)
    "system.cpu.icache.overallMissRate::total", # 7. Tasa de fallos en caché de instrucciones (L1I)
    "system.cpu.branchPred.mispredicted_0::total",# 8. Saltos mal predichos (penalización de control)
    "system.cpu.rename.ROBFullEvents",          # 9. Bloqueos por ROB lleno (¡clave para este parámetro!)
    "system.cpu.iew.iqFullEvents"               # 10. Bloqueos por Cola de Instrucciones llena (IQ)
]

summary_filename = "summary_rob_analysis.csv"

# Inicializar archivo CSV con los encabezados
with open(summary_filename, "w") as summary_file:
    headers = ["ROB_Size"] + metrics_to_extract
    summary_file.write(",".join(headers) + "\n")

    for rob in rob_values:
        outdir = f"m5out_rob_{rob}"
        print(f"\n=== Ejecutando simulación con ROB entries: {rob} (Carpeta: {outdir}) ===")
        
        # Comando para gem5 usando el script de Cortex-A76 y la carga de trabajo de audio
        cmd = [
            "./build/ARM/gem5.fast",
            f"--outdir={outdir}",
            "scripts/CortexA76_scripts_gem5/CortexA76.py",
            "-c", "workloads/mp3_dec/mp3_dec",
            "-o", "-w workloads/mp3_dec/mp3dec_outfile.wav workloads/mp3_dec/mp3dec_testfile.mp3",
            f"--rob_entries={rob}"
        ]
        
        try:
            # Ejecutar la simulación
            subprocess.run(cmd, check=True)
            print(f"-> Simulación con ROB={rob} finalizada con éxito.")
            
            stats_path = os.path.join(outdir, "stats.txt")
            extracted_values = {m: "N/A" for m in metrics_to_extract}
            
            # Leer y extraer las métricas del stats.txt generado
            if os.path.exists(stats_path):
                with open(stats_path, "r") as stats_file:
                    for line in stats_file:
                        parts = line.strip().split()
                        if len(parts) >= 2:
                            metric_name = parts[0]
                            metric_value = parts[1]
                            if metric_name in extracted_values:
                                extracted_values[metric_name] = metric_value
            else:
                print(f"Advertencia: No se encontró el archivo {stats_path}")
            
            # Escribir la fila correspondiente en el CSV
            row_data = [str(rob)] + [extracted_values[m] for m in metrics_to_extract]
            summary_file.write(",".join(row_data) + "\n")
            print(f"-> Métricas extraídas para ROB={rob} guardadas correctamente.")
            
        except subprocess.CalledProcessError as e:
            print(f"Error al ejecutar la simulación con ROB={rob}: {e}")
            break

print(f"\n¡Proceso de barrido del ROB completado!")
print(f"Los resultados detallados se han consolidado en el archivo: '{summary_filename}'")
