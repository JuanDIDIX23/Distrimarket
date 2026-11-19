"""Genera las gráficas de evidencias a partir de los JSON de resultados."""
import json
import pathlib

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

EV = pathlib.Path(__file__).resolve().parents[1] / "docs" / "evidencias"
S1, S2 = "#2a78d6", "#eb6834"          # paleta categórica validada (slots 1 y 2)
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e6e5e0", "#fcfcfb"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.edgecolor": GRID,
                     "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
                     "axes.facecolor": SURF, "figure.facecolor": SURF, "text.color": INK})


def clean(ax):
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)


perf = json.loads((EV / "rendimiento.json").read_text())["resultados"]
series = {"GET /api/products": [r for r in perf if r["escenario"].startswith("GET")],
          "POST /api/orders": [r for r in perf if r["escenario"].startswith("POST")]}
fig, axes = plt.subplots(1, 2, figsize=(11, 4))
for (name, rows), color in zip(series.items(), (S1, S2)):
    x = [r["concurrencia"] for r in rows]
    for ax, key in zip(axes, ("throughput_rps", "p95_ms")):
        ax.plot(x, [r[key] for r in rows], color=color, linewidth=2, marker="o", markersize=7,
                markeredgecolor=SURF, markeredgewidth=2, label=name)
        ax.annotate(name.split()[0], (x[-1], rows[-1][key]), xytext=(8, 0), textcoords="offset points",
                    va="center", color=INK2, fontsize=9)
axes[0].set_title("Throughput (peticiones/s)", loc="left", fontsize=11, color=INK)
axes[1].set_title("Latencia p95 (ms)", loc="left", fontsize=11, color=INK)
for ax in axes:
    clean(ax)
    ax.set_xlabel("Usuarios virtuales concurrentes")
    ax.set_xticks([10, 25, 50, 100])
    ax.set_xlim(0, 115)
    ax.set_ylim(bottom=0)
axes[0].legend(frameon=False, loc="upper right")
fig.suptitle("Rendimiento bajo carga — 100 % de respuestas exitosas en todos los niveles",
             x=0.01, ha="left", fontsize=12, fontweight="bold")
fig.tight_layout()
fig.savefig(EV / "grafica_rendimiento.png", dpi=160)

f3 = next(r for r in json.loads((EV / "tolerancia_fallos.json").read_text()) if r["id"] == "F3")
tl = f3["observado"]["linea_de_tiempo"]
fig, ax = plt.subplots(figsize=(11, 3.8))
xs = [t["intento"] for t in tl]
ax.bar(xs, [t["ms"] for t in tl], color=S1, width=0.6)
for t in tl:
    ax.annotate(f'{t["ms"]:.0f} ms', (t["intento"], t["ms"]), xytext=(0, 4), textcoords="offset points",
                ha="center", fontsize=8.5, color=INK2)
first_open = next(t["intento"] for t in tl if "circuito abierto" in t["detalle"])
ax.axvline(first_open - 0.5, color=INK2, linestyle=(0, (3, 3)), linewidth=1)
top = max(t["ms"] for t in tl)
ax.set_ylim(0, top * 1.35)
ax.text(first_open - 0.35, top * 1.18,
        "circuito ABIERTO → rechazo inmediato (fail-fast)", color=INK, fontsize=9.5)
ax.text(first_open - 0.65, top * 1.18, "intentos reales con reintentos ←", color=INK, fontsize=9.5, ha="right")
clean(ax)
ax.set_xticks(xs)
ax.set_xlabel("Intento de crear pedido con todas las réplicas de inventory caídas (todas responden 503)")
ax.set_ylabel("Tiempo de respuesta (ms)")
ax.set_title("Circuit breaker: de ~157 ms a ~4 ms por solicitud una vez abierto el circuito",
             loc="left", fontsize=12, fontweight="bold")
fig.tight_layout()
fig.savefig(EV / "grafica_circuit_breaker.png", dpi=160)
print("ok")
