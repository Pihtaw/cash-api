#!/usr/bin/env python3
"""Сравнение Rust и .NET: запускает замеры и строит графики.

    pip install matplotlib
    python3 analyze/analyze.py --run                          # собрать, прогнать оба языка 3 раза, нарисовать
    python3 analyze/analyze.py --run --runs 5 --n 1000000     # побольше
    python3 analyze/analyze.py                                # только перерисовать по готовому results.csv

Rust и .NET запускаются по очереди (rust, dotnet, rust, dotnet, ...): если машина
в какой-то момент была занята, это заденет оба языка поровну.

В папке analyze/ появятся:
    results.csv         сырые данные — строки, которые пишут обе программы
    summary.md          таблица: среднее по запускам
    warmup_memory.png   прогрев кеша и память
    throughput.png      сколько get в секунду при разном числе читателей
    latency.png         задержки p95, p99, p99.9
    gc.png              паузы сборщика мусора .NET (у Rust его нет)
"""

import argparse
import csv
import subprocess
from pathlib import Path
from statistics import mean

import matplotlib

matplotlib.use("Agg")  # рисуем в файлы, окно не открываем
import matplotlib.pyplot as plt  # noqa: E402

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
CSV = HERE / "results.csv"

# Цвет закреплён за языком; линии ещё и разными маркерами — чтобы различать без цвета.
STYLE = {"rust": ("Rust", "#2a78d6", "o"), "dotnet": (".NET", "#eb6834", "s")}
plt.rcParams.update({"axes.grid": True, "axes.axisbelow": True, "grid.color": "#e4e3df",
                     "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False})


# -----------------------------------------------------------------------------
# Запуск замеров
# -----------------------------------------------------------------------------

def sh(cmd, cwd):
    print("$", " ".join(map(str, cmd)), flush=True)
    subprocess.run(list(map(str, cmd)), cwd=cwd, check=True)


def run_benchmarks(runs, n, secs):
    sh(["cargo", "build", "--release", "-q"], ROOT / "rust")
    sh(["dotnet", "build", "-c", "Release", "--nologo", "-v", "q"], ROOT / "dotnet")
    CSV.unlink(missing_ok=True)
    common = ["bench", "--out", CSV, "--n", n, "--secs", secs]
    for r in range(1, runs + 1):
        sh(["cargo", "run", "--release", "-q", "--"] + common + ["--run", r], ROOT / "rust")
        sh(["dotnet", "run", "-c", "Release", "--no-build", "--"] + common + ["--run", r], ROOT / "dotnet")


# -----------------------------------------------------------------------------
# Чтение и усреднение
# -----------------------------------------------------------------------------

def load():
    with open(CSV, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k, v in r.items():
            if k != "impl":
                r[k] = float(v)
    return rows


def avg(rows, impl, col, threads=None):
    """Среднее по запускам; для прогрева берём одну строку на запуск."""
    vals = {}
    for r in rows:
        if r["impl"] == impl and (threads is None or r["threads"] == threads):
            vals[(r["run"], r["threads"] if threads is not None else 0)] = r[col]
    return mean(vals.values()) if vals else float("nan")


def rss_end(rows, impl):
    last = {}
    for r in rows:
        if r["impl"] == impl and (r["run"] not in last or r["threads"] > last[r["run"]]["threads"]):
            last[r["run"]] = r
    return mean(r["rss_mb"] for r in last.values())


# -----------------------------------------------------------------------------
# Графики
# -----------------------------------------------------------------------------

def lines(ax, rows, impls, threads, col, scale=1.0):
    for impl in impls:
        name, color, marker = STYLE[impl]
        ax.plot(range(len(threads)), [avg(rows, impl, col, t) / scale for t in threads],
                color=color, marker=marker, markersize=8, linewidth=2, label=name)
    ax.set_xticks(range(len(threads)), [str(int(t)) for t in threads])
    ax.set_xlabel("читателей (+1 писатель)")
    ax.set_ylim(bottom=0)


def bar_values(ax, labels, values, colors, unit):
    bars = ax.bar(labels, values, color=colors, width=0.6)
    for b, v in zip(bars, values):
        ax.annotate(f"{v:.0f} {unit}", (b.get_x() + b.get_width() / 2, v),
                    xytext=(0, 4), textcoords="offset points", ha="center")
    ax.grid(axis="x", visible=False)
    ax.margins(y=0.2)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", action="store_true", help="сначала прогнать замеры")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--n", type=int, default=1_000_000, help="записей в кеше")
    p.add_argument("--secs", type=float, default=3, help="секунд на каждую ступень")
    a = p.parse_args()
    if a.run:
        run_benchmarks(a.runs, a.n, a.secs)

    rows = load()
    impls = [i for i in STYLE if any(r["impl"] == i for r in rows)]
    threads = sorted({r["threads"] for r in rows})
    names = [STYLE[i][0] for i in impls]
    colors = [STYLE[i][1] for i in impls]

    # Прогрев и память
    fig, ax = plt.subplots(1, 3, figsize=(13, 4))
    bar_values(ax[0], names, [avg(rows, i, "warmup_ms") for i in impls], colors, "мс")
    ax[0].set_title("Прогрев кеша, мс (меньше — лучше)", loc="left")
    bar_values(ax[1], names, [avg(rows, i, "rss_after_warmup_mb") for i in impls], colors, "МБ")
    ax[1].set_title("Память после прогрева, МБ", loc="left")
    bar_values(ax[2], names, [rss_end(rows, i) for i in impls], colors, "МБ")
    ax[2].set_title("Память в конце, МБ", loc="left")
    fig.tight_layout()
    fig.savefig(HERE / "warmup_memory.png", dpi=130)

    # Пропускная способность
    fig, ax = plt.subplots(figsize=(7, 4))
    lines(ax, rows, impls, threads, "mops")
    ax.set_title("Млн get в секунду (больше — лучше)", loc="left")
    ax.legend()
    fig.tight_layout()
    fig.savefig(HERE / "throughput.png", dpi=130)

    # Задержки: три отдельные панели, у каждой своя ось
    fig, ax = plt.subplots(1, 3, figsize=(14, 4))
    for axis, col, title in zip(ax, ["p95_ns", "p99_ns", "p999_ns"], ["p95", "p99", "p99.9"]):
        lines(axis, rows, impls, threads, col, scale=1000)
        axis.set_title(f"Задержка get {title}, мкс", loc="left")
    ax[0].legend()
    fig.tight_layout()
    fig.savefig(HERE / "latency.png", dpi=130)

    # Паузы GC .NET
    if "dotnet" in impls:
        fig, ax = plt.subplots(figsize=(7, 4))
        bar_values(ax, [str(int(t)) for t in threads],
                   [avg(rows, "dotnet", "gc_pause_ms", t) for t in threads], STYLE["dotnet"][1], "мс")
        ax.set_xlabel("читателей (+1 писатель)")
        ax.set_title("Паузы сборщика мусора .NET за ступень, мс", loc="left")
        fig.tight_layout()
        fig.savefig(HERE / "gc.png", dpi=130)

    # Таблица
    runs = len({r["run"] for r in rows})
    md = [f"# Rust vs .NET: кеш справочников\n", f"Среднее по {runs} запускам.\n",
          "| | " + " | ".join(names) + " |", "|---|" + "---|" * len(impls),
          "| Прогрев, мс | " + " | ".join(f"{avg(rows, i, 'warmup_ms'):.0f}" for i in impls) + " |",
          "| Память после прогрева, МБ | " + " | ".join(f"{avg(rows, i, 'rss_after_warmup_mb'):.0f}" for i in impls) + " |",
          "| Память в конце, МБ | " + " | ".join(f"{rss_end(rows, i):.0f}" for i in impls) + " |", "",
          "| Читателей | Язык | млн get/с | p50, нс | p95, нс | p99, нс | p99.9, нс | max, нс | паузы GC, мс |",
          "|---|---|---|---|---|---|---|---|---|"]
    for t in threads:
        for i in impls:
            v = [avg(rows, i, c, t) for c in ["mops", "p50_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns", "gc_pause_ms"]]
            md.append(f"| {int(t)} | {STYLE[i][0]} | {v[0]:.2f} | {v[1]:.0f} | {v[2]:.0f} | {v[3]:.0f} | {v[4]:.0f} | {v[5]:.0f} | {v[6]:.1f} |")
    (HERE / "summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))
    print(f"\nГрафики: {', '.join(sorted(p.name for p in HERE.glob('*.png')))}")


if __name__ == "__main__":
    main()
