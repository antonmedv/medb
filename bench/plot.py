#!/usr/bin/env python3
"""Render bench results (results.json) as a four-panel comparison chart."""

import json
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

MEDB, PLAIN, FSYNC = "medb", "map+json", "map+json+fsync"

THEMES = {
    "light": {
        "surface": "#fcfcfb",
        "primary": "#0b0b0b",
        "secondary": "#52514e",
        "muted": "#8a8983",
        "grid": "#e6e5e1",
        "series": {MEDB: "#2a78d6", PLAIN: "#eb6834", FSYNC: "#1baf7a"},
    },
    "dark": {
        "surface": "#1a1a19",
        "primary": "#ffffff",
        "secondary": "#c3c2b7",
        "muted": "#8a8983",
        "grid": "#2f2f2c",
        "series": {MEDB: "#3987e5", PLAIN: "#d95926", FSYNC: "#199e70"},
    },
}

LABEL = {MEDB: "medb", PLAIN: "map+json", FSYNC: "map+json +fsync"}


def counts(v, _=None):
    for unit, step in (("M", 1e6), ("k", 1e3)):
        if v >= step:
            n = v / step
            return f"{n:.0f}{unit}" if n >= 10 or n == int(n) else f"{n:.1f}{unit}"
    return f"{v:.0f}"


def sizes_of_bytes(v, _=None):
    for unit, step in (("MB", 1e6), ("KB", 1e3)):
        if v >= step:
            n = v / step
            return f"{n:.0f} {unit}" if n >= 10 or n == int(n) else f"{n:.1f} {unit}"
    return f"{v:.0f} B"


def crossover(xs, a, b):
    """Size where series a drops below series b, interpolated in log-log space."""
    from math import log10

    for i in range(1, len(xs)):
        if a[i - 1] > b[i - 1] and a[i] <= b[i]:
            x0, x1 = log10(xs[i - 1]), log10(xs[i])
            d0 = log10(a[i - 1]) - log10(b[i - 1])
            d1 = log10(a[i]) - log10(b[i])
            return 10 ** (x0 + (x1 - x0) * d0 / (d0 - d1))
    return None


def endpoint_labels(ax, t, series):
    """Direct-label each line past its last point, nudged apart in log space."""
    from math import log10

    lo, hi = ax.get_ylim()
    span = log10(hi) - log10(lo)
    items = sorted(
        ((log10(ys[-1]), xs[-1], key) for key, xs, ys in series if ys[-1] > 0),
        reverse=True,
    )
    placed = []
    for y, x, key in items:
        if placed and placed[-1][0] - y < 0.075 * span:
            y = placed[-1][0] - 0.075 * span
        placed.append((y, x, key))
    for y, x, key in placed:
        ax.text(
            x * 1.18,
            10**y,
            LABEL[key],
            color=t["series"][key],
            fontsize=8.5,
            va="center",
            ha="left",
        )


def panel(ax, t, title, unit, xs, series, fmt, note=None):
    ax.set_title(title, color=t["primary"], fontsize=11, loc="left", pad=12)
    ax.text(
        0,
        1.015,
        unit,
        transform=ax.transAxes,
        color=t["secondary"],
        fontsize=8.5,
        va="bottom",
    )
    for key, x, y in series:
        ax.plot(
            x,
            y,
            color=t["series"][key],
            linewidth=1.8,
            marker="o",
            markersize=5.5,
            markeredgecolor=t["surface"],
            markeredgewidth=1.4,
            solid_capstyle="round",
            zorder=3 if key == MEDB else 2,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(min(xs) / 1.9, max(xs) * 5.5)
    ax.set_xticks(xs)
    ax.xaxis.set_major_formatter(FuncFormatter(counts))
    ax.xaxis.set_minor_formatter(lambda *_: "")
    ax.yaxis.set_major_formatter(FuncFormatter(fmt))
    ax.grid(True, which="major", color=t["grid"], linewidth=0.6)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(t["grid"])
        ax.spines[side].set_linewidth(0.8)
    ax.tick_params(colors=t["secondary"], labelsize=8.5, length=0, which="both")
    endpoint_labels(ax, t, series)
    if note:
        x, text = note
        ax.axvline(x, color=t["muted"], linewidth=0.8, zorder=1)
        ax.annotate(
            text,
            xy=(x, ax.get_ylim()[1]),
            xytext=(-4, -2),
            textcoords="offset points",
            color=t["secondary"],
            fontsize=8,
            ha="right",
            va="top",
        )


def render(rows, theme, out):
    t = THEMES[theme]
    by = {(r["store"], r["size"]): r for r in rows}
    xs = sorted({r["size"] for r in rows})
    stores = [MEDB, PLAIN, FSYNC]

    def s(field, keys=stores):
        return [(k, xs, [by[(k, n)][field] for n in xs]) for k in keys]

    seq = {k: [by[(k, n)]["writes_per_sec"] for n in xs] for k in stores}
    par = {k: [by[(k, n)]["par_writes_per_sec"] for n in xs] for k in stores}
    writers = by[(MEDB, xs[0])]["writers"]

    plt.rcParams["font.family"] = [
        "Helvetica Neue",
        "Helvetica",
        "Arial",
        "DejaVu Sans",
    ]
    fig, axes = plt.subplots(2, 2, figsize=(11.5, 8.6), dpi=110)
    fig.patch.set_facecolor(t["surface"])
    for ax in axes.flat:
        ax.set_facecolor(t["surface"])

    x1 = crossover(xs, seq[PLAIN], seq[MEDB])
    x2 = crossover(xs, par[PLAIN], par[MEDB])
    panel(
        axes[0][0],
        t,
        "Writes per second — one writer",
        "higher is better · log scale",
        xs,
        s("writes_per_sec"),
        counts,
        (x1, f"medb ahead past ~{counts(x1)} docs  ") if x1 else None,
    )
    panel(
        axes[0][1],
        t,
        f"Writes per second — {writers} concurrent writers",
        "higher is better · log scale",
        xs,
        s("par_writes_per_sec"),
        counts,
        (x2, f"medb ahead past ~{counts(x2)} docs  ") if x2 else None,
    )
    panel(
        axes[1][0],
        t,
        "Bytes written to disk per change",
        "lower is better · log scale · fsync does not change the byte count",
        xs,
        s("bytes_per_write", [MEDB, PLAIN]),
        sizes_of_bytes,
    )
    panel(
        axes[1][1],
        t,
        "Reads per second",
        "higher is better · log scale · both map variants share one line",
        xs,
        s("reads_per_sec", [MEDB, PLAIN]),
        counts,
    )
    for ax in axes[1]:
        ax.set_xlabel("documents in collection", color=t["secondary"], fontsize=8.5)

    fig.suptitle(
        "MeDB vs. a map rewritten to a JSON file on every change",
        color=t["primary"],
        fontsize=15,
        x=0.055,
        y=0.975,
        ha="left",
    )
    fig.text(
        0.055,
        0.938,
        "steady-state writes that replace an existing document · "
        "map+json does not fsync, so its writes are not durable · "
        "medb and map+json+fsync are durable",
        color=t["secondary"],
        fontsize=9,
        ha="left",
    )
    handles = [
        plt.Line2D([], [], color=t["series"][k], linewidth=2.2, label=LABEL[k])
        for k in stores
    ]
    leg = fig.legend(
        handles=handles,
        loc="upper right",
        bbox_to_anchor=(0.985, 0.985),
        ncols=3,
        frameon=False,
        fontsize=9,
        handlelength=1.4,
        columnspacing=1.6,
    )
    for text in leg.get_texts():
        text.set_color(t["secondary"])

    fig.subplots_adjust(
        left=0.055, right=0.948, top=0.855, bottom=0.075, hspace=0.36, wspace=0.24
    )
    for ext in ("svg", "png"):
        fig.savefig(f"{out}.{ext}", facecolor=t["surface"])
    plt.close(fig)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "results.json"
    rows = json.load(open(path))
    render(rows, "light", "bench")
    render(rows, "dark", "bench-dark")
    print("wrote bench.svg bench.png bench-dark.svg bench-dark.png")


if __name__ == "__main__":
    main()
