#!/usr/bin/env python3
"""Render bench results (results.json) as a four-panel comparison chart."""

import argparse
import json
from math import log10

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.offsetbox import AnnotationBbox, DrawingArea, HPacker, TextArea
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter, NullLocator

MEDB, PLAIN, FSYNC = "medb", "map+json", "map+json+fsync"

# GitHub Primer canvas and foreground tokens, so the chart sits flush in the README.
THEMES = {
    "light": {
        "surface": "#ffffff",
        "primary": "#1f2328",
        "secondary": "#59636e",
        "muted": "#818b98",
        "grid": "#d8dee4",
        "grid_minor": "#eff2f5",
        "series": {MEDB: "#2a78d6", PLAIN: "#eb6834", FSYNC: "#1baf7a"},
    },
    "dark": {
        "surface": "#0d1117",
        "primary": "#e6edf3",
        "secondary": "#9198a1",
        "muted": "#6e7681",
        "grid": "#30363d",
        "grid_minor": "#1b2028",
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


def minor_y_labels(ax, fmt):
    """Label the 2 and 5 minor ticks only where a decade-only axis reads too sparse."""
    lo, hi = ax.get_ylim()
    ax.yaxis.set_minor_formatter(
        FuncFormatter(fmt) if log10(hi / lo) < 2 else NullFormatter()
    )


def panel(ax, t, title, xs, series, fmt):
    ax.set_title(title, color=t["primary"], fontsize=11.5, loc="left", pad=10)
    for key, x, y in series:
        hero = key == MEDB
        ax.plot(
            x,
            y,
            color=t["series"][key],
            linewidth=2.4 if hero else 1.8,
            marker="o",
            markersize=6,
            markeredgecolor=t["surface"],
            markeredgewidth=1.8,
            solid_capstyle="round",
            solid_joinstyle="round",
            zorder=4 if hero else 3,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(min(xs) / 2.0, max(xs) * 1.3)
    ax.set_xticks(xs)
    ax.xaxis.set_major_formatter(FuncFormatter(counts))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_major_formatter(FuncFormatter(fmt))
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=(2, 5)))
    minor_y_labels(ax, fmt)
    ax.grid(True, which="major", color=t["grid"], linewidth=0.7)
    ax.grid(True, which="minor", color=t["grid_minor"], linewidth=0.7)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(t["grid"])
        ax.spines[side].set_linewidth(0.8)
    ax.tick_params(which="both", length=0, labelsize=8.5)
    ax.tick_params(which="major", colors=t["secondary"])
    ax.tick_params(which="minor", colors=t["muted"])


def endpoint_labels(fig, ax, t, series):
    """Direct-label each line at its end, each label keyed by its own color rule."""
    ends = [(k, x[-1], y[-1]) for k, x, y in series if y[-1] > 0]
    ranked = sorted(
        ((ax.transData.transform((x, y))[1], k, x, y) for k, x, y in ends), reverse=True
    )
    px = fig.dpi / 72.0
    tops, prev = [], None
    for top, *_ in ranked:
        if prev is not None and prev - top < 15 * px:
            top = prev - 15 * px
        prev = top
        tops.append(top)
    center = sum(top - r[0] for top, r in zip(tops, ranked)) / len(tops)
    boxes = []
    for top, (natural, key, x, y) in zip(tops, ranked):
        rule = DrawingArea(11, 3, 0, 0)
        rule.add_artist(
            Line2D(
                [0, 11],
                [1.5, 1.5],
                color=t["series"][key],
                linewidth=2.2,
                solid_capstyle="round",
            )
        )
        box = AnnotationBbox(
            HPacker(
                children=[
                    rule,
                    TextArea(
                        LABEL[key], textprops=dict(color=t["primary"], fontsize=8.5)
                    ),
                ],
                pad=0,
                sep=4,
                align="center",
            ),
            (x, y),
            xybox=(10, (top - center - natural) / px),
            xycoords="data",
            boxcoords="offset points",
            box_alignment=(0, 0.5),
            frameon=False,
            pad=0,
            annotation_clip=False,
            zorder=6,
        )
        ax.add_artist(box)
        boxes.append(box)
    return boxes


def fit_x_range(fig, panels, labels, xs):
    """One x range for every panel, wide enough for the widest set of end labels."""
    renderer = fig.canvas.get_renderer()
    reach = max(
        box.get_window_extent(renderer).x1 - ax.transData.transform((xs[-1], 1))[0]
        for ax, *_ in panels
        for box in labels[ax]
    )
    width = min(ax.get_window_extent().width for ax, *_ in panels)
    room = width - reach - 6 * fig.dpi / 72.0
    lo = min(xs) / 2.0
    hi = 10 ** (log10(lo) + (log10(xs[-1]) - log10(lo)) * width / room)
    for ax, *_ in panels:
        ax.set_xlim(lo, hi)


def render(rows, theme, out):
    t = THEMES[theme]
    by = {(r["store"], r["size"]): r for r in rows}
    xs = sorted({r["size"] for r in rows})
    stores = [MEDB, PLAIN, FSYNC]

    def s(field, keys=stores):
        return [(k, xs, [by[(k, n)][field] for n in xs]) for k in keys]

    writers = by[(MEDB, xs[0])]["writers"]

    plt.rcParams["font.family"] = [
        "Helvetica Neue",
        "Helvetica",
        "Arial",
        "DejaVu Sans",
    ]
    fig, axes = plt.subplots(2, 2, figsize=(11.2, 8.0), dpi=200)
    fig.patch.set_facecolor(t["surface"])
    for ax in axes.flat:
        ax.set_facecolor(t["surface"])

    panels = [
        (axes[0][0], "Writes per second — one writer", s("writes_per_sec"), counts),
        (
            axes[0][1],
            f"Writes per second — {writers} concurrent writers",
            s("par_writes_per_sec"),
            counts,
        ),
        (
            axes[1][0],
            "Bytes written per change",
            s("bytes_per_write", [MEDB, PLAIN]),
            sizes_of_bytes,
        ),
        (axes[1][1], "Reads per second", s("reads_per_sec", [MEDB, PLAIN]), counts),
    ]

    fig.subplots_adjust(
        left=0.058, right=0.982, top=0.878, bottom=0.115, hspace=0.30, wspace=0.17
    )
    for ax, title, series, fmt in panels:
        panel(ax, t, title, xs, series, fmt)

    writes = [ax for ax, *_ in panels[:2]]
    span = [ax.get_ylim() for ax in writes]
    for ax in writes:
        ax.set_ylim(min(lo for lo, _ in span), max(hi for _, hi in span))
        minor_y_labels(ax, counts)

    fig.canvas.draw()
    fit_x_range(fig, panels, {ax: endpoint_labels(fig, ax, t, series) for ax, _, series, _ in panels}, xs)

    fig.suptitle(
        "MeDB's write cost is flat; rewriting a JSON file grows with the collection",
        color=t["primary"],
        fontsize=16,
        x=0.058,
        y=0.955,
        ha="left",
    )
    fig.text(
        0.52,
        0.042,
        "documents in collection",
        color=t["secondary"],
        fontsize=8.5,
        ha="center",
    )
    for ext in ("svg", "png"):
        fig.savefig(f"{out}.{ext}", facecolor=t["surface"])
    plt.close(fig)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("results", nargs="?", default="results.json")
    args = p.parse_args()
    rows = json.load(open(args.results))
    render(rows, "light", "bench")
    render(rows, "dark", "bench-dark")
    print("wrote bench.svg bench.png bench-dark.svg bench-dark.png")


if __name__ == "__main__":
    main()
