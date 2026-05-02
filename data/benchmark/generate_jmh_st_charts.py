#!/usr/bin/env python3
"""
Generate JMH benchmark bar charts from a single-thread JMH result text file.

Expected format:
    AVLTreeBenchmark.ForIntKey.benchmarkGet  thrpt ...

One chart is produced per operation (Get / Put / Scan):
  x-axis: key type (IntKey, LongKey, StringKey)
  bars:   implementation

Usage:
    python generate_jmh_st_charts.py <jmh_result.txt> <java_version> [--out-dir DIR] [--error-bars]

Options:
    java_version     Label used in chart titles and output filenames (e.g. Java21)
    --out-dir DIR    Directory to write output PNG files (default: same dir as input file)
    --error-bars     Show error bars on each bar (disabled by default)

Output filenames:
    bench-<java_version>-get.png
    bench-<java_version>-put.png
    bench-<java_version>-scan.png
"""

import argparse
import re
import os
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


# ---------------------------------------------------------------------------
# 1. Helpers
# ---------------------------------------------------------------------------

def camel_to_display(name: str) -> str:
    """Insert spaces at CamelCase boundaries.

    Handles both:
      - lowercase→Uppercase: "RedBlack" -> "Red Black"
      - ACRONYM→Word:        "AVLTree"  -> "AVL Tree"
    """
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", name)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    return s


def op_label(raw: str) -> str:
    """Extract a short operation label from a method name.

    Examples:
        "benchmarkGet"          -> "Get"
        "putAndGetBenchmarkGet" -> "Get"
    """
    m = re.search(r"[Bb]enchmark([A-Z]\w*)$", raw)
    if m:
        return m.group(1)
    return raw.capitalize()



# ---------------------------------------------------------------------------
# 2. Parsing
# ---------------------------------------------------------------------------

# AVLTreeBenchmark.ForIntKey.benchmarkGet
_SINGLE_THREAD_RE = re.compile(
    r"^(\w+Benchmark)\.(\w+Key)\.(benchmark\w+)"
    r"\s+thrpt\s+\d+\s+([\d.]+)\s+[±]\s+([\d.]+)",
)

# Map benchmark class name → short implementation label
_IMPL_MAP = {
    "AVLTreeBenchmark":       "AVL Tree",
    "RedBlackTreeBenchmark":  "Red-Black Tree",
    "WormholeBenchmark":      "Wormhole",
}


def parse_jmh(path: str):
    """
    Parse a single-thread JMH throughput result file.

    Returns:
        data[operation][impl][key_type] = (score, error)
    """
    data = defaultdict(lambda: defaultdict(dict))

    with open(path) as fh:
        for line in fh:
            m = _SINGLE_THREAD_RE.match(line.strip())
            if not m:
                continue
            bench_class, key_type, raw_op, score_s, error_s = m.groups()
            impl = _IMPL_MAP.get(bench_class, camel_to_display(
                re.sub(r"Benchmark$", "", bench_class)
            ))
            op = op_label(raw_op)
            data[op][impl][key_type] = (float(score_s), float(error_s))

    return data




# ---------------------------------------------------------------------------
# 3. Plotting
# ---------------------------------------------------------------------------

_PALETTE = [
    "#1f77b4",  # blue
    "#ff7f0e",  # orange
    "#2ca02c",  # green
    "#d62728",  # red
    "#9467bd",  # purple
    "#8c564b",  # brown
    "#e377c2",  # pink
    "#7f7f7f",  # grey
]

_PREFERRED_ORDER = [
    "AVL Tree",
    "Red-Black Tree",
    "Wormhole",
]


def _impl_order(impls):
    preferred = [i for i in _PREFERRED_ORDER if i in impls]
    rest      = sorted(i for i in impls if i not in _PREFERRED_ORDER)
    return preferred + rest


def plot_operation(op, op_data, out_path, title, show_error_bars=False):
    """Draw one grouped-bar chart and save it."""
    impls     = _impl_order(op_data.keys())
    key_types = sorted(
        {kt for impl_data in op_data.values() for kt in impl_data},
        key=lambda s: s.lower(),
    )

    colours = {impl: _PALETTE[i % len(_PALETTE)] for i, impl in enumerate(impls)}

    n_bars  = len(impls)
    bar_w   = 0.18
    group_w = n_bars * bar_w
    x       = np.arange(len(key_types))

    fig, ax = plt.subplots(figsize=(8, 6))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    for idx, impl in enumerate(impls):
        offsets = x - group_w / 2 + idx * bar_w + bar_w / 2
        scores  = [op_data[impl].get(kt, (0, 0))[0] / 1e6 for kt in key_types]
        errors  = [op_data[impl].get(kt, (0, 0))[1] / 1e6 for kt in key_types]
        ax.bar(
            offsets, scores,
            width=bar_w,
            label=impl,
            color=colours[impl],
            edgecolor="white",
            linewidth=0.5,
            yerr=errors if show_error_bars else None,
            capsize=3 if show_error_bars else 0,
            error_kw={"elinewidth": 1, "ecolor": "black", "capthick": 1},
        )

    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle="--", color="#cccccc", linewidth=0.8)
    ax.xaxis.grid(False)

    ax.set_title(title, fontsize=13, fontweight="normal", pad=12)
    ax.set_xlabel("Key Type", fontsize=11)
    ax.set_ylabel("Throughput (Millions of ops/s)", fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(key_types, fontsize=10)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:.1f}"))

    ax.legend(
        title="Implementation",
        title_fontsize=9,
        fontsize=9,
        loc="upper right",
        frameon=True,
        framealpha=0.9,
        edgecolor="#cccccc",
    )

    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(axis="both", which="both", length=0)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ---------------------------------------------------------------------------
# 4. Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate JMH single-thread benchmark bar charts.",
    )
    parser.add_argument(
        "jmh_file",
        help="Path to the JMH result text file",
    )
    parser.add_argument(
        "java_version",
        help="Java version label used in titles and filenames (e.g. Java21)",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Directory to write output PNG files (default: same dir as input file)",
    )
    parser.add_argument(
        "--error-bars",
        action="store_true",
        default=False,
        help="Show error bars on each bar (disabled by default)",
    )
    args = parser.parse_args()

    java_version = args.java_version
    out_dir = args.out_dir or os.path.dirname(os.path.abspath(args.jmh_file))
    os.makedirs(out_dir, exist_ok=True)

    print(f"Parsing:      {args.jmh_file}")
    print(f"Java version: {java_version}")
    print(f"Error bars:   {'enabled' if args.error_bars else 'disabled'}")
    print(f"Output dir:   {out_dir}")

    data = parse_jmh(args.jmh_file)

    for op, op_data in data.items():
        fname    = f"bench-{java_version.lower()}-{op.lower()}.png"
        title    = f"Performance Comparison on {java_version} — {op}"
        out_path = os.path.join(out_dir, fname)
        print(f"Plotting {op} ...")
        plot_operation(op, op_data, out_path, title, args.error_bars)

    print("Done.")


if __name__ == "__main__":
    main()
