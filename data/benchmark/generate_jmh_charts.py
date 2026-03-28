#!/usr/bin/env python3
"""
Generate JMH benchmark chart images from a JMH result text file.
Produces one chart per operation (Get, Put, Scan) in the style of the reference image.

Usage:
    python generate_jmh_charts.py <jmh_result.txt> [java_version] [--error-bars]

Options:
    java_version   Label used in the chart title and output filenames (default: Java8)
    --error-bars   Show error bars on the bars (disabled by default)

Output:
    bench-java8-get.png, bench-java8-put.png, bench-java8-scan.png
    (written to the same directory as the input file)
"""

import argparse
import re
import os
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


# ---------------------------------------------------------------------------
# 1. Parsing
# ---------------------------------------------------------------------------

def parse_jmh(path: str):
    """
    Parse a JMH throughput result file.

    Returns a nested dict:
        data[operation][impl][key_type] = (score, error)

    operation : "Get" | "Put" | "Scan"
    impl      : "AVL Tree" | "Red Black Tree" | "Wormhole" | "ThreadSafe Wormhole"
    key_type  : "IntKey" | "LongKey" | "StringKey"
    """
    # Regex to match lines like:
    # BenchmarkAVLTreeForIntKey.benchmarkGet  thrpt  4  6957601.694 +- 1727243.632  ops/s
    pattern = re.compile(
        r"^Benchmark(\w+?)For(\w+Key)\.(benchmark\w+)"
        r"\s+thrpt\s+\d+\s+([\d.]+)\s+[±]\s+([\d.]+)",
    )

    impl_map = {
        "AVLTree":             "AVL Tree",
        "RedBlackTree":        "Red Black Tree",
        "ThreadSafeWormhole":  "ThreadSafe Wormhole",
        "Wormhole":            "Wormhole",
    }
    op_map = {
        "benchmarkGet":  "Get",
        "benchmarkPut":  "Put",
        "benchmarkScan": "Scan",
    }

    data = defaultdict(lambda: defaultdict(dict))

    with open(path) as fh:
        for line in fh:
            m = pattern.match(line.strip())
            if not m:
                continue
            raw_impl, key_type, raw_op, score_s, error_s = m.groups()
            impl = impl_map.get(raw_impl, raw_impl)
            op   = op_map.get(raw_op, raw_op)
            data[op][impl][key_type] = (float(score_s), float(error_s))

    return data


# ---------------------------------------------------------------------------
# 2. Plotting
# ---------------------------------------------------------------------------

COLOURS = {
    "AVL Tree":            "#1f77b4",
    "Red Black Tree":      "#ff7f0e",
    "Wormhole":            "#2ca02c",
    "ThreadSafe Wormhole": "#d62728",
}

KEY_TYPES  = ["IntKey", "LongKey", "StringKey"]
IMPL_ORDER = ["AVL Tree", "Red Black Tree", "Wormhole", "ThreadSafe Wormhole"]


def plot_operation(op, op_data, out_path, java_version="Java8", show_error_bars=False):
    """Draw one grouped-bar chart for a single operation and save it."""
    impls = [i for i in IMPL_ORDER if i in op_data]

    n_bars  = len(impls)
    bar_w   = 0.18
    group_w = n_bars * bar_w
    x       = np.arange(len(KEY_TYPES))

    fig, ax = plt.subplots(figsize=(8, 6))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    for idx, impl in enumerate(impls):
        offsets = x - group_w / 2 + idx * bar_w + bar_w / 2
        scores  = [op_data[impl].get(kt, (0, 0))[0] / 1e6 for kt in KEY_TYPES]
        errors  = [op_data[impl].get(kt, (0, 0))[1] / 1e6 for kt in KEY_TYPES]
        ax.bar(
            offsets, scores,
            width=bar_w,
            label=impl,
            color=COLOURS.get(impl, "#999999"),
            edgecolor="white",
            linewidth=0.5,
            yerr=errors if show_error_bars else None,
            capsize=3 if show_error_bars else 0,
            error_kw={"elinewidth": 1, "ecolor": "black", "capthick": 1},
        )

    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle="--", color="#cccccc", linewidth=0.8)
    ax.xaxis.grid(False)

    ax.set_title(
        f"Performance Comparison on {java_version} for {op} Operation",
        fontsize=13, fontweight="normal", pad=12,
    )
    ax.set_xlabel("Key Type", fontsize=11)
    ax.set_ylabel("Throughput (Millions of ops/s)", fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(KEY_TYPES, fontsize=10)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:.1f}"))

    ax.legend(
        title="Tree Implementation",
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
# 3. Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate JMH benchmark chart images from a JMH result text file.",
    )
    parser.add_argument(
        "jmh_file",
        help="Path to the JMH result text file",
    )
    parser.add_argument(
        "java_version",
        nargs="?",
        default="Java8",
        help="Java version label used in titles and filenames (default: Java8)",
    )
    parser.add_argument(
        "--error-bars",
        action="store_true",
        default=False,
        help="Show error bars on each bar (disabled by default)",
    )
    args = parser.parse_args()

    out_dir = os.path.dirname(os.path.abspath(args.jmh_file))

    print(f"Parsing: {args.jmh_file}")
    print(f"Error bars: {'enabled' if args.error_bars else 'disabled'}")
    data = parse_jmh(args.jmh_file)

    for op, op_data in data.items():
        fname    = f"bench-{args.java_version.lower()}-{op.lower()}.png"
        out_path = os.path.join(out_dir, fname)
        print(f"Plotting {op} ...")
        plot_operation(op, op_data, out_path, args.java_version, args.error_bars)

    print("Done.")


if __name__ == "__main__":
    main()
