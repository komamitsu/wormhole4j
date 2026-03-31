#!/usr/bin/env python3
"""
Generate JMH benchmark chart images from a JMH result text file.
Produces one chart per operation (Get, Put, Scan) in the style of the reference image.

Usage:
    python generate_jmh_charts.py <jmh_result.txt> <java_version> [--error-bars]

Options:
    java_version   Label used in chart titles and output filenames (e.g. Java8, Java21)
    --error-bars   Show error bars on the bars (disabled by default)

Output:
    bench-<java_version>-get.png
    bench-<java_version>-put.png
    bench-<java_version>-scan.png
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

    All keys are derived solely from the file content — no hardcoded
    operation names, implementation names, or key types.
    """
    # Matches lines like:
    # BenchmarkAVLTreeForIntKey.benchmarkGet  thrpt  4  6957601.694 ± 1727243.632  ops/s
    pattern = re.compile(
        r"^Benchmark(\w+?)For(\w+Key)\.(benchmark\w+)"
        r"\s+thrpt\s+\d+\s+([\d.]+)\s+[±]\s+([\d.]+)",
    )

    # CamelCase → "Title Case" for display (e.g. "AVLTree" -> "AVL Tree")
    def camel_to_display(name: str) -> str:
        # Insert a space before each uppercase run that follows a lowercase letter
        return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)

    # "benchmarkGet" -> "Get"
    def op_label(raw: str) -> str:
        return raw.removeprefix("benchmark").capitalize()

    data = defaultdict(lambda: defaultdict(dict))

    with open(path) as fh:
        for line in fh:
            m = pattern.match(line.strip())
            if not m:
                continue
            raw_impl, key_type, raw_op, score_s, error_s = m.groups()
            impl = camel_to_display(raw_impl)
            op   = op_label(raw_op)
            data[op][impl][key_type] = (float(score_s), float(error_s))

    return data





# ---------------------------------------------------------------------------
# 2. Plotting
# ---------------------------------------------------------------------------

# Preferred colour cycle — extended automatically for unknown implementations
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

# Known implementations kept in a stable display order; unknown ones are
# appended alphabetically after them.
_PREFERRED_ORDER = [
    "AVL Tree",
    "Red Black Tree",
    "Wormhole",
    "Thread Safe Wormhole",
]


def _impl_order(impls):
    """Return impls sorted: preferred order first, then alphabetical."""
    preferred = [i for i in _PREFERRED_ORDER if i in impls]
    rest      = sorted(i for i in impls if i not in _PREFERRED_ORDER)
    return preferred + rest


def plot_operation(op, op_data, out_path, java_version, show_error_bars=False):
    """Draw one grouped-bar chart for a single operation and save it."""
    impls     = _impl_order(op_data.keys())
    key_types = sorted(                        # sort for a consistent x-axis order
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

    ax.set_title(
        f"Performance Comparison on {java_version} for {op} Operation",
        fontsize=13, fontweight="normal", pad=12,
    )
    ax.set_xlabel("Key Type", fontsize=11)
    ax.set_ylabel("Throughput (Millions of ops/s)", fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(key_types, fontsize=10)
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
        help="Java version label used in titles and filenames (e.g. Java8, Java21)",
    )
    parser.add_argument(
        "--error-bars",
        action="store_true",
        default=False,
        help="Show error bars on each bar (disabled by default)",
    )
    args = parser.parse_args()

    java_version = args.java_version
    out_dir      = os.path.dirname(os.path.abspath(args.jmh_file))

    print(f"Parsing:      {args.jmh_file}")
    print(f"Java version: {java_version}")
    print(f"Error bars:   {'enabled' if args.error_bars else 'disabled'}")
    data = parse_jmh(args.jmh_file)

    for op, op_data in data.items():
        fname    = f"bench-{java_version.lower()}-{op.lower()}.png"
        out_path = os.path.join(out_dir, fname)
        print(f"Plotting {op} ...")
        plot_operation(op, op_data, out_path, java_version, args.error_bars)

    print("Done.")


if __name__ == "__main__":
    main()
