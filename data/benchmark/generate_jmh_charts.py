#!/usr/bin/env python3
"""
Generate JMH benchmark chart images from a JMH result text file.
Supports both single-thread and multi-thread JMH result formats.

Single-thread format:
    AVLTreeForIntKey.benchmarkGet  thrpt ...
    → one chart per operation (Get / Put / Scan)
      x-axis: key type,  bars: implementation

Multi-thread format:
    ConcurrentWormholeForIntKey.PutAndGet:putAndGetBenchmarkGet  thrpt ...
    → one chart per workload+operation pair (e.g. PutAndGet/Get, PutAndScan/Put)
      x-axis: key type,  bars: implementation
    Group-level aggregate lines (no colon) are skipped.

Usage:
    python generate_jmh_charts.py <jmh_result.txt> <java_version> [--error-bars]

Options:
    java_version   Label used in chart titles and output filenames (e.g. Java8, Java21)
    --error-bars   Show error bars on each bar (disabled by default)

Output (single-thread):
    bench-<java_version>-get.png  etc.

Output (multi-thread):
    bench-<java_version>-putandget-get.png  etc.

Files are written to the same directory as the input file.
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


def _impl_from_class(class_name: str) -> str:
    """'ConcurrentWormholeForIntKey' -> 'Concurrent Wormhole'"""
    bare = re.sub(r"For\w+Key$", "", class_name)
    return camel_to_display(bare)


def _key_type_from_class(class_name: str) -> str | None:
    """'ConcurrentWormholeForIntKey' -> 'IntKey'"""
    m = re.search(r"For(\w+Key)$", class_name)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# 2. Parsing
# ---------------------------------------------------------------------------

# Single-thread: AVLTreeForIntKey.benchmarkGet
_SINGLE_THREAD_RE = re.compile(
    r"^(\w+For\w+Key)\.(benchmark\w+)"
    r"\s+thrpt\s+\d+\s+([\d.]+)\s+[±]\s+([\d.]+)",
)

# Multi-thread: ConcurrentWormholeForIntKey.PutAndGet:putAndGetBenchmarkGet
# The colon is required — lines without it are group aggregates and are skipped.
_MULTI_THREAD_RE = re.compile(
    r"^(\w+For\w+Key)\.(\w+):(\w+Benchmark\w+)"
    r"\s+thrpt\s+\d+\s+([\d.]+)\s+[±]\s+([\d.]+)",
    re.IGNORECASE,
)


def parse_jmh(path: str):
    """
    Parse a JMH throughput result file (single-thread or multi-thread).

    Returns a nested dict:

      Single-thread:
        data[workload=None][operation][impl][key_type] = (score, error)

      Multi-thread:
        data[workload][operation][impl][key_type] = (score, error)

    workload is the JMH @Group name (e.g. "PutAndGet"), or None for
    single-thread benchmarks that have no group structure.
    """
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    with open(path) as fh:
        for line in fh:
            line = line.strip()

            # Try multi-thread first (more specific pattern)
            m = _MULTI_THREAD_RE.match(line)
            if m:
                class_name, workload, raw_op, score_s, error_s = m.groups()
                key_type = _key_type_from_class(class_name)
                if key_type is None:
                    continue
                impl = _impl_from_class(class_name)
                op   = op_label(raw_op)
                data[workload][op][impl][key_type] = (float(score_s), float(error_s))
                continue

            m = _SINGLE_THREAD_RE.match(line)
            if m:
                class_name, raw_op, score_s, error_s = m.groups()
                key_type = _key_type_from_class(class_name)
                if key_type is None:
                    continue
                impl = _impl_from_class(class_name)
                op   = op_label(raw_op)
                data[None][op][impl][key_type] = (float(score_s), float(error_s))

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
    "Red Black Tree",
    "Wormhole",
    "Thread Safe Wormhole",
    "Concurrent Skip List Map",
    "Concurrent Wormhole",
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

    for workload, ops in data.items():
        for op, op_data in ops.items():
            if workload is None:
                # Single-thread: bench-java8-get.png
                fname = f"bench-{java_version.lower()}-{op.lower()}.png"
                title = f"Performance Comparison on {java_version} for {op} Operation"
            else:
                # Multi-thread: bench-java21-putandget-get.png
                fname = f"bench-{java_version.lower()}-{workload.lower()}-{op.lower()}.png"
                title = (f"Performance Comparison on {java_version} "
                         f"for {op} Operation (Workload: {workload})")

            out_path = os.path.join(out_dir, fname)
            print(f"Plotting {op} ...")
            plot_operation(op, op_data, out_path, title, args.error_bars)

    print("Done.")


if __name__ == "__main__":
    main()
