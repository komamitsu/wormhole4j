#!/usr/bin/env python3
"""
Generate multi-thread JMH benchmark line charts showing throughput scalability.

Takes multiple JMH result files (one per thread count) and produces line charts
with thread count on the x-axis, one chart per (scenario × key type) combination.

Each chart shows one line per (implementation × operation) pair, e.g.:
  - Wormhole - Put
  - Wormhole - Get
  - Concurrent Skip List Map - Put
  - Concurrent Skip List Map - Get

Usage:
    python generate_jmh_mt_charts.py \\
        --input 1 path/to/1-1.txt \\
        --input 2 path/to/2-2.txt \\
        --input 4 path/to/4-4.txt \\
        --input 8 path/to/8-8.txt \\
        --java-version Java21 \\
        --out-dir path/to/output \\
        [--error-bands]

Options:
    --input N FILE     Thread count N and corresponding JMH result file.
                       Repeat for each thread count.
    --java-version     Label used in chart titles and output filenames.
    --out-dir          Directory to write output PNG files (default: current dir).
    --error-bands      Shade ±error around each line (disabled by default).

Output filenames:
    bench-<java_version>-mt-<scenario>-<keytype>.png
    e.g. bench-java21-mt-putandget-intkey.png
"""

import argparse
import re
import os
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


# ---------------------------------------------------------------------------
# 1. Helpers  (reused from generate_jmh_charts.py)
# ---------------------------------------------------------------------------

def camel_to_display(name: str) -> str:
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", name)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    return s


def op_label(raw: str) -> str:
    m = re.search(r"[Bb]enchmark([A-Z]\w*)$", raw)
    return m.group(1) if m else raw.capitalize()


def _impl_from_class(class_name: str) -> str:
    bare = re.sub(r"For\w+Key$", "", class_name)
    return camel_to_display(bare)


def _key_type_from_class(class_name: str) -> str | None:
    m = re.search(r"For(\w+Key)$", class_name)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# 2. Parsing
# ---------------------------------------------------------------------------

# ConcurrentSkipListMapMultiThreadBenchmark.ForIntKey.PutAndGet:putAndGetBenchmarkGet
# BenchmarkClass.ForXxxKey.Scenario:operationBenchmarkOp  thrpt  N  score ± error
_MT_RE = re.compile(
    r"^(\w+Benchmark)\.(\w+Key)\.(\w+):(\w+Benchmark\w+)"
    r"\s+thrpt\s+\d+\s+([\d.]+)\s+[±]\s+([\d.]+)",
    re.IGNORECASE,
)

# Map benchmark class name prefix → short implementation label
_IMPL_MAP = {
    "ConcurrentSkipListMapMultiThreadBenchmark": "Concurrent Skip List Map",
    "ConcurrentWormholeMultiThreadBenchmark":    "Concurrent Wormhole",
}


def parse_mt_file(path: str) -> dict:
    """
    Parse one multi-thread JMH result file.

    Returns:
        data[scenario][op][impl][key_type] = (score, error)
    """
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    with open(path) as fh:
        for line in fh:
            m = _MT_RE.match(line.strip())
            if not m:
                continue
            bench_class, key_type, scenario, raw_op, score_s, error_s = m.groups()
            impl = _IMPL_MAP.get(bench_class, camel_to_display(
                re.sub(r"MultiThreadBenchmark$", "", bench_class)
            ))
            op = op_label(raw_op)
            data[scenario][op][impl][key_type] = (float(score_s), float(error_s))
    return data


# ---------------------------------------------------------------------------
# 3. Plotting
# ---------------------------------------------------------------------------

# Line style per operation so Put and Get/Scan are visually distinct
# even without colour.
_OP_STYLE = {
    "Put":  {"linestyle": "-",  "marker": "o"},
    "Get":  {"linestyle": "--", "marker": "s"},
    "Scan": {"linestyle": ":",  "marker": "^"},
}
_DEFAULT_STYLE = {"linestyle": "-.", "marker": "D"}

# Colour per implementation
_IMPL_COLOUR = {
    "Concurrent Wormhole":        "#1f77b4",  # blue
    "Concurrent Skip List Map":   "#ff7f0e",  # orange
}
_FALLBACK_COLOURS = [
    "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
]

_PREFERRED_IMPL_ORDER = [
    "Concurrent Skip List Map",
    "Concurrent Wormhole",
]


def _impl_order(impls):
    preferred = [i for i in _PREFERRED_IMPL_ORDER if i in impls]
    rest      = sorted(i for i in impls if i not in _PREFERRED_IMPL_ORDER)
    return preferred + rest


def plot_scaling_chart(
    thread_counts,          # list of ints, e.g. [1, 2, 4, 8]
    scenario,               # str, e.g. "PutAndGet"
    key_type,               # str, e.g. "IntKey"
    # series_data[impl][op] = list of (score, error) aligned with thread_counts
    series_data,
    out_path,
    title,
    show_error_bands=False,
):
    """Draw one line chart (scenario × key_type) and save it."""
    fig, ax = plt.subplots(figsize=(8, 6))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    impls = _impl_order(series_data.keys())
    fallback_idx = 0

    for impl in impls:
        colour = _IMPL_COLOUR.get(impl)
        if colour is None:
            colour = _FALLBACK_COLOURS[fallback_idx % len(_FALLBACK_COLOURS)]
            fallback_idx += 1

        for op, points in sorted(series_data[impl].items()):
            scores = [p[0] / 1e6 for p in points]
            errors = [p[1] / 1e6 for p in points]
            style  = _OP_STYLE.get(op, _DEFAULT_STYLE)

            ax.plot(
                thread_counts, scores,
                label=f"{impl} — {op}",
                color=colour,
                linewidth=2,
                **style,
                markersize=6,
            )
            if show_error_bands:
                lo = [s - e for s, e in zip(scores, errors)]
                hi = [s + e for s, e in zip(scores, errors)]
                ax.fill_between(thread_counts, lo, hi, color=colour, alpha=0.12)

    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle="--", color="#cccccc", linewidth=0.8)
    ax.xaxis.grid(True, linestyle="--", color="#eeeeee", linewidth=0.6)

    ax.set_title(title, fontsize=13, fontweight="normal", pad=12)
    ax.set_xlabel("Number of Threads (PUT + GET/SCAN)", fontsize=11)
    ax.set_ylabel("Throughput (Millions of ops/s)", fontsize=11)

    # X-axis: show only the actual thread counts
    ax.set_xticks(thread_counts)
    ax.set_xticklabels([f"{n}+{n}" for n in thread_counts], fontsize=10)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:.1f}"))

    ax.legend(
        title="Implementation — Operation",
        title_fontsize=9,
        fontsize=9,
        loc="upper left",
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
        description="Generate multi-thread JMH scalability line charts.",
    )
    parser.add_argument(
        "--input",
        nargs=2,
        metavar=("THREAD_COUNT", "FILE"),
        action="append",
        required=True,
        help="Thread count N and its JMH result file. Repeat for each count.",
    )
    parser.add_argument(
        "--java-version",
        default="Java21",
        help="Label used in chart titles and filenames (e.g. Java21).",
    )
    parser.add_argument(
        "--out-dir",
        default=".",
        help="Directory for output PNG files (default: current directory).",
    )
    parser.add_argument(
        "--error-bands",
        action="store_true",
        default=False,
        help="Shade ±error bands around each line (disabled by default).",
    )
    args = parser.parse_args()

    java_version = args.java_version
    out_dir      = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    # Sort inputs by thread count
    inputs = sorted((int(n), path) for n, path in args.input)
    thread_counts = [n for n, _ in inputs]

    print(f"Java version:  {java_version}")
    print(f"Thread counts: {thread_counts}")
    print(f"Error bands:   {'enabled' if args.error_bands else 'disabled'}")
    print(f"Output dir:    {out_dir}")

    # Parse all files: parsed[thread_count] = data dict
    parsed = {}
    for n, path in inputs:
        print(f"Parsing ({n}+{n}): {path}")
        parsed[n] = parse_mt_file(path)

    # Collect all (scenario, key_type) combinations across all files
    combos = set()
    for n in thread_counts:
        for scenario, ops in parsed[n].items():
            for op, impls in ops.items():
                for impl, kts in impls.items():
                    for kt in kts:
                        combos.add((scenario, kt))

    # Build series and plot one chart per (scenario, key_type)
    for scenario, key_type in sorted(combos):
        # series_data[impl][op] = list of (score, error) per thread count
        series_data = defaultdict(lambda: defaultdict(list))

        for n in thread_counts:
            ops = parsed[n].get(scenario, {})
            # Collect all impls and ops present at any thread count
            all_impls = {impl for op_data in ops.values() for impl in op_data}
            all_ops   = set(ops.keys())
            for op in all_ops:
                for impl in all_impls:
                    val = ops.get(op, {}).get(impl, {}).get(key_type)
                    series_data[impl][op].append(val if val else (0.0, 0.0))

        # e.g. bench-java21-mt-putandget-intkey.png
        fname = (
            f"bench-{java_version.lower()}-mt"
            f"-{scenario.lower()}"
            f"-{key_type.lower()}.png"
        )
        out_path = os.path.join(out_dir, fname)
        title = (
            f"Scalability on {java_version}: {scenario} — {key_type}"
        )

        print(f"Plotting {scenario} / {key_type} ...")
        plot_scaling_chart(
            thread_counts=thread_counts,
            scenario=scenario,
            key_type=key_type,
            series_data=series_data,
            out_path=out_path,
            title=title,
            show_error_bands=args.error_bands,
        )

    print("Done.")


if __name__ == "__main__":
    main()
