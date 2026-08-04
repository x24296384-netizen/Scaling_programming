"""Generate report-ready performance charts.

Raw benchmark JSON files remain under results/. The generated PNG
figures are stored under docs/evidence/charts/.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt


BENCHMARK_DIR = Path(
    "results/speed/benchmarks"
)

OUTPUT_DIR = Path(
    "docs/evidence/charts"
)

REPORT_LOADS = {
    100,
    500,
    1000,
}


def load_speed_runs() -> list[dict[str, Any]]:
    """Load the selected AWS Lambda-S3 benchmark executions."""

    runs: list[dict[str, Any]] = []

    for path in BENCHMARK_DIR.glob(
        "lambda_s3_validation_*.json"
    ):
        document = json.loads(
            path.read_text(
                encoding="utf-8-sig"
            )
        )

        for run in document.get(
            "runs",
            [],
        ):
            requested_records = run.get(
                "requested_records"
            )

            if requested_records in REPORT_LOADS:
                runs.append(run)

    return sorted(
        runs,
        key=lambda run: int(
            run["requested_records"]
        ),
    )


def save_figure(
    filename: str,
    footer: str | None = None,
) -> None:
    """Save one high-resolution report figure."""

    if footer:
        plt.figtext(
            0.5,
            0.015,
            footer,
            ha="center",
            va="bottom",
            fontsize=8.5,
            wrap=True,
        )

        plt.tight_layout(
            rect=(0, 0.075, 1, 1)
        )
    else:
        plt.tight_layout()

    output_path = (
        OUTPUT_DIR / filename
    )

    plt.savefig(
        output_path,
        dpi=300,
        bbox_inches="tight",
    )

    plt.close()

    print(
        f"Created: {output_path}"
    )


def label_bars(
    bars: Any,
    *,
    suffix: str = "",
) -> None:
    """Add readable values above vertical bars."""

    for bar in bars:
        value = bar.get_height()

        plt.annotate(
            f"{value:g}{suffix}",
            (
                bar.get_x()
                + bar.get_width() / 2,
                value,
            ),
            textcoords="offset points",
            xytext=(0, 6),
            ha="center",
            va="bottom",
        )


def generate_speed_charts(
    speed_runs: list[
        dict[str, Any]
    ],
) -> None:
    """Generate the three AWS speed-layer charts."""

    if len(speed_runs) != 3:
        raise ValueError(
            "Expected benchmark results for "
            "loads 100, 500 and 1000."
        )

    loads = [
        int(
            run["requested_records"]
        )
        for run in speed_runs
    ]

    producer_throughput = [
        float(
            run[
                "producer_throughput_records_per_second"
            ]
        )
        for run in speed_runs
    ]

    successful_records = [
        int(
            run["successful_records"]
        )
        for run in speed_runs
    ]

    observed_records = [
        int(
            run[
                "snapshot_observed_records"
            ]
        )
        for run in speed_runs
    ]

    wait_seconds = [
        float(
            run["time_to_snapshot_seconds"]
        )
        for run in speed_runs
    ]

    completed = [
        bool(
            run["snapshot_completed"]
        )
        for run in speed_runs
    ]

    # Figure 1: Kinesis producer throughput.
    plt.figure(
        figsize=(8.5, 5.4)
    )

    plt.plot(
        loads,
        producer_throughput,
        marker="o",
        linewidth=2,
    )

    plt.title(
        "Kinesis Producer Throughput by Load"
    )

    plt.xlabel(
        "Successfully submitted records"
    )

    plt.ylabel(
        "Records per second"
    )

    plt.xticks(loads)

    plt.ylim(
        bottom=0,
        top=max(
            producer_throughput
        ) * 1.18,
    )

    plt.grid(
        axis="y",
        alpha=0.3,
    )

    for load, value in zip(
        loads,
        producer_throughput,
    ):
        plt.annotate(
            f"{value:.1f}",
            (load, value),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
        )

    save_figure(
        "01_kinesis_producer_throughput.png",
        (
            "Each load was executed once. "
            "The results demonstrate observed performance, "
            "not linear scalability."
        ),
    )

    # Figure 2: submitted versus final S3 view.
    positions = list(
        range(len(loads))
    )

    width = 0.36

    plt.figure(
        figsize=(8.5, 5.4)
    )

    submitted_bars = plt.bar(
        [
            position - width / 2
            for position in positions
        ],
        successful_records,
        width=width,
        label="Successfully submitted",
    )

    snapshot_bars = plt.bar(
        [
            position + width / 2
            for position in positions
        ],
        observed_records,
        width=width,
        label="Final single-key S3 snapshot",
    )

    plt.title(
        "Submitted Records Versus Final Single-Key S3 Snapshot"
    )

    plt.xlabel(
        "Requested load"
    )

    plt.ylabel(
        "Records"
    )

    plt.xticks(
        positions,
        [
            str(load)
            for load in loads
        ],
    )

    plt.ylim(
        bottom=0,
        top=max(
            successful_records
        ) * 1.18,
    )

    plt.grid(
        axis="y",
        alpha=0.3,
    )

    plt.legend()

    label_bars(
        submitted_bars
    )

    label_bars(
        snapshot_bars
    )

    save_figure(
        "02_submitted_vs_single_key_snapshot.png",
        (
            "At the 1,000-record load, the 500-record ""difference was caused by "
            "isolated Lambda in-memory windows overwriting "
            "the same S3 object. It is not evidence of "
            "Kinesis record loss."
        ),
    )

    # Figure 3: completed materialisation time and timeout.
    status_labels = [
        (
            f"{load}\nCompleted"
            if is_complete
            else f"{load}\nTimeout"
        )
        for load, is_complete in zip(
            loads,
            completed,
        )
    ]

    fig, ax = plt.subplots(
        figsize=(10, 6)
    )

    wait_bars = ax.bar(
        status_labels,
        wait_seconds,
    )

    # Visually distinguish runs that reached the timeout.
    for bar, is_complete in zip(
        wait_bars,
        completed,
    ):
        if not is_complete:
            bar.set_hatch("//")
            bar.set_linewidth(0.8)

    ax.set_title(
        "S3 Snapshot Completion and Timeout",
        fontsize=16,
        pad=14,
    )

    ax.set_xlabel(
        "Requested load and benchmark outcome",
        fontsize=12,
    )

    ax.set_ylabel(
        "Elapsed seconds",
        fontsize=12,
    )

    maximum_wait = max(
        wait_seconds
    )

    # Leave enough space for the longest annotation.
    ax.set_ylim(
        0,
        maximum_wait * 1.38,
    )

    ax.grid(
        axis="y",
        alpha=0.3,
    )

    # Make the chart border lighter.
    for spine in ax.spines.values():
        spine.set_linewidth(0.8)

    for index, (
        value,
        is_complete,
    ) in enumerate(
        zip(
            wait_seconds,
            completed,
        )
    ):
        if is_complete:
            label = (
                f"{value:.2f} s"
            )

            vertical_offset = (
                maximum_wait * 0.025
            )

            font_size = 11
        else:
            label = (
                "Timeout reached\n"
                f"{value:.2f} s elapsed\n"
                "Expected snapshot not observed"
            )

            vertical_offset = (
                maximum_wait * 0.055
            )

            font_size = 9.5

        ax.text(
            index,
            value + vertical_offset,
            label,
            ha="center",
            va="bottom",
            fontsize=font_size,
            bbox=(
                {
                    "facecolor": "white",
                    "edgecolor": "none",
                    "alpha": 0.9,
                    "pad": 2.0,
                }
                if not is_complete
                else None
            ),
        )

    fig.text(
        0.5,
        0.015,
        (
            "A timeout bar represents an incomplete benchmark, "
            "not a completed snapshot materialisation time."
        ),
        ha="center",
        fontsize=9,
    )

    output_path = (
        OUTPUT_DIR
        / "03_snapshot_wait_and_timeout.png"
    )

    fig.tight_layout(
        rect=(0, 0.06, 1, 0.95)
    )

    fig.savefig(
        output_path,
        dpi=300,
        bbox_inches="tight",
    )

    plt.close(fig)

    print(
        f"Created: {output_path}"
    )


def generate_emr_charts() -> None:
    """Generate the EMR batch-performance charts."""

    workers = [
        1,
        2,
        4,
    ]

    execution_seconds = [
        410.4,
        380.1,
        294.1,
    ]

    observed_speedup = [
        1.00,
        1.08,
        1.40,
    ]

    ideal_speedup = [
        1.00,
        2.00,
        4.00,
    ]

    efficiency_percent = [
        100,
        54,
        35,
    ]

    # Figure 4: execution time.
    plt.figure(
        figsize=(8.5, 5.4)
    )

    execution_bars = plt.bar(
        [
            str(worker)
            for worker in workers
        ],
        execution_seconds,
    )

    plt.title(
        "EMR Batch Execution Time by Core Worker Count"
    )

    plt.xlabel(
        "Core workers"
    )

    plt.ylabel(
        "Execution time in seconds"
    )

    plt.ylim(
        bottom=0,
        top=max(
            execution_seconds
        ) * 1.18,
    )

    plt.grid(
        axis="y",
        alpha=0.3,
    )

    for bar, value in zip(
        execution_bars,
        execution_seconds,
    ):
        plt.annotate(
            f"{value:.1f} s",
            (
                bar.get_x()
                + bar.get_width() / 2,
                value,
            ),
            textcoords="offset points",
            xytext=(0, 6),
            ha="center",
        )

    save_figure(
        "04_emr_execution_time.png",
        (
            "Execution time decreased as workers increased, "
            "although the improvement remained sub-linear."
        ),
    )

    # Figure 5: observed versus ideal speedup.
    plt.figure(
        figsize=(8.5, 5.4)
    )

    plt.plot(
        workers,
        observed_speedup,
        marker="o",
        linewidth=2,
        label="Observed speedup",
    )

    plt.plot(
        workers,
        ideal_speedup,
        marker="o",
        linestyle="--",
        linewidth=2,
        label="Ideal linear speedup",
    )

    plt.title(
        "Observed Versus Ideal EMR Speedup"
    )

    plt.xlabel(
        "Core workers"
    )

    plt.ylabel(
        "Speedup relative to one worker"
    )

    plt.xticks(workers)

    plt.ylim(
        bottom=0,
        top=4.5,
    )

    plt.grid(
        axis="y",
        alpha=0.3,
    )

    plt.legend()

    for worker, value in zip(
        workers,
        observed_speedup,
    ):
        plt.annotate(
            f"{value:.2f}×",
            (worker, value),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
        )

    save_figure(
        "05_emr_observed_vs_ideal_speedup.png",
        (
            "The growing distance from ideal speedup shows "
            "the effect of fixed Spark overhead, partitioning "
            "constraints and shuffle or network costs."
        ),
    )

    # Figure 6: efficiency percentage.
    plt.figure(
        figsize=(8.5, 5.4)
    )

    efficiency_bars = plt.bar(
        [
            str(worker)
            for worker in workers
        ],
        efficiency_percent,
    )

    plt.title(
        "EMR Parallel Efficiency"
    )

    plt.xlabel(
        "Core workers"
    )

    plt.ylabel(
        "Parallel efficiency (%)"
    )

    plt.ylim(
        bottom=0,
        top=115,
    )

    plt.grid(
        axis="y",
        alpha=0.3,
    )

    label_bars(
        efficiency_bars,
        suffix="%",
    )

    save_figure(
        "06_emr_parallel_efficiency.png",
        (
            "Efficiency decreased from 100% to 35% as the "
            "worker count increased, demonstrating diminishing returns."
        ),
    )


def main() -> None:
    """Generate all final report charts."""

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Remove only charts produced by this script.
    for pattern in (
        "01_*.png",
        "02_*.png",
        "03_*.png",
        "04_*.png",
        "05_*.png",
        "06_*.png",
    ):
        for path in OUTPUT_DIR.glob(
            pattern
        ):
            path.unlink()

    speed_runs = load_speed_runs()

    generate_speed_charts(
        speed_runs
    )

    generate_emr_charts()

    print(
        "\nAll six report-ready charts "
        "created successfully."
    )


if __name__ == "__main__":
    main()
