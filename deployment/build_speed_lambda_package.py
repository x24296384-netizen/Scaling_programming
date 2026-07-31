"""Build the deployment package for the Kinesis speed Lambda."""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


PROJECT_ROOT = Path(
    __file__
).resolve().parents[1]

OUTPUT_PATH = (
    PROJECT_ROOT
    / "build"
    / "scp-speed-processor-25186396.zip"
)

REQUIRED_FILES = [
    Path("speed/__init__.py"),
    Path("speed/lambda_handler.py"),
    Path("speed/batch_delta_store.py"),
    Path("speed/snapshot_store.py"),
    Path("speed/stream_consumer.py"),
    Path("speed/window_analytics.py"),
]


def validate_sources() -> None:
    """Confirm that every required Python file exists and parses."""

    missing: list[str] = []

    for relative_path in REQUIRED_FILES:
        source_path = (
            PROJECT_ROOT / relative_path
        )

        if not source_path.is_file():
            missing.append(
                relative_path.as_posix()
            )
            continue

        source = source_path.read_text(
            encoding="utf-8-sig"
        )

        ast.parse(
            source,
            filename=str(source_path),
        )

    if missing:
        raise FileNotFoundError(
            "Missing required Lambda files: "
            + ", ".join(missing)
        )


def calculate_sha256(
    path: Path,
) -> str:
    """Calculate the SHA-256 digest of one file."""

    digest = hashlib.sha256()

    with path.open("rb") as file_handle:
        for block in iter(
            lambda: file_handle.read(
                1024 * 1024
            ),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def build_package() -> None:
    """Create a clean Lambda ZIP with speed at its root."""

    validate_sources()

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    with ZipFile(
        OUTPUT_PATH,
        mode="w",
        compression=ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        for relative_path in REQUIRED_FILES:
            source_path = (
                PROJECT_ROOT
                / relative_path
            )

            archive.write(
                source_path,
                arcname=relative_path.as_posix(),
            )

    with ZipFile(
        OUTPUT_PATH,
        mode="r",
    ) as archive:
        members = archive.namelist()

        expected = [
            path.as_posix()
            for path in REQUIRED_FILES
        ]

        if members != expected:
            raise RuntimeError(
                "Unexpected ZIP structure: "
                f"{members}"
            )

        bad_member = archive.testzip()

        if bad_member is not None:
            raise RuntimeError(
                "Corrupt ZIP member: "
                f"{bad_member}"
            )

    print(
        f"Package: {OUTPUT_PATH}"
    )
    print(
        f"Size: {OUTPUT_PATH.stat().st_size} bytes"
    )
    print(
        f"SHA256: {calculate_sha256(OUTPUT_PATH)}"
    )

    print("\nMembers:")

    for member in members:
        print(f"- {member}")

    print(
        "\nPackage validation: OK"
    )


if __name__ == "__main__":
    build_package()
