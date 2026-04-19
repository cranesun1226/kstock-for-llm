from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _read_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _resolve_path(project_root: Path, raw_path: str | None, default: str) -> Path:
    if raw_path:
        candidate = Path(raw_path)
    else:
        candidate = Path(default)
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return candidate


@dataclass(frozen=True)
class Settings:
    project_root: Path
    data_dir: Path
    raw_dir: Path
    silver_dir: Path
    gold_dir: Path
    database_path: Path
    api_key: str


def load_settings(project_root: Path | None = None) -> Settings:
    root = project_root or Path.cwd()
    _read_dotenv(root / ".env")

    api_key = os.environ.get("OPENDART_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "OPENDART_API_KEY is not set. Add it to your shell environment or .env."
        )

    data_dir = _resolve_path(
        root,
        os.environ.get("OPENDART_DATA_DIR") or os.environ.get("KSTOCK_DATA_DIR"),
        "data",
    )
    raw_dir = data_dir / "raw"
    silver_dir = data_dir / "silver"
    gold_dir = data_dir / "gold"
    database_path = _resolve_path(
        root,
        os.environ.get("OPENDART_DB_PATH") or os.environ.get("KSTOCK_DB_PATH"),
        str(data_dir / "opendart.db"),
    )

    return Settings(
        project_root=root,
        data_dir=data_dir,
        raw_dir=raw_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        database_path=database_path,
        api_key=api_key,
    )
