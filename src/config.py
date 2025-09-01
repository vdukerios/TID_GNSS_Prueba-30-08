import json
from pathlib import Path
from typing import Any, Dict


def load_params(path: Path = None) -> Dict[str, Any]:
    """Load parameters JSON from project config/params.json by default."""
    if path is None:
        base = Path(__file__).resolve().parents[1]
        path = base / "config" / "params.json"
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    return cfg


def get_protocol_params(cfg: Dict[str, Any], protocol_key: str) -> Dict[str, Any]:
    return cfg.get("protocols", {}).get(protocol_key, {})


__all__ = ["load_params", "get_protocol_params"]
