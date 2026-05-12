"""Disk persistence for completed enrichments.

Survives session_state resets (file uploader X, page refresh) but not container
restarts. For full durability we'd need Cloud Storage; for now /tmp is good
enough because we have min-instances=1 keeping one container alive.
"""
import json
import os
import time
from datetime import datetime

import pandas as pd

CACHE_DIR = "/tmp/enrich_cache"
DATA_FILE = os.path.join(CACHE_DIR, "last_enrichment.parquet")
META_FILE = os.path.join(CACHE_DIR, "last_enrichment.json")


def _ensure_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def save_enrichment(df, filename, category):
    """Persist the enriched DataFrame and its metadata."""
    _ensure_dir()
    try:
        df.to_parquet(DATA_FILE, index=False)
    except Exception:
        # Parquet needs pyarrow; fall back to pickle if not available
        df.to_pickle(DATA_FILE)
    meta = {
        "filename": filename,
        "category": category,
        "rows": len(df),
        "saved_at": datetime.now().isoformat(),
        "epoch": time.time(),
    }
    with open(META_FILE, "w") as f:
        json.dump(meta, f)


def load_enrichment(max_age_hours=24):
    """Load the most recent enrichment if it exists and is within max_age.

    Returns (df, metadata) or (None, None) if no fresh cache.
    """
    if not (os.path.exists(DATA_FILE) and os.path.exists(META_FILE)):
        return None, None

    try:
        with open(META_FILE) as f:
            meta = json.load(f)
        age_hours = (time.time() - meta.get("epoch", 0)) / 3600
        if age_hours > max_age_hours:
            return None, None

        try:
            df = pd.read_parquet(DATA_FILE)
        except Exception:
            df = pd.read_pickle(DATA_FILE)
        return df, meta
    except Exception as e:
        print(f"[PERSISTENCE] Failed to load: {e}", flush=True)
        return None, None


def clear_enrichment():
    """Delete the cached enrichment files."""
    for p in (DATA_FILE, META_FILE):
        try:
            os.remove(p)
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"[PERSISTENCE] Failed to delete {p}: {e}", flush=True)


def has_cached_enrichment(max_age_hours=24):
    """Quick check without loading the full DataFrame."""
    if not os.path.exists(META_FILE):
        return False
    try:
        with open(META_FILE) as f:
            meta = json.load(f)
        age_hours = (time.time() - meta.get("epoch", 0)) / 3600
        return age_hours <= max_age_hours
    except Exception:
        return False


def get_cached_metadata():
    """Return just the metadata of the cached enrichment, or None."""
    if not os.path.exists(META_FILE):
        return None
    try:
        with open(META_FILE) as f:
            return json.load(f)
    except Exception:
        return None
