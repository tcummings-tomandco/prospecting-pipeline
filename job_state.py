"""Thread-safe background job state.

Stores job state at module level so it survives Streamlit script reruns.
This is what lets the enrichment continue when the user switches browser tabs:
the worker thread keeps running even though the WebSocket connection drops.

State lives in process memory. Cloud Run with min-instances=1 keeps the
container alive between requests so jobs don't get lost. If the container
is replaced (deploy, crash, scale-down) any in-flight jobs are lost.
"""
import threading
import traceback
import uuid
from datetime import datetime

_jobs = {}
_lock = threading.Lock()


def create_job(kind="enrich"):
    """Create a pending job and return its ID."""
    job_id = str(uuid.uuid4())
    with _lock:
        _jobs[job_id] = {
            "id": job_id,
            "kind": kind,
            "status": "pending",
            "progress": 0,
            "total": 0,
            "current": "",
            "messages": [],
            "result": None,
            "error": None,
            "created_at": datetime.now().isoformat(),
            "completed_at": None,
        }
    return job_id


def update_progress(job_id, idx, total, current, message):
    """Worker calls this to report progress."""
    with _lock:
        if job_id in _jobs:
            j = _jobs[job_id]
            j["progress"] = idx + 1
            j["total"] = total
            j["current"] = current
            j["messages"].append(message)
            # Cap message buffer
            if len(j["messages"]) > 100:
                j["messages"] = j["messages"][-100:]


def get_job(job_id):
    """Return a snapshot of the job state."""
    with _lock:
        if job_id not in _jobs:
            return None
        # Return a shallow copy so caller can't mutate state
        j = _jobs[job_id]
        return {
            "id": j["id"],
            "kind": j["kind"],
            "status": j["status"],
            "progress": j["progress"],
            "total": j["total"],
            "current": j["current"],
            "messages": list(j["messages"]),
            "result": j["result"],
            "error": j["error"],
            "created_at": j["created_at"],
            "completed_at": j["completed_at"],
        }


def list_active_jobs(kind=None):
    """List job IDs that are pending or running."""
    with _lock:
        return [
            jid for jid, j in _jobs.items()
            if j["status"] in ("pending", "running")
            and (kind is None or j["kind"] == kind)
        ]


def clear_job(job_id):
    """Remove a job from memory (call after the user is done with the result)."""
    with _lock:
        _jobs.pop(job_id, None)


def run_in_background(job_id, target_fn):
    """Run target_fn() in a daemon thread, capturing result/error in job state.

    target_fn should be a zero-arg callable (use functools.partial or a closure
    to bind your arguments). Use update_progress(job_id, ...) inside the function
    to report progress.
    """
    with _lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "running"

    def wrapper():
        try:
            result = target_fn()
            with _lock:
                if job_id in _jobs:
                    _jobs[job_id]["result"] = result
                    _jobs[job_id]["status"] = "complete"
                    _jobs[job_id]["completed_at"] = datetime.now().isoformat()
        except Exception as e:
            err_msg = f"{e}\n\n{traceback.format_exc()}"
            print(f"[JOB {job_id}] FAILED: {err_msg}", flush=True)
            with _lock:
                if job_id in _jobs:
                    _jobs[job_id]["error"] = err_msg
                    _jobs[job_id]["status"] = "failed"
                    _jobs[job_id]["completed_at"] = datetime.now().isoformat()

    t = threading.Thread(target=wrapper, daemon=True, name=f"job-{job_id[:8]}")
    t.start()
    return t
