import hashlib
import os
import re
import threading
import time

PII_PATTERNS = [
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
]


def scrub_and_fingerprint(raw_text: str, blob_iterations: int = 120_000) -> tuple[str, str]:
    """
    Redacts common PII patterns and computes a deterministic cryptographic fingerprint.
    CPU-bound (~40-60ms per invocation depending on host CPU).
    """
    cleaned = raw_text
    for pattern in PII_PATTERNS:
        cleaned = pattern.sub("[REDACTED]", cleaned)

    digest = cleaned.encode("utf-8")
    for _ in range(blob_iterations):
        digest = hashlib.sha256(digest).digest()

    return cleaned, digest.hex()


def init_pool_worker() -> None:
    """Keep fingerprinting off the latency path, and exit if the parent dies.

    Windows `TerminateProcess` does not reap pool workers, and the harness
    stops uvicorn that way. A dead parent would otherwise leave the children
    blocked on the pool queue.
    """
    try:
        if os.name == "nt":
            import ctypes

            # BELOW_NORMAL_PRIORITY_CLASS
            ctypes.windll.kernel32.SetPriorityClass(
                ctypes.windll.kernel32.GetCurrentProcess(), 0x00004000
            )
        else:
            os.nice(10)
    except Exception:
        pass
    threading.Thread(target=_exit_when_parent_dies, name="parent-watch", daemon=True).start()


def _exit_when_parent_dies() -> None:
    parent = os.getppid()
    if os.name == "nt":
        import ctypes

        kernel = ctypes.windll.kernel32
        handle = kernel.OpenProcess(0x1000, False, parent)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return
        code = ctypes.c_ulong()
        try:
            while True:
                time.sleep(0.5)
                if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)):
                    os._exit(0)
                if code.value != 259:  # STILL_ACTIVE
                    os._exit(0)
        finally:
            kernel.CloseHandle(handle)
        return

    while True:
        time.sleep(0.5)
        if os.getppid() != parent:
            os._exit(0)


def pool_warmup() -> int:
    """Touch the worker process so the pool is hot before the first burst."""
    return os.getpid()
