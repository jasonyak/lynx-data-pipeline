"""Thread-safe utilities for parallel processing."""
import json
import threading
import time
from collections import defaultdict

from config import PRICING
from scraping.scraper import WebsiteScraper
from analysis.local_ai import LocalRefiner


class ThreadSafeCostTracker:
    """Thread-safe cost tracker replacing defaultdict."""
    def __init__(self):
        self._lock = threading.Lock()
        self._data = defaultdict(lambda: {"input": 0, "output": 0})

    def add(self, step: str, input_tokens: int, output_tokens: int):
        with self._lock:
            self._data[step]["input"] += input_tokens
            self._data[step]["output"] += output_tokens

    def get_snapshot(self) -> dict:
        with self._lock:
            return dict(self._data)


class ThreadSafeOutputWriter:
    """Thread-safe file writer for parallel record output."""
    def __init__(self, output_path: str):
        self._lock = threading.Lock()
        self._file = open(output_path, 'a')
        self._written_count = 0

    def write(self, record: dict):
        with self._lock:
            self._file.write(json.dumps(record) + "\n")
            self._file.flush()
            self._written_count += 1

    def get_written_count(self) -> int:
        with self._lock:
            return self._written_count

    def close(self):
        self._file.close()


class ProgressReporter:
    """Background thread that prints progress every 5 seconds."""
    def __init__(self, total: int, cost_tracker: ThreadSafeCostTracker):
        self._total = total
        self._cost_tracker = cost_tracker
        self._completed = 0
        self._lock = threading.Lock()
        self._start_time = time.time()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._report_loop, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=1)

    def increment(self):
        with self._lock:
            self._completed += 1

    def _report_loop(self):
        while not self._stop_event.wait(timeout=5.0):
            self._print_progress()
        self._print_progress()  # Final report

    def _print_progress(self):
        with self._lock:
            completed = self._completed

        remaining = self._total - completed
        pct = (completed / self._total * 100) if self._total > 0 else 0

        elapsed = time.time() - self._start_time
        if completed > 0:
            rate = completed / elapsed
            eta_seconds = remaining / rate if rate > 0 else 0
            eta_str = f"{eta_seconds:.0f}s"
        else:
            eta_str = "calculating..."

        # Get current cost
        cost_snapshot = self._cost_tracker.get_snapshot()
        total_cost = self._calculate_cost(cost_snapshot)

        print(f"\n[Progress] {completed}/{self._total} ({pct:.1f}%) | "
              f"Remaining: {remaining} | ETA: {eta_str} | "
              f"Cost: ${total_cost:.4f}")

    def _calculate_cost(self, cost_snapshot: dict) -> float:
        total = 0.0
        for step, tokens in cost_snapshot.items():
            input_cost = (tokens["input"] / 1_000_000) * PRICING["gemini"]["input"]
            output_cost = (tokens["output"] / 1_000_000) * PRICING["gemini"]["output"]
            total += input_cost + output_cost
        return total


class ThreadSafeRefiner:
    """Thread-safe wrapper for LocalRefiner with lock-based access."""
    def __init__(self):
        self._lock = threading.Lock()
        self._refiner = LocalRefiner()

    def rank_images(self, *args, **kwargs):
        with self._lock:
            return self._refiner.rank_images(*args, **kwargs)

    def filter_pdfs(self, *args, **kwargs):
        with self._lock:
            return self._refiner.filter_pdfs(*args, **kwargs)

    def refine_text(self, *args, **kwargs):
        with self._lock:
            return self._refiner.refine_text(*args, **kwargs)


# Thread-local storage for per-thread scrapers
_thread_local = threading.local()


def get_thread_scraper() -> WebsiteScraper:
    """Get or create thread-local scraper instance."""
    if not hasattr(_thread_local, 'scraper'):
        _thread_local.scraper = WebsiteScraper()
    return _thread_local.scraper
