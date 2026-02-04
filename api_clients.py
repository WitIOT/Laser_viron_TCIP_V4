# api_clients.py
from __future__ import annotations

import json
import threading
import time
import urllib.request
from dataclasses import dataclass
from typing import Callable, Optional


# -------------------------
# Common helpers
# -------------------------
def _safe_json_loads(s: str) -> dict:
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _http_get_text(url: str, timeout: float = 4.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore").strip()


def _http_post_text(url: str, timeout: float = 4.0) -> str:
    req = urllib.request.Request(url, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore").strip()


@dataclass
class RoofResult:
    ok: bool
    state: str = "UNKNOWN"     # ON/OFF/UNKNOWN
    raw_text: str = ""
    error: str = ""


class SlidingRoofClient:
    """
    Client สำหรับ Door/Roof API (open/close/status)
    - ทำงาน async ด้วย thread เพื่อไม่ให้ UI ค้าง
    - ส่งผลกลับผ่าน callback (on_result)
    """

    def __init__(
        self,
        base_url_getter: Callable[[], str],
        timeout: float = 4.0,
        logger: Optional[Callable[[str], None]] = None,
    ):
        self._base_url_getter = base_url_getter
        self._timeout = float(timeout)
        self._log = logger

    def _base(self) -> str:
        b = (self._base_url_getter() or "").strip()
        return b if b.endswith("/") else (b + "/")

    @staticmethod
    def _parse_state_from_text(text: str) -> str:
        obj = _safe_json_loads(text)
        msg = str(obj.get("message", "")).upper().strip()
        if msg in ("ON", "OFF"):
            return msg
        return "UNKNOWN"

    def post_open(self, on_result: Optional[Callable[[RoofResult], None]] = None) -> None:
        self._post_async(self._base() + "open", on_result)

    def post_close(self, on_result: Optional[Callable[[RoofResult], None]] = None) -> None:
        self._post_async(self._base() + "close", on_result)

    def get_status(self, on_result: Optional[Callable[[RoofResult], None]] = None) -> None:
        self._get_async(self._base() + "status", on_result)

    def _post_async(self, url: str, on_result: Optional[Callable[[RoofResult], None]]) -> None:
        def worker():
            try:
                text = _http_post_text(url, timeout=self._timeout)
                state = self._parse_state_from_text(text)
                res = RoofResult(ok=True, state=state, raw_text=text)
                if self._log:
                    self._log(f"Roof POST OK: {url} -> {text}")
            except Exception as e:
                res = RoofResult(ok=False, state="UNKNOWN", raw_text="", error=str(e))
                if self._log:
                    self._log(f"Roof POST failed: {e}")
            if on_result:
                on_result(res)

        threading.Thread(target=worker, daemon=True).start()

    def _get_async(self, url: str, on_result: Optional[Callable[[RoofResult], None]]) -> None:
        def worker():
            try:
                text = _http_get_text(url, timeout=self._timeout)
                state = self._parse_state_from_text(text)
                res = RoofResult(ok=True, state=state, raw_text=text)
                if self._log:
                    self._log(f"Roof GET OK: {url} -> {text}")
            except Exception as e:
                res = RoofResult(ok=False, state="UNKNOWN", raw_text="", error=str(e))
                if self._log:
                    self._log(f"Roof GET failed: {e}")
            if on_result:
                on_result(res)

        threading.Thread(target=worker, daemon=True).start()


class LimitStatusClient:
    """
    Client สำหรับ GET /limit/status -> ON/OFF/N/A
    """

    def __init__(
        self,
        url_getter: Callable[[], str],
        timeout: float = 2.0,
        logger: Optional[Callable[[str], None]] = None,
    ):
        self._url_getter = url_getter
        self._timeout = float(timeout)
        self._log = logger

    def fetch_state(self, timeout: Optional[float] = None) -> str:
        url = (self._url_getter() or "").strip()
        if not url:
            return "N/A"
        t = self._timeout if timeout is None else float(timeout)
        try:
            text = _http_get_text(url, timeout=t)
            obj = _safe_json_loads(text)
            limit = obj.get("limit", {}) if isinstance(obj, dict) else {}
            state = str(limit.get("state", "")).upper().strip()
            if state in ("ON", "OFF"):
                return state
            return "N/A"
        except Exception as e:
            if self._log:
                self._log(f"Limit GET failed: {e}")
            return "N/A"


class IntervalPoller:
    """
    ตัวช่วย polling (เช่น roof status) ทุก N วินาที แบบหยุด/เริ่มได้
    """

    def __init__(self, interval_sec: float, fn: Callable[[], None]):
        self.interval_sec = max(0.2, float(interval_sec))
        self.fn = fn
        self._stop = threading.Event()
        self._th: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._th and self._th.is_alive():
            return
        self._stop.clear()
        self._th = threading.Thread(target=self._run, daemon=True)
        self._th.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        next_t = time.monotonic()
        while not self._stop.is_set():
            try:
                self.fn()
            except Exception:
                pass
            next_t += self.interval_sec
            self._stop.wait(max(0.05, next_t - time.monotonic()))
