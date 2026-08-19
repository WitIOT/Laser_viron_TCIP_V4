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
        # cooldown กัน log error ซ้ำ — log ได้ทุก 30s เท่านั้นเมื่อ fail ต่อเนื่อง
        self._err_cooldown: float = 30.0
        self._last_err_ts: float = 0.0
        self._err_lock = threading.Lock()

    def _should_log_error(self) -> bool:
        """คืน True ถ้าผ่าน cooldown แล้ว (กัน log spam)"""
        now = time.monotonic()
        with self._err_lock:
            if now - self._last_err_ts >= self._err_cooldown:
                self._last_err_ts = now
                return True
        return False

    def _reset_err_cooldown(self) -> None:
        with self._err_lock:
            self._last_err_ts = 0.0

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
                self._reset_err_cooldown()   # reset เมื่อสำเร็จ
                if self._log:
                    self._log(f"Roof POST OK: {url} -> {text}")
            except Exception as e:
                res = RoofResult(ok=False, state="UNKNOWN", raw_text="", error=str(e))
                if self._log and self._should_log_error():
                    self._log(f"Roof POST failed: {e} (จะ suppress log ถัดไป {self._err_cooldown:.0f}s)")
            if on_result:
                on_result(res)

        threading.Thread(target=worker, daemon=True).start()

    def _get_async(self, url: str, on_result: Optional[Callable[[RoofResult], None]]) -> None:
        def worker():
            try:
                text = _http_get_text(url, timeout=self._timeout)
                state = self._parse_state_from_text(text)
                res = RoofResult(ok=True, state=state, raw_text=text)
                self._reset_err_cooldown()
                if self._log:
                    self._log(f"Roof GET OK: {url} -> {text}")
            except Exception as e:
                res = RoofResult(ok=False, state="UNKNOWN", raw_text="", error=str(e))
                if self._log and self._should_log_error():
                    self._log(f"Roof GET failed: {e} (จะ suppress log ถัดไป {self._err_cooldown:.0f}s)")
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
        # cooldown กัน log "Limit GET failed" ซ้ำ ทุก 30s
        self._err_cooldown: float = 30.0
        self._last_err_ts: float = 0.0
        self._err_lock = threading.Lock()

    def _should_log_error(self) -> bool:
        now = time.monotonic()
        with self._err_lock:
            if now - self._last_err_ts >= self._err_cooldown:
                self._last_err_ts = now
                return True
        return False

    def _reset_err_cooldown(self) -> None:
        with self._err_lock:
            self._last_err_ts = 0.0

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
                self._reset_err_cooldown()   # reset เมื่อสำเร็จ
                return state
            return "N/A"
        except Exception as e:
            if self._log and self._should_log_error():
                self._log(f"Limit GET failed: {e} (suppress ถัดไป {self._err_cooldown:.0f}s)")
            return "N/A"

    def fetch_state_async(
        self,
        on_result: Optional[Callable[[str], None]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """GET limit/status แบบ non-blocking — ผลกลับผ่าน callback (daemon thread)"""
        def worker():
            state = self.fetch_state(timeout=timeout)
            if on_result:
                try:
                    on_result(state)
                except Exception:
                    pass
        threading.Thread(target=worker, daemon=True, name="LimitPollThread").start()


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


@dataclass
class RainData:
    """ผลลัพธ์จาก GET /api/rain"""
    ok: bool
    is_raining: bool = False
    intensity: float = 0.0        # mm/hr
    total: float = 0.0            # mm accumulation
    timestamp: str = ""
    lens_bad: bool = False
    error: str = ""


class RainSensorClient:
    """
    Client สำหรับ GET /api/rain

    JSON ที่คาดหวัง:
    {
      "ok": true,
      "is_raining": false,
      "intensity": {"value": 0, "unit": "mm/hr"},
      "accumulation": {"total": {"value": 55.5, "unit": "mm"}},
      "lens": {"lens_bad": false, ...},
      "timestamp": "2026-04-29T08:39:20.505104+00:00"
    }

    ใช้งาน:
        client = RainSensorClient(url_getter=lambda: self.rain_api_url, timeout=2.5)
        client.fetch(on_result=self._on_rain_result)   # async (non-blocking)
        data = client.fetch_sync(timeout=2.5)          # sync (blocking)
    """

    def __init__(
        self,
        url_getter: Callable[[], str],
        timeout: float = 2.5,
        logger: Optional[Callable[[str], None]] = None,
    ):
        self._url_getter = url_getter
        self._timeout = float(timeout)
        self._log = logger

    def _get_url(self) -> str:
        return (self._url_getter() or "").strip()

    @staticmethod
    def _parse(raw: str) -> RainData:
        """แปลง JSON string → RainData"""
        obj = _safe_json_loads(raw)
        if not obj.get("ok"):
            return RainData(ok=False, error="API returned ok=false")
        try:
            is_raining = bool(obj.get("is_raining", False))
            intensity  = float((obj.get("intensity") or {}).get("value", 0.0))
            total      = float(
                ((obj.get("accumulation") or {}).get("total") or {}).get("value", 0.0)
            )
            ts         = str(obj.get("timestamp", ""))[:19].replace("T", " ")
            lens_bad   = bool((obj.get("lens") or {}).get("lens_bad", False))
            return RainData(
                ok=True,
                is_raining=is_raining,
                intensity=intensity,
                total=total,
                timestamp=ts,
                lens_bad=lens_bad,
            )
        except Exception as e:
            return RainData(ok=False, error=f"Parse error: {e}")

    # ------------------------------------------------------------------ #
    #  Rain condition evaluation                                          #
    # ------------------------------------------------------------------ #

    CONDITION_FLAG      = "flag"        # ใช้ is_raining จาก API เท่านั้น
    CONDITION_INTENSITY = "intensity"   # ใช้ intensity > threshold เท่านั้น
    CONDITION_OR        = "or"          # is_raining=true หรือ intensity > threshold
    CONDITION_AND       = "and"         # is_raining=true และ intensity > threshold

    @staticmethod
    def evaluate_rain(
        data: "RainData",
        condition: str = "flag",
        intensity_threshold: float = 0.1,
    ) -> bool:
        """
        ตัดสินว่า "ฝนตก" หรือไม่ ตาม condition ที่เลือก

        condition:
            "flag"      – ใช้ data.is_raining จาก API (default)
            "intensity" – intensity > intensity_threshold
            "or"        – is_raining=True หรือ intensity > threshold
            "and"       – is_raining=True และ intensity > threshold
        """
        flag = data.is_raining
        above = data.intensity > intensity_threshold

        if condition == "intensity":
            return above
        elif condition == "or":
            return flag or above
        elif condition == "and":
            return flag and above
        else:  # "flag" (default)
            return flag

    def fetch_sync(self, timeout: Optional[float] = None) -> RainData:
        """GET /api/rain แบบ blocking – คืน RainData เสมอ (ok=False ถ้าล้มเหลว)"""
        url = self._get_url()
        if not url:
            return RainData(ok=False, error="rain_api_url is empty")
        t = self._timeout if timeout is None else float(timeout)
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=t) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            return self._parse(raw)
        except Exception as e:
            if self._log:
                self._log(f"Rain GET failed: {e}")
            return RainData(ok=False, error=str(e))

    def fetch(
        self,
        on_result: Optional[Callable[["RainData"], None]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """GET /api/rain แบบ async – ผลกลับผ่าน callback on_result (daemon thread)"""
        def worker():
            result = self.fetch_sync(timeout=timeout)
            if on_result:
                try:
                    on_result(result)
                except Exception:
                    pass

        threading.Thread(target=worker, daemon=True, name="RainSensorThread").start()
