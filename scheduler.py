# scheduler.py
from __future__ import annotations
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Bangkok")

class FireRestScheduler(threading.Thread):
    """Run FIRE/REST cycles between start_time and end_time.
    Calls on_fire() at the start of each firing window, then on_rest() for rest windows.
    on_tick(now_dt) is called periodically for UI refreshes.
    """

    def __init__(
        self,
        start_time: datetime,
        end_time: datetime,
        fire_minutes: int,
        rest_minutes: int,
        on_fire,
        on_rest,
        on_tick,
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True)
        self.start_dt = start_time
        self.end_dt = end_time
        self.fire_td = timedelta(minutes=fire_minutes)
        self.rest_td = timedelta(minutes=rest_minutes)
        self.on_fire = on_fire
        self.on_rest = on_rest
        self.on_tick = on_tick
        self.stop_event = stop_event

    @staticmethod
    def count_fire_cycles(start_dt: datetime, end_dt: datetime, fire_td: timedelta, rest_td: timedelta) -> int:
        if end_dt <= start_dt or fire_td.total_seconds() <= 0 or rest_td.total_seconds() < 0:
            return 0
        cycle = fire_td + rest_td
        total = end_dt - start_dt
        full_cycles = int(total // cycle)
        leftover = total - (cycle * full_cycles)
        extra = 1 if leftover >= fire_td else 0
        return full_cycles + extra

    def run(self):
        now = datetime.now(TZ)
        while not self.stop_event.is_set() and now < self.start_dt:
            time.sleep(0.2)
            now = datetime.now(TZ)
            self.on_tick(now)

        current = self.start_dt
        while not self.stop_event.is_set() and current < self.end_dt:
            # FIRE phase
            fire_until = min(current + self.fire_td, self.end_dt)
            if datetime.now(TZ) < fire_until:
                self.on_fire()
            while not self.stop_event.is_set() and datetime.now(TZ) < fire_until:
                time.sleep(0.2)
                self.on_tick(datetime.now(TZ))
            if self.stop_event.is_set() or datetime.now(TZ) >= self.end_dt:
                break
            # REST phase
            rest_until = min(fire_until + self.rest_td, self.end_dt)
            if datetime.now(TZ) < rest_until:
                self.on_rest()
            while not self.stop_event.is_set() and datetime.now(TZ) < rest_until:
                time.sleep(0.2)
                self.on_tick(datetime.now(TZ))
            current = rest_until
