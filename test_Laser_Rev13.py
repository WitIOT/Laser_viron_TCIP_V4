# test_roof_safety_v4.py
"""
Tests สำหรับบั๊กความปลอดภัยเรื่องหลังคา/เลเซอร์ ใน Laser_Rev13.py

เคสจริงที่เจอ:
  1. ฝนตกระหว่างรอรอบถัดไป → โปรแกรมถูก pause
  2. prefire timer ที่ค้างอยู่พยายามเปิดหลังคา (ถูกบล็อกด้วย rain guard)
  3. ฝนหยุด → ผู้ใช้เปิดหลังคาเองและยิง manual
  4. postrest timer ที่ค้างอยู่สั่ง "ปิดหลังคา" ขณะเลเซอร์ยังยิง → เลเซอร์ไม่ตัด (อันตราย)

รันด้วย: pytest test_roof_safety_v2.py -v
ทดสอบเฉพาะ logic ไม่สร้างหน้าต่าง Tk / ไม่ต่อเลเซอร์จริง
"""
import queue
import threading
import time
import types

import pytest

import Laser_Rev13 as L

App = L.App


class FakeRoofClient:
    """จำลอง SlidingRoofClient — บันทึกว่าถูกสั่งปิด/เปิดกี่ครั้ง"""

    def __init__(self, order=None):
        self.close_calls = 0
        self.open_calls = 0
        self._order = order

    def post_close(self, on_result=None):
        self.close_calls += 1
        if self._order is not None:
            self._order.append("close")

    def post_open(self, on_result=None):
        self.open_calls += 1
        if self._order is not None:
            self._order.append("open")


class FakeLaser:
    """จำลอง LaserClient — บันทึกคำสั่งที่ถูกส่งแบบ sync"""

    def __init__(self, order=None):
        self.sent = []
        self._order = order

    def send_cmd(self, cmd: str) -> str:
        self.sent.append(cmd)
        if self._order is not None:
            self._order.append(cmd)
        return "OK"


def make_app():
    """สร้าง object ที่มีเฉพาะ attribute ที่เมธอดเหล่านี้ใช้ — ไม่เรียก App.__init__ (เลี่ยง Tk)"""
    app = App.__new__(App)
    app.order = []                      # ลำดับเหตุการณ์รวม (คำสั่งเลเซอร์ + คำสั่งหลังคา)
    app.is_firing = False
    app.manual_lock = threading.Lock()
    app.tele_pause_until = 0.0
    app.roof_client = FakeRoofClient(order=app.order)
    app.laser = FakeLaser(order=app.order)
    app.msg_q = queue.Queue()
    app.logs = []
    app._prefire_timers = {}
    app._postrest_timers = {}
    app._delayed_close_after_ids = {}
    app.roof_na_grace_sec = 10.0
    app._roof_na_since = None
    # ต้องกำหนดไว้เสมอ: App สืบทอด tk.Tk ซึ่ง __getattr__ จะวนซ้ำถ้า attribute ไม่มีจริง
    app.active_program_idx = None
    app.tele_owner_idx = None

    app.log = lambda msg: app.logs.append(str(msg))
    app._on_roof_result = lambda *a, **k: None
    # after() ในเทสต์: เรียก callback ทันที (ไม่มี event loop จริง)
    app.after = lambda ms, fn=None, *a: (fn() if callable(fn) else None)
    app.after_cancel = lambda aid: None
    app._append_status_point = lambda v: None
    return app


def wait_for(cond, timeout=2.0, interval=0.02):
    """รอจนเงื่อนไขเป็นจริง (roof_close ทำงานใน background thread)"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return False


# --------------------------------------------------------------------- #
#  Interlock: ปิดหลังคาขณะยิง ต้องดับเลเซอร์ก่อน
# --------------------------------------------------------------------- #
class TestRoofCloseInterlock:
    def test_close_while_firing_sends_standby_first(self):
        """บั๊กหลัก: ปิดหลังคาขณะเลเซอร์ยิงอยู่ ต้องสั่ง STANDBY ก่อนเสมอ"""
        app = make_app()
        app.is_firing = True

        app.roof_close(reason="auto postrest")

        assert app.is_firing is False, "is_firing ต้องถูกเคลียร์ทันที (synchronous)"
        assert wait_for(lambda: app.roof_client.close_calls == 1), "หลังคาต้องถูกสั่งปิด"
        assert "$STANDBY" in app.laser.sent, "ต้องสั่ง $STANDBY ก่อนปิดหลังคา"

    def test_standby_is_sent_before_close_command(self):
        """ลำดับสำคัญ: STANDBY ต้องส่งถึงเลเซอร์จริงก่อนคำสั่งปิดหลังคา"""
        app = make_app()
        app.is_firing = True

        app.roof_close()

        assert wait_for(lambda: len(app.order) >= 2)
        assert app.order[0] == "$STANDBY", f"ลำดับผิด: {app.order}"
        assert app.order[1] == "close", f"ลำดับผิด: {app.order}"

    def test_close_when_not_firing_does_not_send_standby(self):
        """ถ้าไม่ได้ยิงอยู่ ไม่ต้องยุ่งกับเลเซอร์"""
        app = make_app()
        app.is_firing = False

        app.roof_close()

        assert app.laser.sent == []
        assert app.roof_client.close_calls == 1

    def test_close_still_proceeds_when_standby_fails(self):
        """ถ้าสั่ง STANDBY ไม่สำเร็จ ยังต้องปิดหลังคาต่อ (fail-safe)"""
        app = make_app()
        app.is_firing = True

        def boom(cmd):
            raise RuntimeError("laser offline")
        app.laser.send_cmd = boom

        app.roof_close()

        assert wait_for(lambda: app.roof_client.close_calls == 1), "หลังคาต้องถูกปิดแม้ STANDBY ล้มเหลว"

    def test_external_off_goes_through_interlock(self):
        """เส้นทาง auto postrest ก็ต้องผ่าน interlock เดียวกัน"""
        app = make_app()
        app.is_firing = True

        app._external_off()

        assert app.is_firing is False
        assert wait_for(lambda: app.roof_client.close_calls == 1)
        assert "$STANDBY" in app.laser.sent

    def test_delayed_roof_close_goes_through_interlock(self):
        """เส้นทาง delayed close (หลัง FINAL REST) ก็ต้องผ่าน interlock"""
        app = make_app()
        app.is_firing = True

        app._delayed_roof_close()

        assert app.is_firing is False
        assert wait_for(lambda: app.roof_client.close_calls == 1)
        assert "$STANDBY" in app.laser.sent


# --------------------------------------------------------------------- #
#  ยกเลิก timer หลังคาที่ค้างอยู่
# --------------------------------------------------------------------- #
class TestTimerCancellation:
    def test_cancel_api_timers_cancels_both_prefire_and_postrest(self):
        app = make_app()
        fired = []
        t_pre = threading.Timer(30.0, lambda: fired.append("pre"))
        t_post = threading.Timer(30.0, lambda: fired.append("post"))
        t_pre.daemon = t_post.daemon = True
        t_pre.start()
        t_post.start()
        app._prefire_timers[0] = t_pre
        app._postrest_timers[0] = t_post

        app._cancel_api_timers_for(0)

        # cancel() หยุด callback ทันที แต่ thread ใช้เวลาสักครู่กว่าจะจบ → join ก่อนเช็ค
        t_pre.join(timeout=2.0)
        t_post.join(timeout=2.0)

        assert not t_pre.is_alive(), "prefire timer ต้องถูกยกเลิก"
        assert not t_post.is_alive(), "postrest timer ต้องถูกยกเลิก"
        assert app._prefire_timers.get(0) is None
        assert app._postrest_timers.get(0) is None
        assert fired == [], "callback ของ timer ต้องไม่ถูกเรียก"

    def test_cancel_delayed_roof_close_clears_handle(self):
        app = make_app()
        cancelled = []
        app._delayed_close_after_ids = {0: "after#123"}
        app.after_cancel = lambda aid: cancelled.append(aid)

        app._cancel_delayed_roof_close(0)

        assert cancelled == ["after#123"]
        assert app._delayed_close_after_ids == {}

    def test_cancel_delayed_roof_close_is_safe_when_nothing_scheduled(self):
        app = make_app()
        app._cancel_delayed_roof_close()  # ต้องไม่ raise
        assert app._delayed_close_after_ids == {}

    def test_cancel_one_program_does_not_cancel_another(self):
        """แยก handle ตามโปรแกรม: หยุดโปรแกรม 0 ต้องไม่ยกเลิก delayed close ของโปรแกรม 1"""
        app = make_app()
        cancelled = []
        app._delayed_close_after_ids = {0: "a0", 1: "a1"}
        app.after_cancel = lambda aid: cancelled.append(aid)

        app._cancel_delayed_roof_close(0)

        assert cancelled == ["a0"]
        assert app._delayed_close_after_ids == {1: "a1"}


# --------------------------------------------------------------------- #
#  ฝนตก → STOP โปรแกรม (ไม่ใช่ pause) + กัน popup block laser
# --------------------------------------------------------------------- #
class TestRainStopsPrograms:
    def _make_rain_app(self):
        app = make_app()
        app._RAIN_POPUP_COOLDOWN = 300.0
        app._rain_popup_ts = 0.0
        app.programs = []
        app._sched_log = lambda i, m: app.logs.append(f"[{i}] {m}")
        app._ui_update_prog = lambda *a, **k: None
        app._show_rain_popup = lambda *a, **k: None
        app._get_roof_status_cached = lambda: "OFF"
        app._batch_stopping = False
        app.stopped_idx = []
        app.stop_program = lambda i: app.stopped_idx.append(i)
        return app

    def test_rain_stops_running_programs(self):
        """เคสที่รายงาน: ฝนตกต้อง STOP โปรแกรม (ไม่ใช่ pause) เพื่อหยุด scheduler จริง"""
        app = self._make_rain_app()
        runner = types.SimpleNamespace(is_alive=lambda: True)
        prog = {"runner": runner}
        app.programs = [prog]

        app._on_rain_started()

        assert app.stopped_idx == [0], "ฝนตก → ต้องเรียก stop_program ของโปรแกรมที่รันอยู่"

    def test_rain_does_not_stop_already_stopped_programs(self):
        app = self._make_rain_app()
        runner = types.SimpleNamespace(is_alive=lambda: False)
        app.programs = [{"runner": runner}]

        app._on_rain_started()

        assert app.stopped_idx == []

    def test_rain_stops_laser_when_firing(self):
        app = self._make_rain_app()
        app.is_firing = True

        app._on_rain_started()

        assert app.is_firing is False
        assert wait_for(lambda: "$STANDBY" in app.laser.sent), "ฝนตกขณะยิง → ต้องสั่ง STANDBY"


class TestFireGuardSuppressedDuringRain:
    def _guard_app(self, state, raining, monkeypatch, warned):
        app = make_app()
        app._is_safety_fire_enabled = lambda: True
        app._get_roof_status_cached = lambda: state
        app.is_raining_now = lambda: raining
        app._apply_roof_status = lambda *a, **k: None
        # รัน callback ของ after ทันที เพื่อให้ _warn (ถ้ามี) ถูกเรียกจริง
        app.after = lambda ms, fn=None, *a: (fn() if callable(fn) else None)
        monkeypatch.setattr(L.messagebox, "showwarning",
                            lambda *a, **k: warned.append(a))
        return app

    def test_no_popup_when_roof_closed_during_rain(self, monkeypatch):
        """บั๊กที่รายงาน: ฝนตกโปรแกรมถูกสั่งหยุด แต่ยังมี popup block laser เด้ง"""
        warned = []
        app = self._guard_app("OFF", raining=True, monkeypatch=monkeypatch, warned=warned)
        ok = app._guard_fire_by_roof()
        assert ok is False, "หลังคาปิด → ยังต้องบล็อกการยิง"
        assert warned == [], "ขณะฝนตก ต้องไม่เด้ง popup block laser"

    def test_popup_shown_when_roof_closed_without_rain(self, monkeypatch):
        """ควบคุม: ไม่มีฝน หลังคาปิด → ยังต้องเตือนตามปกติ"""
        warned = []
        app = self._guard_app("OFF", raining=False, monkeypatch=monkeypatch, warned=warned)
        ok = app._guard_fire_by_roof()
        assert ok is False
        assert len(warned) == 1, "ไม่มีฝน หลังคาปิด → ต้องเตือน 1 ครั้ง"


# --------------------------------------------------------------------- #
#  Orphan timer: prefire เปิดหลังคาเองหลังโปรแกรมหยุดแล้ว
# --------------------------------------------------------------------- #
class FakeVar:
    def __init__(self, value=True):
        self._v = value

    def get(self):
        return self._v

    def set(self, v):
        self._v = v


def make_sched_app(active=True, stopped=False):
    """app จำลองที่มีโปรแกรม 1 ตัว พร้อม state ตามที่ระบุ"""
    from datetime import timedelta

    app = make_app()
    app.roof_auto_sched_var = FakeVar(True)
    app.laser = object()          # ถือว่า connect แล้ว
    app.roof_preopen_sec = 15
    app.roof_postclose_sec = 0.05
    app._sched_log = lambda i, m: app.logs.append(f"[{i}] {m}")
    app.is_raining_now = lambda: False
    app._is_safety_fire_enabled = lambda: False   # ให้ _go จบเร็ว ไม่ต้องรอ roof
    app._wait_roof_on = lambda **k: True

    manager_stop = threading.Event()
    if stopped:
        manager_stop.set()

    runner = types.SimpleNamespace(is_alive=lambda: active)
    app.programs = [{
        "runner": runner,
        "manager_stop": manager_stop,
    }]
    app.opened = []
    app.closed = []
    app._external_on = lambda: app.opened.append("open")
    app._external_off = lambda: app.closed.append("close")
    return app


class TestProgramActiveGuard:
    def test_active_program_is_active(self):
        app = make_sched_app()
        assert app._is_program_active(0) is True

    def test_stopped_program_is_not_active(self):
        app = make_sched_app(stopped=True)
        assert app._is_program_active(0) is False

    def test_dead_runner_is_not_active(self):
        app = make_sched_app(active=False)
        assert app._is_program_active(0) is False

    def test_bad_index_is_not_active(self):
        app = make_sched_app()
        assert app._is_program_active(99) is False


class TestPrefireNotArmedWhenInactive:
    def test_prefire_not_armed_for_stopped_program(self):
        from datetime import timedelta
        app = make_sched_app(stopped=True)
        app._schedule_prefire_api(0, L.datetime.now(L.TZ) + timedelta(minutes=10))
        assert app._prefire_timers.get(0) is None

    def test_prefire_not_armed_when_fire_time_already_passed(self):
        """เวลายิงผ่านไปแล้ว → ต้องไม่ตั้ง timer delay=0 ที่เปิดหลังคาทันที"""
        from datetime import timedelta
        app = make_sched_app()
        app._schedule_prefire_api(0, L.datetime.now(L.TZ) - timedelta(minutes=5))
        assert app._prefire_timers.get(0) is None, "ไม่ควรตั้ง timer สำหรับเวลาที่ผ่านไปแล้ว"
        time.sleep(0.2)
        assert app.opened == [], "หลังคาต้องไม่ถูกเปิด"

    def test_postrest_not_armed_for_stopped_program(self):
        app = make_sched_app(stopped=True)
        app._schedule_postrest_api(0)
        assert app._postrest_timers.get(0) is None


class TestOrphanTimerDoesNotActuateRoof:
    """
    บั๊กที่รายงาน: prefire เปิดหลังคาเองตอน 14:24:56 ทั้งที่โปรแกรมหยุดไปแล้วตั้งแต่ 14:06:55
    ต่อให้ timer หลุดรอดมาได้ ตอนมันทำงานจริงต้องตรวจสถานะซ้ำแล้วไม่เปิดหลังคา
    """

    def test_prefire_go_does_not_open_roof_if_program_stopped_after_arming(self):
        from datetime import timedelta
        app = make_sched_app()
        # ตั้ง timer ให้ทำงานในอีก ~0.05s
        start = L.datetime.now(L.TZ) + timedelta(seconds=15.05)
        app._schedule_prefire_api(0, start)
        assert app._prefire_timers.get(0) is not None, "ควรตั้ง timer ได้ตอนโปรแกรมยัง active"

        # จำลอง: ผู้ใช้กด Stop Program หลังจาก timer ถูกตั้งไปแล้ว
        app.programs[0]["manager_stop"].set()

        time.sleep(0.4)
        assert app.opened == [], "โปรแกรมหยุดแล้ว → prefire ต้องไม่เปิดหลังคา"

    def test_prefire_go_does_not_open_roof_when_raining(self):
        from datetime import timedelta
        app = make_sched_app()
        start = L.datetime.now(L.TZ) + timedelta(seconds=15.05)
        app._schedule_prefire_api(0, start)
        app.is_raining_now = lambda: True

        time.sleep(0.4)
        assert app.opened == [], "ฝนตก → prefire ต้องไม่เปิดหลังคา"

    def test_prefire_go_opens_roof_when_still_active(self):
        """ควบคุม: ถ้าทุกอย่างปกติ prefire ต้องเปิดหลังคาตามเดิม"""
        from datetime import timedelta
        app = make_sched_app()
        start = L.datetime.now(L.TZ) + timedelta(seconds=15.05)
        app._schedule_prefire_api(0, start)

        time.sleep(0.4)
        assert app.opened == ["open"], "สถานะปกติ prefire ต้องเปิดหลังคา"

    def test_postrest_go_does_not_close_roof_if_program_stopped(self):
        """orphan postrest หลังกด Stop Program ต้องไม่ปิดหลังคา (stop_program จัดการเอง)"""
        app = make_sched_app()
        app._schedule_postrest_api(0)
        assert app._postrest_timers.get(0) is not None

        app.programs[0]["manager_stop"].set()

        time.sleep(0.4)
        assert app.closed == [], "โปรแกรมหยุดแล้ว → postrest ต้องไม่ปิดหลังคา"

    def test_postrest_closes_roof_when_program_active(self):
        """
        ควบคุม: postrest ทำงานระหว่างรอบพัก (โปรแกรมยัง active)
        ต้องปิดหลังคาตามปกติ (auto open/close cycle)
        """
        app = make_sched_app()
        app._schedule_postrest_api(0)
        assert app._postrest_timers.get(0) is not None

        time.sleep(0.4)
        assert app.closed == ["close"], "โปรแกรมยัง active → postrest ต้องปิดหลังคา"


# --------------------------------------------------------------------- #
#  Index shift หลังลบโปรแกรม
# --------------------------------------------------------------------- #
class TestReindexAfterRemove:
    def test_timer_keys_shift_down(self):
        """ลบโปรแกรม 0 → timer ของโปรแกรม 1,2 ต้องเลื่อนมาเป็น 0,1"""
        app = make_app()
        t0, t1, t2 = object(), object(), object()
        app._prefire_timers = {0: t0, 1: t1, 2: t2}
        app._postrest_timers = {1: t1, 2: t2}

        app._reindex_after_remove(0)

        assert app._prefire_timers == {0: t1, 1: t2}, "timer ต้องเลื่อน index ตามโปรแกรม"
        assert app._postrest_timers == {0: t1, 1: t2}

    def test_removed_program_timer_is_cancelled(self):
        app = make_app()
        fired = []
        t = threading.Timer(30.0, lambda: fired.append("x"))
        t.daemon = True
        t.start()
        app._prefire_timers = {1: t}

        app._reindex_after_remove(1)

        t.join(timeout=2.0)
        assert not t.is_alive(), "timer ของโปรแกรมที่ถูกลบต้องถูกยกเลิก"
        assert app._prefire_timers == {}
        assert fired == []

    def test_active_and_tele_owner_idx_shift(self):
        app = make_app()
        app.active_program_idx = 2
        app.tele_owner_idx = 3

        app._reindex_after_remove(1)

        assert app.active_program_idx == 1
        assert app.tele_owner_idx == 2

    def test_removed_index_clears_owner(self):
        app = make_app()
        app.active_program_idx = 1
        app.tele_owner_idx = 1

        app._reindex_after_remove(1)

        assert app.active_program_idx is None
        assert app.tele_owner_idx is None

    def test_lower_indices_unchanged(self):
        app = make_app()
        app.active_program_idx = 0
        app.tele_owner_idx = 0

        app._reindex_after_remove(2)

        assert app.active_program_idx == 0
        assert app.tele_owner_idx == 0

    def test_delayed_close_ids_shift(self):
        app = make_app()
        app._delayed_close_after_ids = {0: "a0", 2: "a2"}
        app.after_cancel = lambda aid: None

        app._reindex_after_remove(0)

        assert app._delayed_close_after_ids == {1: "a2"}


# --------------------------------------------------------------------- #
#  รวม flag การปิดหลังคาอัตโนมัติ
# --------------------------------------------------------------------- #
class TestRoofAutoCloseFlags:
    def _app(self, sched, ctrl):
        app = make_app()
        app.roof_auto_sched_var = FakeVar(sched)
        app.roof_auto_ctrl_var = FakeVar(ctrl)
        return app

    def test_enabled_when_either_flag_on(self):
        assert self._app(True, False)._is_roof_auto_close_enabled() is True
        assert self._app(False, True)._is_roof_auto_close_enabled() is True
        assert self._app(True, True)._is_roof_auto_close_enabled() is True

    def test_disabled_only_when_both_off(self):
        assert self._app(False, False)._is_roof_auto_close_enabled() is False


# --------------------------------------------------------------------- #
#  N/A grace period ขณะยิง
# --------------------------------------------------------------------- #
class TestRoofNaGracePeriod:
    def _monitor_app(self, state):
        app = make_app()
        app.is_firing = True
        app._is_safety_fire_enabled = lambda: True
        app._get_roof_status_cached = lambda: state
        app._warn_roof = lambda *a, **k: None
        app.after = lambda ms, fn=None, *a: None   # ไม่ reschedule ใน test
        return app

    def test_na_does_not_stop_laser_immediately(self):
        app = self._monitor_app("N/A")
        app._monitor_roof_during_fire()
        assert app.is_firing is True, "N/A ช่วงแรกยังไม่ตัด (กัน API กระตุก)"
        assert app._roof_na_since is not None

    def test_na_stops_laser_after_grace_expires(self):
        app = self._monitor_app("N/A")
        app.roof_na_grace_sec = 10.0
        app._roof_na_since = time.monotonic() - 11.0   # ค้างมาแล้ว 11 วินาที

        app._monitor_roof_during_fire()

        assert app.is_firing is False, "N/A เกิน grace → ต้องหยุดเลเซอร์"
        assert "$STANDBY" in app.laser.sent or any("STANDBY" in s for s in app.logs)

    def test_grace_zero_disables_na_cut(self):
        """ตั้ง roof_na_grace_sec = 0 → กลับไปพฤติกรรมเดิม (ตัดเฉพาะ OFF)"""
        app = self._monitor_app("N/A")
        app.roof_na_grace_sec = 0
        app._roof_na_since = time.monotonic() - 999

        app._monitor_roof_during_fire()

        assert app.is_firing is True, "grace=0 → N/A ต้องไม่ตัดเลเซอร์"

    def test_na_timer_resets_when_status_recovers(self):
        app = self._monitor_app("ON")
        app._roof_na_since = time.monotonic() - 5

        app._monitor_roof_during_fire()

        assert app._roof_na_since is None, "อ่านสถานะได้แล้ว → ต้อง reset ตัวจับเวลา"
        assert app.is_firing is True

    def test_off_still_stops_laser_immediately(self):
        app = self._monitor_app("OFF")
        app._monitor_roof_during_fire()
        assert app.is_firing is False, "OFF ต้องตัดทันทีเหมือนเดิม"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
