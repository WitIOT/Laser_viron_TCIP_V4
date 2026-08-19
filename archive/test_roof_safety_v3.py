# test_roof_safety_v3.py
"""
Tests สำหรับบั๊กความปลอดภัยเรื่องหลังคา/เลเซอร์ ใน laser_Rev11_v3.py

เคสจริงที่เจอ:
  1. ฝนตกระหว่างรอรอบถัดไป → โปรแกรมถูก pause
  2. prefire timer ที่ค้างอยู่พยายามเปิดหลังคา (ถูกบล็อกด้วย rain guard)
  3. ฝนหยุด → ผู้ใช้เปิดหลังคาเองและยิง manual
  4. postrest timer ที่ค้างอยู่สั่ง "ปิดหลังคา" ขณะเลเซอร์ยังยิง → เลเซอร์ไม่ตัด (อันตราย)

รันด้วย: pytest test_roof_safety_v2.py -v
ทดสอบเฉพาะ logic ไม่สร้างหน้าต่าง Tk / ไม่ต่อเลเซอร์จริง
"""
import threading
import time
import types

import pytest

import laser_Rev11_v3 as L

App = L.App


class FakeRoofClient:
    """จำลอง SlidingRoofClient — บันทึกว่าถูกสั่งปิด/เปิดกี่ครั้ง"""

    def __init__(self):
        self.close_calls = 0
        self.open_calls = 0

    def post_close(self, on_result=None):
        self.close_calls += 1

    def post_open(self, on_result=None):
        self.open_calls += 1


def make_app():
    """สร้าง object ที่มีเฉพาะ attribute ที่เมธอดเหล่านี้ใช้ — ไม่เรียก App.__init__ (เลี่ยง Tk)"""
    app = App.__new__(App)
    app.is_firing = False
    app.manual_lock = threading.Lock()
    app.tele_pause_until = 0.0
    app.roof_client = FakeRoofClient()
    app.sent = []
    app.logs = []
    app._prefire_timers = {}
    app._postrest_timers = {}
    app._delayed_close_after_id = None

    app._send = lambda cmd: app.sent.append(cmd)
    app.log = lambda msg: app.logs.append(str(msg))
    app._on_roof_result = lambda *a, **k: None
    # after() ในเทสต์: เรียก callback ทันที (ไม่มี event loop จริง)
    app.after = lambda ms, fn=None, *a: (fn() if callable(fn) else None)
    app.after_cancel = lambda aid: None
    app._append_status_point = lambda v: None
    return app


# --------------------------------------------------------------------- #
#  Interlock: ปิดหลังคาขณะยิง ต้องดับเลเซอร์ก่อน
# --------------------------------------------------------------------- #
class TestRoofCloseInterlock:
    def test_close_while_firing_sends_standby_first(self):
        """บั๊กหลัก: ปิดหลังคาขณะเลเซอร์ยิงอยู่ ต้องสั่ง STANDBY ก่อนเสมอ"""
        app = make_app()
        app.is_firing = True

        app.roof_close(reason="auto postrest")

        assert "$STANDBY" in app.sent, "ต้องสั่ง $STANDBY ก่อนปิดหลังคา"
        assert app.is_firing is False, "is_firing ต้องถูกเคลียร์"
        assert app.roof_client.close_calls == 1, "หลังคาต้องถูกสั่งปิด"

    def test_standby_is_sent_before_close_command(self):
        """ลำดับสำคัญ: STANDBY ต้องมาก่อนคำสั่งปิดหลังคา"""
        app = make_app()
        app.is_firing = True
        order = []
        app._send = lambda cmd: order.append(("send", cmd))
        app.roof_client.post_close = lambda on_result=None: order.append(("close", None))

        app.roof_close()

        assert order[0] == ("send", "$STANDBY")
        assert order[1][0] == "close"

    def test_close_when_not_firing_does_not_send_standby(self):
        """ถ้าไม่ได้ยิงอยู่ ไม่ต้องยุ่งกับเลเซอร์"""
        app = make_app()
        app.is_firing = False

        app.roof_close()

        assert "$STANDBY" not in app.sent
        assert app.roof_client.close_calls == 1

    def test_external_off_goes_through_interlock(self):
        """เส้นทาง auto postrest ก็ต้องผ่าน interlock เดียวกัน"""
        app = make_app()
        app.is_firing = True

        app._external_off()

        assert "$STANDBY" in app.sent
        assert app.is_firing is False
        assert app.roof_client.close_calls == 1

    def test_delayed_roof_close_goes_through_interlock(self):
        """เส้นทาง delayed close (หลัง FINAL REST) ก็ต้องผ่าน interlock"""
        app = make_app()
        app.is_firing = True

        app._delayed_roof_close()

        assert "$STANDBY" in app.sent
        assert app.is_firing is False
        assert app.roof_client.close_calls == 1


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
        app._delayed_close_after_id = "after#123"
        app.after_cancel = lambda aid: cancelled.append(aid)

        app._cancel_delayed_roof_close()

        assert cancelled == ["after#123"]
        assert app._delayed_close_after_id is None

    def test_cancel_delayed_roof_close_is_safe_when_nothing_scheduled(self):
        app = make_app()
        app._delayed_close_after_id = None
        app._cancel_delayed_roof_close()  # ต้องไม่ raise
        assert app._delayed_close_after_id is None


# --------------------------------------------------------------------- #
#  ฝนตก → ต้องยกเลิก timer หลังคาของโปรแกรมที่ pause
# --------------------------------------------------------------------- #
class TestRainCancelsRoofTimers:
    def _make_rain_app(self):
        app = make_app()
        app._RAIN_POPUP_COOLDOWN = 300.0
        app._rain_popup_ts = 0.0
        app.programs = []
        app._sched_log = lambda i, m: app.logs.append(f"[{i}] {m}")
        app._ui_update_prog = lambda *a, **k: None
        app._show_rain_popup = lambda *a, **k: None
        app._get_roof_status_cached = lambda: "OFF"
        app.cancelled_idx = []
        return app

    def test_rain_cancels_roof_timers_for_running_programs(self):
        """ต้นเหตุของบั๊ก: ฝนตก pause โปรแกรม แต่ timer หลังคาไม่ถูกยกเลิก"""
        app = self._make_rain_app()

        runner = types.SimpleNamespace(is_alive=lambda: True)
        prog = {"runner": runner, "paused": threading.Event()}
        app.programs = [prog]
        app._cancel_api_timers_for = lambda i: app.cancelled_idx.append(i)

        app._on_rain_started()

        assert prog["paused"].is_set(), "โปรแกรมต้องถูก pause"
        assert app.cancelled_idx == [0], "ต้องยกเลิก prefire/postrest timer ของโปรแกรมที่รันอยู่"

    def test_rain_does_not_cancel_timers_for_stopped_programs(self):
        app = self._make_rain_app()
        runner = types.SimpleNamespace(is_alive=lambda: False)
        app.programs = [{"runner": runner, "paused": threading.Event()}]
        app._cancel_api_timers_for = lambda i: app.cancelled_idx.append(i)

        app._on_rain_started()

        assert app.cancelled_idx == []

    def test_rain_stops_laser_when_firing(self):
        app = self._make_rain_app()
        app.is_firing = True
        app._cancel_api_timers_for = lambda i: None

        app._on_rain_started()

        assert "$STANDBY" in app.sent
        assert app.is_firing is False


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


def make_sched_app(active=True, paused=False, stopped=False):
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
    paused_ev = threading.Event()
    if stopped:
        manager_stop.set()
    if paused:
        paused_ev.set()

    runner = types.SimpleNamespace(is_alive=lambda: active)
    app.programs = [{
        "runner": runner,
        "paused": paused_ev,
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

    def test_paused_program_is_not_active(self):
        app = make_sched_app(paused=True)
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

    def test_postrest_not_armed_for_paused_program(self):
        app = make_sched_app(paused=True)
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

    def test_postrest_go_does_not_close_roof_if_program_paused(self):
        """เคสที่รายงาน: ฝนตก→pause→ผู้ใช้คุมหลังคาเอง → orphan postrest ต้องไม่ปิดหลังคา"""
        app = make_sched_app()
        app._schedule_postrest_api(0)
        assert app._postrest_timers.get(0) is not None

        app.programs[0]["paused"].set()

        time.sleep(0.4)
        assert app.closed == [], "โปรแกรมถูก pause → postrest ต้องไม่ปิดหลังคา"

    def test_postrest_still_closes_roof_when_program_finishes_normally(self):
        """
        กันไม่ให้ guard เข้มเกินไป: โปรแกรมจบตามปกติ (manager thread จบแล้ว)
        หลังคาต้องยังถูกปิดให้เรียบร้อย ไม่ค้างเปิดทิ้งไว้
        """
        app = make_sched_app()
        app._schedule_postrest_api(0)
        assert app._postrest_timers.get(0) is not None

        # จำลองโปรแกรมจบเอง: manager หยุด + thread ตาย (ไม่ใช่ pause)
        app.programs[0]["manager_stop"].set()
        app.programs[0]["runner"].is_alive = lambda: False

        time.sleep(0.4)
        assert app.closed == ["close"], "โปรแกรมจบปกติ → หลังคาต้องถูกปิด"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
