# app3.py
from __future__ import annotations
import socket, threading, queue, time, csv, os, re, json, calendar
from datetime import datetime, timedelta, timezone, date
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        TZ = ZoneInfo("Asia/Bangkok")
    except ZoneInfoNotFoundError:
        TZ = timezone(timedelta(hours=7))
except Exception:
    TZ = timezone(timedelta(hours=7))

from api_clients import SlidingRoofClient, LimitStatusClient, RoofResult
from tutorial_overlay import TutorialOverlay
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


# ---------------- Paths ----------------
# LOG_DIR = "logs"
# LOG_DIR = r"C:\Users\LiDAR\OneDrive - NARIT (1)\LiDAR\LiDAR-data\Laser-logs"
LOG_DIR = r"logs/data"
SETTINGS_DIR = "setting"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(SETTINGS_DIR, exist_ok=True)
CONFIG_FILE = os.path.join(SETTINGS_DIR, "laser_scheduler_settings.json")


# ---------------- Laser Client ----------------
class LaserClient:
    def _send_and_read_one(self, cmd: str, timeout_s: float) -> str:
        """Send command and read response safely (multi-line tolerant), drain leftovers, drop echo."""
        if not self.sock:
            raise RuntimeError("Not connected")

        s = self.sock
        # ส่งคำสั่ง
        s.sendall((cmd.strip() + "\n").encode())

        # อ่านอย่างน้อย 1 บรรทัด (หรือจน timeout)
        s.settimeout(timeout_s)
        chunks: list[bytes] = []
        try:
            while True:
                b = s.recv(1024)
                if not b:
                    break
                chunks.append(b)
                if b.endswith(b"\n"):
                    break
        except socket.timeout:
            pass

        data = b"".join(chunks)

        # ✅ Drain เศษข้อมูลที่ค้างอยู่ (กัน response ไปโผล่ในคำสั่งถัดไป)
        try:
            s.settimeout(0.02)
            while True:
                b = s.recv(4096)
                if not b:
                    break
                data += b
        except socket.timeout:
            pass

        text = data.decode(errors="ignore").replace("\r", "")
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        if not lines:
            return ""

        # บางเครื่อง echo คำสั่งกลับมา → ตัดทิ้ง
        c = cmd.strip()
        if lines and lines[0].startswith(c):
            lines = lines[1:]

        # ✅ เลือกบรรทัดที่ตรงกับคำสั่งที่ถาม (กัน response สลับ/หลุด)
        expect = ""
        try:
            s_cmd = cmd.strip()
            if s_cmd.startswith("$"):
                s_cmd = s_cmd[1:]
            expect = s_cmd.split()[0].upper() if s_cmd else ""
        except Exception:
            expect = ""

        if expect:
            for ln in lines:
                up = ln.upper()
                if expect in up:
                    return ln

        # fallback: คืนบรรทัดสุดท้ายที่มีข้อมูล
        return lines[-1] if lines else ""

    def try_send_cmd(self, cmd: str, call_timeout: float | None = None) -> str | None:
        if not self.sock:
            raise RuntimeError("Not connected")

        # พยายามจับล็อกแบบไม่บล็อก
        locked = self.lock.acquire(blocking=False)
        if not locked:
            return None  # BUSY: มีคนใช้อยู่ (เช่น Telemetry/คำสั่งอื่น)

        try:
            timeout_s = float(call_timeout) if call_timeout is not None else float(self.timeout)
            resp = self._send_and_read_one(cmd, timeout_s=timeout_s)
            return resp.strip() if resp else ""
        finally:
            # กู้คืน timeout เดิม (กัน side effect)
            try:
                if self.sock:
                    self.sock.settimeout(self.timeout)
            except Exception:
                pass
            self.lock.release()

    def get_status(self):
        # ใช้ non-blocking + timeout สั้น
        resp = self.try_send_cmd("$STATUS ?", call_timeout=0.4)
        if not resp:
            return None  # กลับ None เพื่อให้ผู้เรียกตัดสินใจว่าจะคงค่าเดิมไว้
        parts = resp.split()
        if len(parts) < 2:
            return None
        try:
            state = int(parts[1][0:2], 16)
        except ValueError:
            return None

        if state & 0b10000000:
            mode = "FIRE"
        elif state & 0b01000000:
            mode = "STANDBY"
        else:
            mode = "STOP"

        # ready = "Not Ready" if state & 0b00000001 else "Ready"
        return f"{mode} "


    def __init__(self, host: str, port: int, timeout: float = 3.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: socket.socket | None = None
        self.lock = threading.Lock()

    def connect(self):
        self.close()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect((self.host, self.port))
        self.sock = s

    def close(self):
        with self.lock:
            if self.sock:
                try: self.sock.close()
                except Exception: pass
                self.sock = None

    def send_cmd(self, cmd: str) -> str:
        with self.lock:
            if not self.sock:
                raise RuntimeError("Not connected")
            resp = self._send_and_read_one(cmd, timeout_s=float(self.timeout))
            # กู้คืน timeout เดิม
            try:
                if self.sock:
                    self.sock.settimeout(self.timeout)
            except Exception:
                pass
            return resp.strip() if resp else ""


# ---------------- One-shot Scheduler (single occurrence) ----------------
class FireRestScheduler(threading.Thread):
    def __init__(
        self,
        start_time: datetime,
        end_time: datetime,
        fire_ms: int,
        rest_ms: int,
        on_fire,
        on_rest,
        on_tick,
        stop_event: threading.Event,
        on_done=None,
    ):
        super().__init__(daemon=True)
        self.start_dt = start_time
        self.end_dt = end_time
        self.fire_td = timedelta(milliseconds=fire_ms)
        self.rest_td = timedelta(milliseconds=rest_ms)
        self.on_fire = on_fire
        self.on_rest = on_rest
        self.on_tick = on_tick
        self.stop_event = stop_event
        self.on_done = on_done

    @staticmethod
    def count_fire_cycles(start_dt: datetime, end_dt: datetime, fire_td: timedelta, rest_td: timedelta) -> int:
        if end_dt <= start_dt or fire_td.total_seconds() <= 0 or rest_td.total_seconds() < 0:
            return 0
        cycle = fire_td + rest_td
        total = end_dt - start_dt
        full = int(total // cycle)
        leftover = total - (cycle * full)
        return full + (1 if leftover >= fire_td else 0)

    def run(self):
        try:
            now = datetime.now(TZ)
            while not self.stop_event.is_set() and now < self.start_dt:
                time.sleep(0.2)
                now = datetime.now(TZ)
                try: self.on_tick(now)
                except Exception: pass

            current = self.start_dt
            while not self.stop_event.is_set() and current < self.end_dt:
                # FIRE
                fire_until = min(current + self.fire_td, self.end_dt)
                if datetime.now(TZ) < fire_until:
                    try: self.on_fire()
                    except Exception: pass
                while not self.stop_event.is_set() and datetime.now(TZ) < fire_until:
                    time.sleep(0.2)
                    try: self.on_tick(datetime.now(TZ))
                    except Exception: pass
                if self.stop_event.is_set() or datetime.now(TZ) >= self.end_dt:
                    break

                # REST
                rest_until = min(fire_until + self.rest_td, self.end_dt)
                if datetime.now(TZ) < rest_until:
                    try: self.on_rest(False)
                    except Exception: pass
                while not self.stop_event.is_set() and datetime.now(TZ) < rest_until:
                    time.sleep(0.2)
                    try: self.on_tick(datetime.now(TZ))
                    except Exception: pass
                current = rest_until
        finally:
            try: self.on_rest(True)
            except Exception: pass
            try:
                if self.on_done: self.on_done()
            except Exception:
                pass


# ---------------- Calendar Dialog ----------------
class CalendarDialog(tk.Toplevel):
    """
    ปฏิทินเลือกวัน:
      - multi=True  -> เลือกหลายวัน (Only select date)
      - multi=False -> เลือกวันเดียว (Once)
    ผลลัพธ์: set[date] หรือ None หากยกเลิก
    """
    def __init__(self, master, title="Select dates", multi=True, initial=None):
        super().__init__(master)
        self.title(title)
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()

        self.result = None

        self.multi = multi
        self.selected: set[date] = set(initial or [])
        today = date.today()
        self.year = today.year
        self.month = today.month

        frm_top = ttk.Frame(self); frm_top.pack(padx=8, pady=6, fill=tk.X)
        self.lbl_title = ttk.Label(frm_top, text="")
        btn_prev = ttk.Button(frm_top, text="<", width=3, command=self.prev_month)
        btn_next = ttk.Button(frm_top, text=">", width=3, command=self.next_month)
        btn_prev.pack(side=tk.LEFT)
        self.lbl_title.pack(side=tk.LEFT, expand=True)
        btn_next.pack(side=tk.LEFT)

        self.grid_frame = ttk.Frame(self); self.grid_frame.pack(padx=8, pady=6)

        frm_bot = ttk.Frame(self); frm_bot.pack(padx=8, pady=8, fill=tk.X)
        ttk.Button(frm_bot, text="Today", command=self.go_today).pack(side=tk.LEFT)
        ttk.Button(frm_bot, text="Cancel", command=self.cancel).pack(side=tk.RIGHT)
        ttk.Button(frm_bot, text="OK", command=self.ok).pack(side=tk.RIGHT, padx=6)

        self.buttons = []  # เก็บปุ่มวัน
        self.draw_month()
        self.protocol("WM_DELETE_WINDOW", self.cancel)
        self.wait_visibility()
        self.focus()
        self.wait_window(self) 

    def draw_month(self):
        for w in self.grid_frame.winfo_children():
            w.destroy()
        self.buttons.clear()

        self.lbl_title.config(text=f"{calendar.month_name[self.month]} {self.year}")

        # headers
        days = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        for i, d in enumerate(days):
            ttk.Label(self.grid_frame, text=d, width=4, anchor="center").grid(row=0, column=i, padx=1, pady=1)

        monthcal = calendar.Calendar(firstweekday=0).monthdatescalendar(self.year, self.month)
        for r, week in enumerate(monthcal, start=1):
            for c, d in enumerate(week):
                def mkcmd(dd=d):
                    return lambda: self.toggle_date(dd)
                if d.month != self.month:
                    b = ttk.Label(self.grid_frame, text=str(d.day), width=4, anchor="center", foreground="gray")
                    b.grid(row=r, column=c, padx=1, pady=1)
                else:
                    selected = (d in self.selected)
                    btn = tk.Button(self.grid_frame, text=str(d.day), width=4,
                                    relief=tk.SUNKEN if selected else tk.RAISED,
                                    command=mkcmd(d))
                    btn.grid(row=r, column=c, padx=1, pady=1)
                    self.buttons.append((d, btn))

    def update_buttons(self):
        for d, btn in self.buttons:
            btn.config(relief=tk.SUNKEN if d in self.selected else tk.RAISED)

    def toggle_date(self, d: date):
        if self.multi:
            if d in self.selected:
                self.selected.remove(d)
            else:
                self.selected.add(d)
        else:
            self.selected = {d}
        self.update_buttons()

    def prev_month(self):
        if self.month == 1:
            self.month = 12; self.year -= 1
        else:
            self.month -= 1
        self.draw_month()

    def next_month(self):
        if self.month == 12:
            self.month = 1; self.year += 1
        else:
            self.month += 1
        self.draw_month()

    def go_today(self):
        t = date.today()
        self.year, self.month = t.year, t.month
        self.draw_month()

    def ok(self):
        self.result = set(self.selected)
        self.destroy()

    def cancel(self):
        self.result = None
        self.destroy()


# ---------------- Tkinter App ----------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        # --- Sliding roof API base ---192.168.3.209:8000/api/gpio/23
        self.roof_api_base = "http://192.168.3.150:8000/door"
        self.limit_api_url = "http://192.168.3.150:8000/limit/status"
        self.log_dir = LOG_DIR
        # self.roof_api_base = "http://192.168.49.8:8000/door/"
        # self.limit_api_url = "http://192.168.49.8:8000/limit/status"
        self._roof_polling = False
        self._limit_poll_inflight = False
        self._roof_state_cached = "N/A"
        self._roof_state_ts = 0.0

        # --- Rain Sensor ---
        self.rain_enabled = True          # เปิด/ปิดการทำงานของ Rain Sensor
        self.rain_api_url = "http://192.168.3.150:8000/api/rain"
        self.rain_api_timeout = 2.5    # seconds – HTTP request timeout
        self.rain_stale_sec   = 10.0   # seconds – grace period ก่อนแสดง N/A
        self.rain_poll_interval = 1    # seconds – ความถี่ poll (ขั้นต่ำ 1)
        self._rain_is_raining = False          # สถานะฝนล่าสุด
        self._rain_intensity = 0.0             # mm/hr
        self._rain_total = 0.0                 # mm total
        self._rain_poll_stop = False
        self._rain_poll_inflight = False
        self._rain_last_ts = ""
        self._rain_last_ok_ts: float | None = None   # monotonic time ของการอ่านสำเร็จล่าสุด
        self._rain_fail_count: int = 0               # จำนวน consecutive failures
        self._rain_monitor_stop = threading.Event()

          # ใช้ bool สำหรับหยุด polling ใน _poll_roof_status()
        self.roof_client = SlidingRoofClient(
            base_url_getter=lambda: self.roof_api_base,
            timeout=4.0,
            logger=getattr(self, "log", None),
        )
        self.limit_client = LimitStatusClient(
            url_getter=lambda: self.limit_api_url,
            timeout=3.0,
            logger=getattr(self, "log", None),
        )
        self.title("Laser Software v5_Rev8 (Rain Sensor)")
        self.geometry("1460x900")
        self.minsize(900, 600)      # ขนาดต่ำสุดที่ยังใช้ได้
        self.resizable(True, True)  # ปรับขนาดได้ทั้งสองแกน

        self._roof_state_cached = "N/A"
        self._roof_state_ts = 0.0
        self._limit_poll_inflight = False


        self.tele_pause_until = 0.0 

        self.msg_q: queue.Queue[str] = queue.Queue()
        self.laser: LaserClient | None = None
        self.is_firing = False
        self.manual_lock = threading.Lock()

        # Telemetry
        self.tele_thread: threading.Thread | None = None
        self.tele_stop = threading.Event()
        self.tele_interval_sec = 2
        self.last_dtemf: float | None = None
        self.last_ltemf: float | None = None
        self.tele_owner_idx: int | None = None  # ติดตามว่า CSV นี้เป็นของโปรแกรมไหน

        # CSV manual (parallel) สำหรับกรณีกด FIRE/STANDBY ระหว่าง Timer
        self.manual_parallel_path: str | None = None
        self._manual_header_written: str | None = None

        # programs list (แต่ละโปรแกรมเก็บตัวแปร/วิดเจ็ต/สถานะของตัวเอง)
        self.programs: list[dict] = []

        # --- Auto sliding roof by scheduler ---
        self.roof_auto_sched_var = tk.BooleanVar(value=True)
        self._prefire_timers = {}
        self._postrest_timers = {}
        # --- Temp & RH Sensor state (ต้องอยู่ก่อน tk vars) ---
        self.sensor_enabled        = True
        self.sensor_api_url        = "http://192.168.49.8:8000/api/sensor"
        self.sensor_api_timeout    = 5.0
        self.sensor_poll_interval  = 5      # วินาที
        self._sensor_poll_stop     = False
        self._sensor_poll_inflight = False
        self._sensor_last_ok_ts: float | None = None
        self._sensor_stale_sec     = 30.0
        # cache ค่าล่าสุด
        self._sensor_in_temp   = 0.0
        self._sensor_in_humi   = 0.0
        self._sensor_in_dew    = 0.0
        self._sensor_out_temp  = 0.0
        self._sensor_out_humi  = 0.0
        self._sensor_out_dew   = 0.0

        # --- Rain sensor UI variables (ประกาศก่อน _build_ui) ---
        self.rain_api_url_var      = tk.StringVar(value=self.rain_api_url)
        self.rain_timeout_var      = tk.DoubleVar(value=self.rain_api_timeout)
        self.rain_stale_var        = tk.DoubleVar(value=self.rain_stale_sec)
        self.rain_interval_var     = tk.IntVar(value=self.rain_poll_interval)
        self.rain_enabled_var      = tk.BooleanVar(value=self.rain_enabled)
        self.rain_status_var       = tk.StringVar(value="Rain: -")
        self.rain_intensity_var    = tk.StringVar(value="-")
        self.rain_total_var        = tk.StringVar(value="-")
        # Temp/RH Sensor UI vars
        self.sensor_enabled_var    = tk.BooleanVar(value=self.sensor_enabled)
        self.sensor_api_url_var    = tk.StringVar(value=self.sensor_api_url)
        self.sensor_timeout_var    = tk.DoubleVar(value=self.sensor_api_timeout)
        self.sensor_interval_var   = tk.IntVar(value=self.sensor_poll_interval)
        self.sensor_stale_var      = tk.DoubleVar(value=self._sensor_stale_sec)
        # display vars
        self.sensor_in_temp_var    = tk.StringVar(value="-")
        self.sensor_in_humi_var    = tk.StringVar(value="-")
        self.sensor_in_dew_var     = tk.StringVar(value="-")
        self.sensor_out_temp_var   = tk.StringVar(value="-")
        self.sensor_out_humi_var   = tk.StringVar(value="-")
        self.sensor_out_dew_var    = tk.StringVar(value="-")
        self.sensor_ts_var         = tk.StringVar(value="-")

        # --- Temp Control state (ต้องประกาศก่อน _build_ui) ---
        self.temp_ctl_enabled   = tk.BooleanVar(value=True)
        self.max_temp_var       = tk.DoubleVar(value=32.5)   # LTEMF threshold
        self.max_dtemf_var      = tk.DoubleVar(value=35.0)   # DTEMF threshold
        self._temp_alarm_active  = False  # LTEMF alarm flag
        self._dtemf_alarm_active = False  # DTEMF alarm flag

        self._batch_stopping = False  # FIX: ต้องเป็น False ตอน startup
        self.roof_auto_var = tk.BooleanVar(value=True)

        # --- roof auto flag (ป้องกัน AttributeError) ---
        self.roof_auto_var = tk.BooleanVar(value=False)
        # default = เปิด safety fire
        self.safety_fire_enabled_var = tk.BooleanVar(value=True)
        self.roof_auto_ctrl_var = tk.BooleanVar(value=True)  # เปิดอัตโนมัติ/ปิดตามสถานะเลเซอร์
        self.roof_preopen_sec = 15  # เปิดก่อน FIRE กี่วินาที
        self.roof_postclose_sec = 3  # ปิดหลัง REST กี่วินาที

        # --- Monday warmup logic ---
        self.monday_warmup_enabled_var = tk.BooleanVar(value=False)
        self.monday_warmup_threshold_var = tk.DoubleVar(value=26.90)
        self.monday_warmup_lead_min_var = tk.IntVar(value=30)
        self._monday_warmup_sent = {}
        self._monday_ready = {}
        self._monday_last_poll = {}

        self._ui_refs = {}
        self._tutorial = None
        self._qsdelay_last_poll = 0.0
        self._dfreq_last_poll = 0.0

        self._patch_messagebox_with_timestamp()

        # Build UI
        self._build_ui()
        # Start roof auto-refresh after UI is ready
        # self.after(1000, lambda: self.roof_toggle_auto() if self.roof_auto_var.get() else None)
        self._init_plots()
        self.after(200, self._drain_logs)
        self.after(500, self._update_clock_and_plot)
        self.after(1000, self._temp_monitor_tick)
        self.after(1000, self._ui_telemetry_tick)

        self.after(1000, self._auto_update_status)

        self.after(1000, self._monitor_roof_during_fire)
        # Rain sensor polling – เริ่มเฉพาะเมื่อ rain_enabled=True
        if getattr(self, 'rain_enabled', True):
            self.after(1000, self._poll_rain_sensor)
        # Temp/RH sensor polling
        if getattr(self, 'sensor_enabled', True):
            self.after(2000, self._poll_sensor)

        self._poll_roof_status()
        # self._monitor_roof_during_fire()

        self.active_program_lock = threading.Lock()
        self.active_program_idx = None

        self._load_config_into_ui()
        if not self.programs:  # อย่างน้อย 1 โปรแกรม
            self.add_program()

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # ---------- UI ----------
    # ------------------------------------------------------------------ #
    #  Scrollable tab helper                                              #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _make_scrollable_tab(tab_frame: "tk.Frame") -> "tk.Frame":
        """
        ครอบ tab_frame ด้วย Canvas + Scrollbar แนวตั้ง
        คืน inner frame ที่ควร pack/grid widget เข้าไป
        Widget ใน inner frame จะ scroll ได้เมื่อเนื้อหาเกินความสูงหน้าต่าง
        """
        canvas = tk.Canvas(tab_frame, highlightthickness=0)
        vsb    = ttk.Scrollbar(tab_frame, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        inner = ttk.Frame(canvas)
        win_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _on_inner_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _on_canvas_configure(event):
            # ขยาย inner frame ให้เต็มความกว้าง canvas เสมอ
            canvas.itemconfigure(win_id, width=event.width)

        def _on_mousewheel(event):
            # Windows/macOS delta; Linux Button-4/5
            if event.num == 4:
                canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                canvas.yview_scroll(1, "units")
            else:
                canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        inner.bind("<Configure>", _on_inner_configure)
        canvas.bind("<Configure>", _on_canvas_configure)

        # bind mouse wheel ทั้ง Windows (<MouseWheel>) และ Linux (<Button-4/5>)
        for seq in ("<MouseWheel>", "<Button-4>", "<Button-5>"):
            canvas.bind(seq, _on_mousewheel)
            inner.bind(seq, _on_mousewheel)

        return inner

    def _build_ui(self):
        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        tab_main    = ttk.Frame(nb)
        tab_cfg     = ttk.Frame(nb)
        tab_network = ttk.Frame(nb)
        tab_conn    = ttk.Frame(nb)
        nb.add(tab_main,    text="Main")
        nb.add(tab_cfg,     text="Settings / Config")
        nb.add(tab_network, text="Network Scanner")
        nb.add(tab_conn,    text="Connection Settings")
        self._nb = nb
        self._tab_main    = tab_main
        self._tab_cfg     = tab_cfg
        self._tab_network = tab_network
        self._tab_conn    = tab_conn

        # Scrollable inner frames สำหรับ tab ที่มีเนื้อหายาว
        tab_cfg_inner     = self._make_scrollable_tab(tab_cfg)
        tab_network_inner = self._make_scrollable_tab(tab_network)
        tab_conn_inner    = self._make_scrollable_tab(tab_conn)

        root = ttk.Frame(tab_main); root.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Connection
        conn = ttk.LabelFrame(root, text="Connection")
        conn.grid(row=0, column=0, columnspan=3, sticky="nwe", padx=5, pady=5)
        btn_connect = ttk.Button(conn, text="Connect", command=self.connect)
        btn_connect.grid(row=0, column=0, padx=5)
        btn_disconnect = ttk.Button(conn, text="Disconnect", command=self.disconnect)
        btn_disconnect.grid(row=0, column=1, padx=5)
        btn_tutorial = ttk.Button(conn, text="Tutorial", command=self._start_tutorial)
        btn_tutorial.grid(row=0, column=2, padx=5)
        self.conn_status = ttk.Label(conn, text="Disconnected", foreground="red")
        self.conn_status.grid(row=0, column=3, padx=10)

        self._ui_refs["connect_btn"] = btn_connect
        self._ui_refs["disconnect_btn"] = btn_disconnect

        # Manual controls
        man = ttk.LabelFrame(root, text="Manual Control")
        man.grid(row=1, column=0, sticky="nwe", padx=5, pady=5)
        btn_fire = ttk.Button(man, text="FIRE", command=self.cmd_fire, width=10)
        btn_fire.grid(row=0, column=0, padx=5, pady=2)
        
        btn_standby = ttk.Button(man, text="STANDBY", command=self.cmd_standby, width=10)
        btn_standby.grid(row=0, column=1, padx=5, pady=2)
        # ttk.Button(man, text="TEMP?", command=self.cmd_temp, width=10).grid(row=0, column=2, padx=5, pady=2)
        btn_stop = ttk.Button(man, text="STOP", command=self.cmd_stop, width=10)
        btn_stop.grid(row=0, column=3, padx=5, pady=2)
        self._ui_refs["manual_frame"] = man
        self._ui_refs["fire_btn"] = btn_fire
        self._ui_refs["standby_btn"] = btn_standby
        self._ui_refs["stop_btn"] = btn_stop

        self.laser_status_var = tk.StringVar(value="Laser: -")
        ttk.Label(conn, textvariable=self.laser_status_var, foreground="blue").grid(
            row=2, column=0, columnspan=9, sticky="w", padx=10, pady=4
        )

        # --- Rain Sensor status display in Main ---
        rain_frm = ttk.LabelFrame(root, text="Rain Sensor")
        rain_frm.grid(row=1, column=2, rowspan=1, sticky="nwe", padx=5, pady=5)
        self._ui_refs["rain_frame"] = rain_frm

        # Row 0: status + intensity + total
        ttk.Label(rain_frm, text="Status:").grid(row=0, column=0, sticky="e", padx=5, pady=4)
        self.rain_status_lbl = ttk.Label(rain_frm, textvariable=self.rain_status_var,
                                          font=("Segoe UI", 10, "bold"), foreground="gray")
        self.rain_status_lbl.grid(row=0, column=1, sticky="w", padx=5, pady=4)

        ttk.Label(rain_frm, text="Intensity:").grid(row=0, column=2, sticky="e", padx=5, pady=4)
        ttk.Label(rain_frm, textvariable=self.rain_intensity_var).grid(row=0, column=3, sticky="w", padx=5, pady=4)

        ttk.Label(rain_frm, text="Total:").grid(row=0, column=4, sticky="e", padx=5, pady=4)
        ttk.Label(rain_frm, textvariable=self.rain_total_var).grid(row=0, column=5, sticky="w", padx=5, pady=4)

        # Row 1: last update timestamp
        ttk.Label(rain_frm, text="Last update:").grid(row=1, column=0, sticky="e", padx=5, pady=2)
        self.rain_ts_var = tk.StringVar(value="-")
        ttk.Label(rain_frm, textvariable=self.rain_ts_var, foreground="gray").grid(
            row=1, column=1, columnspan=4, sticky="w", padx=5, pady=2)


        

        # --- Temp & RH Sensor display in Main (card-based layout) ---
        sensor_frm = ttk.LabelFrame(root, text="Temp & RH Sensor")
        sensor_frm.grid(row=2, column=2, sticky="nwe", padx=5, pady=5)
        self._ui_refs["sensor_frame"] = sensor_frm

        # Section headers
        ttk.Label(sensor_frm, text="Indoor",
                  font=("Segoe UI", 8, "bold"), foreground="gray"
                  ).grid(row=0, column=0, columnspan=3, pady=(6, 2), padx=6)
        ttk.Separator(sensor_frm, orient="vertical"
                      ).grid(row=0, column=3, rowspan=4, sticky="ns", padx=6)
        ttk.Label(sensor_frm, text="Outdoor",
                  font=("Segoe UI", 8, "bold"), foreground="gray"
                  ).grid(row=0, column=4, columnspan=3, pady=(6, 2), padx=6)

        # Helper: one mini-card (label on top, big value, small unit)
        def _sensor_card(parent, label, textvariable, unit, row, col):
            frm = ttk.Frame(parent, relief="groove", borderwidth=1)
            frm.grid(row=row, column=col, padx=4, pady=4, sticky="nswe")
            ttk.Label(frm, text=label,  font=("Segoe UI", 8),
                      foreground="gray").pack(pady=(4, 0))
            ttk.Label(frm, textvariable=textvariable,
                      font=("Segoe UI", 13, "bold"), width=6,
                      anchor="center").pack()
            ttk.Label(frm, text=unit, font=("Segoe UI", 8),
                      foreground="gray").pack(pady=(0, 4))

        _sensor_card(sensor_frm, "Temp",  self.sensor_in_temp_var,  "°C", 1, 0)
        _sensor_card(sensor_frm, "RH",    self.sensor_in_humi_var,  "%",  1, 1)
        _sensor_card(sensor_frm, "Dew",   self.sensor_in_dew_var,   "°C", 1, 2)
        _sensor_card(sensor_frm, "Temp",  self.sensor_out_temp_var, "°C", 1, 4)
        _sensor_card(sensor_frm, "RH",    self.sensor_out_humi_var, "%",  1, 5)
        _sensor_card(sensor_frm, "Dew",   self.sensor_out_dew_var,  "°C", 1, 6)

        # Last update
        ttk.Label(sensor_frm, text="Last update:",
                  font=("Segoe UI", 8)).grid(row=2, column=0, sticky="e", padx=5, pady=(2,6))
        ttk.Label(sensor_frm, textvariable=self.sensor_ts_var,
                  font=("Segoe UI", 8),
                  foreground="gray").grid(row=2, column=1, columnspan=6, sticky="w", padx=2, pady=(2,6))
        # Telemetry
        tele = ttk.LabelFrame(root, text="Telemetry – DTEMF / LTEMF")
        tele.grid(row=1, column=1, sticky="nwe", padx=5, pady=5)
        ttk.Label(tele, text="DTEMF:").grid(row=0, column=0, sticky="e")
        self.lbl_dtemf = ttk.Label(tele, text="-"); self.lbl_dtemf.grid(row=0, column=1, sticky="w", padx=5)
        ttk.Label(tele, text="LTEMF:").grid(row=0, column=2, sticky="e")
        self.lbl_ltemf = ttk.Label(tele, text="-"); self.lbl_ltemf.grid(row=0, column=3, sticky="w", padx=5)
        self.record_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(tele, text="Save CSV", variable=self.record_var, command=self._toggle_telemetry)\
            .grid(row=1, column=0, sticky="w", pady=4)
        ttk.Label(tele, text="File:").grid(row=1, column=1, sticky="e")
        self.csv_name_var = tk.StringVar(value=self._default_csv_name())
        ttk.Entry(tele, textvariable=self.csv_name_var, width=44)\
            .grid(row=1, column=2, columnspan=2, sticky="we", padx=5)
        self._ui_refs["tele_frame"] = tele

        # Setting
        setting = ttk.LabelFrame(root, text="Setting")
        setting.grid(row=2, column=0, sticky="nwe", padx=5, pady=5)
        self._ui_refs["setting_frame"] = setting
        f_qs = ttk.Frame(setting); f_qs.pack(fill=tk.X, pady=2)
        ttk.Label(f_qs, text="QSDELAY (µs):").pack(side=tk.LEFT, padx=5)
        self.qsdelay_var = tk.StringVar(value="220")
        qs_entry = ttk.Entry(f_qs, textvariable=self.qsdelay_var, width=10); qs_entry.pack(side=tk.LEFT)
        self._ui_refs["qs_entry"] = qs_entry
        btn_qs_set = ttk.Button(f_qs, text="Set", command=self.apply_qsdelay); btn_qs_set.pack(side=tk.LEFT, padx=4)
        self.qsdelay_live_var = tk.StringVar(value="-")
        ttk.Label(f_qs, text="Value:").pack(side=tk.LEFT, padx=(8, 2))
        ttk.Label(f_qs, textvariable=self.qsdelay_live_var, width=6).pack(side=tk.LEFT)
        ttk.Label(f_qs, text="recommend: 0 – 400", foreground="gray").pack(side=tk.LEFT, padx=8)
        qs_entry.bind("<Return>", lambda _: self.apply_qsdelay())

        f_df = ttk.Frame(setting); f_df.pack(fill=tk.X, pady=2)
        ttk.Label(f_df, text="Frequency (Hz):").pack(side=tk.LEFT, padx=5)
        self.freq_var = tk.StringVar(value="20")
        fr_entry = ttk.Entry(f_df, textvariable=self.freq_var, width=10); fr_entry.pack(side=tk.LEFT)
        self._ui_refs["df_entry"] = fr_entry
        btn_df_set = ttk.Button(f_df, text="Set", command=self.apply_dfreq); btn_df_set.pack(side=tk.LEFT, padx=4)
        # Live DFREQ value (เหมือน QSDELAY Value:)
        self.dfreq_live_var = tk.StringVar(value="-")
        ttk.Label(f_df, text="Value:").pack(side=tk.LEFT, padx=(8, 2))
        ttk.Label(f_df, textvariable=self.dfreq_live_var, width=6).pack(side=tk.LEFT)
        ttk.Label(f_df, text="recommend: 1 – 22", foreground="gray").pack(side=tk.LEFT, padx=8)
        fr_entry.bind("<Return>", lambda _: self.apply_dfreq())

        ttk.Button(setting, text="Save Settings", command=self.save_config).pack(anchor="e", padx=5, pady=4)
        # ===== Temp Control (วางใน Setting) =====
        tempf = ttk.LabelFrame(setting, text="Temp Control")
        tempf.pack(fill=tk.X, padx=4, pady=4)
        self._ui_refs["temp_frame"] = tempf

        temp_enable = ttk.Checkbutton(tempf, text="Enable", variable=self.temp_ctl_enabled)
        temp_enable.grid(row=0, column=0, padx=5, pady=5, sticky="w")

        # LTEMF row
        ttk.Label(tempf, text="Max LTEMF (°C):")\
            .grid(row=0, column=1, padx=5, pady=5, sticky="e")
        temp_max = ttk.Entry(tempf, textvariable=self.max_temp_var, width=8)
        temp_max.grid(row=0, column=2, padx=5, pady=5, sticky="w")
        ttk.Label(tempf, text="(Laser internal — STANDBY if exceeded)",
                  foreground="gray").grid(row=0, column=3, padx=5, sticky="w")

        # DTEMF row
        ttk.Label(tempf, text="Max DTEMF (°C):")\
            .grid(row=1, column=1, padx=5, pady=5, sticky="e")
        temp_dmax = ttk.Entry(tempf, textvariable=self.max_dtemf_var, width=8)
        temp_dmax.grid(row=1, column=2, padx=5, pady=5, sticky="w")
        ttk.Label(tempf, text="(Diode temp — STANDBY if exceeded)",
                  foreground="gray").grid(row=1, column=3, padx=5, sticky="w")

        self._ui_refs["temp_enable"] = temp_enable
        self._ui_refs["temp_max"]    = temp_max
        self._ui_refs["temp_dmax"]   = temp_dmax

        # ---- Control Sliding Roof (under Setting) ----
        roof_group = ttk.LabelFrame(setting, text="Control Sliding Roof")
        roof_group.pack(fill=tk.X, padx=4, pady=4)
        self._ui_refs["roof_frame"] = roof_group
        # roof_group.grid(row=3, column=0, sticky="nwe", padx=4, pady=4)

        frm_roof = ttk.Frame(roof_group); frm_roof.pack(fill=tk.X, padx=6, pady=6)

        btn_roof_open = ttk.Button(frm_roof, text="Open", width=16,
                   command=self.roof_open)
        btn_roof_open.pack(side=tk.LEFT, padx=3)
        btn_roof_close = ttk.Button(frm_roof, text="Close", width=16,
                   command=self.roof_close)
        btn_roof_close.pack(side=tk.LEFT, padx=3)
        # ttk.Button(frm_roof, text="Refresh status", width=18,
        #            command=self.roof_refresh).pack(side=tk.LEFT, padx=3)

        # self.roof_status_var = tk.StringVar(value="Status: -")
        # ttk.Label(frm_roof, textvariable=self.roof_status_var).pack(side=tk.LEFT, padx=10)
        # ttk.Label(f_roof, text="Status: ").pack(side=tk.LEFT, padx=(12,0))

        # self.roof_status_var = tk.StringVar(value="UNKNOWN")
        self.roof_status_var = tk.StringVar(value="N/A")

        self.roof_status_lbl = ttk.Label(frm_roof, textvariable=self.roof_status_var)
        self.roof_status_lbl.pack(side=tk.LEFT, padx=2)
        self._ui_refs["roof_open_btn"] = btn_roof_open
        self._ui_refs["roof_close_btn"] = btn_roof_close
        self._ui_refs["roof_status_lbl"] = self.roof_status_lbl

        self.roof_auto_sched_cb = ttk.Checkbutton(
            frm_roof,
            text="Enable auto open (T-15s) / auto close (+3s)",
            variable=self.roof_auto_sched_var,
        )
        self.roof_auto_sched_cb.pack(side=tk.RIGHT, padx=6)
        self._update_roof_auto_label()
        self._ui_refs["roof_auto_cb"] = self.roof_auto_sched_cb

        # self.roof_auto_var = tk.BooleanVar(value=True)
        # ttk.Checkbutton(frm_roof, text="Auto-refresh (5s)",
        #                 variable=self.roof_auto_var,
        #                 command=self.roof_toggle_auto).pack(side=tk.RIGHT, padx=3)

        # Programs group
        prog_box = ttk.LabelFrame(root, text="Scheduled Programs")
        prog_box.grid(row=2, column=1, columnspan=1, sticky="nwe", padx=5, pady=5)
        self._ui_refs["programs_frame"] = prog_box

        toolbar = ttk.Frame(prog_box); toolbar.pack(fill=tk.X, pady=3)
        btn_add_prog = ttk.Button(toolbar, text="+ Add Program", command=self.add_program)
        btn_add_prog.pack(side=tk.LEFT, padx=4)
        btn_start_all = ttk.Button(toolbar, text="Start All", command=self.start_all)
        btn_start_all.pack(side=tk.LEFT, padx=4)
        btn_stop_all = ttk.Button(toolbar, text="Stop All", command=self.stop_all_programs)
        btn_stop_all.pack(side=tk.LEFT, padx=4)

        btn_remove_all = ttk.Button(toolbar, text="Remove All", command=self.remove_all_programs)
        btn_remove_all.pack(side=tk.LEFT, padx=4)
        self._ui_refs["add_program_btn"] = btn_add_prog
        self._ui_refs["start_all_btn"] = btn_start_all
        self._ui_refs["stop_all_btn"] = btn_stop_all
        self._ui_refs["remove_all_btn"] = btn_remove_all

        # 👇 นาฬิกามุมขวา
        self.clock_var = tk.StringVar(value="Time: -")
        ttk.Label(toolbar, textvariable=self.clock_var).pack(side=tk.RIGHT, padx=6)

        self.prog_nb = ttk.Notebook(prog_box); self.prog_nb.pack(fill=tk.BOTH, expand=True)

        # Plots + Logs
        vis = ttk.Panedwindow(root, orient=tk.HORIZONTAL); vis.grid(row=3, column=0, columnspan=3, sticky="nswe", padx=5, pady=5)
        self.plot_frame = ttk.LabelFrame(vis, text="Realtime Charts")
        logs_container = ttk.LabelFrame(vis, text="Logs")
        vis.add(self.plot_frame, weight=3); vis.add(logs_container, weight=2)
        self._ui_refs["charts_frame"] = self.plot_frame
        self._ui_refs["logs_frame"] = logs_container
        nb = ttk.Notebook(logs_container); nb.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        tab_all = ttk.Frame(nb); nb.add(tab_all, text="All except Schedule")
        ttk.Button(tab_all, text="Clear", command=self.clear_terminal).pack(anchor="ne", padx=6, pady=4)
        self.log_text = tk.Text(tab_all, height=16); self.log_text.pack(fill=tk.BOTH, expand=True)

        tab_sched = ttk.Frame(nb); nb.add(tab_sched, text="Schedule Logs")
        ttk.Button(tab_sched, text="Clear", command=self.clear_sched_terminal).pack(anchor="ne", padx=6, pady=4)
        self.sched_log_text = tk.Text(tab_sched, height=16); self.sched_log_text.pack(fill=tk.BOTH, expand=True)

        # ---------- Terminal Tab ----------
        tab_term = ttk.Frame(nb); nb.add(tab_term, text="Terminal")
        self._build_terminal_tab(tab_term)

        self._build_config_tab(tab_cfg_inner)
        self._build_network_tab(tab_network_inner)
        self._build_conn_settings_tab(tab_conn_inner)

        # Responsive column weights: left panel | right panel | rain sensor
        # col0=left panel, col1=right panel (programs+logs), col2=sensors
        root.columnconfigure(0, weight=2, minsize=320)
        root.columnconfigure(1, weight=5)
        root.columnconfigure(2, weight=2, minsize=280)
        root.rowconfigure(3, weight=1)  # vis/charts row expands

    # ------------------------------------------------------------------ #
    #  Terminal Tab                                                        #
    # ------------------------------------------------------------------ #
    def _build_terminal_tab(self, parent):
        """Tab: Manual Command Terminal – ส่งคำสั่งตรงไปยังเลเซอร์"""
        self._term_history: list[str] = []
        self._term_hist_idx: int = -1

        out_frame = ttk.Frame(parent)
        out_frame.pack(fill=tk.BOTH, expand=True, padx=6, pady=(6, 2))

        self.term_text = tk.Text(
            out_frame, height=14, state="disabled",
            bg="#1e1e1e", fg="#d4d4d4", insertbackground="white",
            font=("Consolas", 10),
        )
        self.term_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(out_frame, command=self.term_text.yview)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.term_text.configure(yscrollcommand=sb.set)

        self.term_text.tag_configure("cmd",  foreground="#9cdcfe")
        self.term_text.tag_configure("resp", foreground="#b5cea8")
        self.term_text.tag_configure("err",  foreground="#f44747")
        self.term_text.tag_configure("info", foreground="#dcdcaa")

        quick_frame = ttk.LabelFrame(parent, text="Quick Commands")
        quick_frame.pack(fill=tk.X, padx=6, pady=2)
        quick_cmds = [
            ("STATUS",  "$STATUS ?"), ("DTEMF",   "$DTEMF ?"),
            ("LTEMF",   "$LTEMF ?"),  ("QSDELAY", "$QSDELAY ?"),
            ("DFREQ",   "$DFREQ ?"),  ("PARA",    "$PARA ?"),
            ("FPARA",   "$FPARA ?"),  ("TEXTS",   "$TEXTS ?"),
            ("HELP",    "$HELP ?"),   ("HOURS",   "$HOURS ?"),
        ]
        for i, (label, cmd) in enumerate(quick_cmds):
            ttk.Button(quick_frame, text=label, width=9,
                       command=lambda c=cmd: self._term_send(c),
                       ).grid(row=0, column=i, padx=2, pady=4)

        inp_frame = ttk.Frame(parent)
        inp_frame.pack(fill=tk.X, padx=6, pady=(2, 6))
        ttk.Label(inp_frame, text="Command:").pack(side=tk.LEFT, padx=(0, 4))
        self.term_cmd_var = tk.StringVar()
        self._term_entry = ttk.Entry(inp_frame, textvariable=self.term_cmd_var,
                                     font=("Consolas", 10))
        self._term_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))
        ttk.Button(inp_frame, text="Send",  width=8,
                   command=self._term_send_from_entry).pack(side=tk.LEFT, padx=2)
        ttk.Button(inp_frame, text="Clear", width=8,
                   command=self._term_clear).pack(side=tk.LEFT, padx=2)

        self._term_entry.bind("<Return>", lambda _: self._term_send_from_entry())
        self._term_entry.bind("<Up>",     self._term_hist_up)
        self._term_entry.bind("<Down>",   self._term_hist_down)

        self._term_print("info", "Terminal ready. Type a command (e.g. $STATUS ?) and press Enter or Send.")
        self._term_print("info", "Arrow Up/Down เลือกคำสั่งที่เคยส่ง  |  Quick buttons ด้านบนสำหรับคำสั่งที่ใช้บ่อย")

    def _term_print(self, tag: str, text: str):
        def _insert():
            self.term_text.configure(state="normal")
            ts = datetime.now(TZ).strftime("%H:%M:%S")
            self.term_text.insert(tk.END, f"[{ts}] {text}\n", tag)
            self.term_text.configure(state="disabled")
            self.term_text.see(tk.END)
        self.after(0, _insert)

    def _term_clear(self):
        self.term_text.configure(state="normal")
        self.term_text.delete("1.0", tk.END)
        self.term_text.configure(state="disabled")

    def _term_send_from_entry(self):
        cmd = self.term_cmd_var.get().strip()
        if not cmd:
            return
        self.term_cmd_var.set("")
        self._term_send(cmd)

    def _term_send(self, cmd: str):
        if not self.laser:
            self._term_print("err", "Not connected. Please connect first.")
            return
        if not self._term_history or self._term_history[-1] != cmd:
            self._term_history.append(cmd)
        self._term_hist_idx = -1
        self._term_print("cmd", f">>> {cmd}")

        def worker():
            try:
                resp = self.laser.send_cmd(cmd)
                if resp:
                    self._term_print("resp", f"    {resp}")
                else:
                    self._term_print("info", "    (no response)")
            except Exception as e:
                self._term_print("err", f"    ERROR: {e}")

        threading.Thread(target=worker, daemon=True).start()

    def _term_hist_up(self, event):
        if not self._term_history:
            return
        if self._term_hist_idx == -1:
            self._term_hist_idx = len(self._term_history) - 1
        else:
            self._term_hist_idx = max(0, self._term_hist_idx - 1)
        self.term_cmd_var.set(self._term_history[self._term_hist_idx])
        self._term_entry.icursor(tk.END)

    def _term_hist_down(self, event):
        if not self._term_history or self._term_hist_idx == -1:
            return
        if self._term_hist_idx < len(self._term_history) - 1:
            self._term_hist_idx += 1
            self.term_cmd_var.set(self._term_history[self._term_hist_idx])
        else:
            self._term_hist_idx = -1
            self.term_cmd_var.set("")
        self._term_entry.icursor(tk.END)

    def _build_config_tab(self, parent):
        """Tab 2: กำหนด roof_api_base, limit_api_url และ logs directory"""
        parent.columnconfigure(0, weight=1)
        parent.columnconfigure(1, weight=1)

        conn_cfg = ttk.LabelFrame(parent, text="Laser Connection Settings")
        conn_cfg.grid(row=0, column=0, columnspan=2, sticky="nwe", padx=10, pady=10)
        conn_cfg.columnconfigure(1, weight=1)

        self.ip_var = tk.StringVar(value="127.0.0.1")
        self.port_var = tk.IntVar(value=2323)
        self.user_var = tk.StringVar(value="VR70AB07")

        ttk.Label(conn_cfg, text="IP").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ip_entry = ttk.Entry(conn_cfg, textvariable=self.ip_var, width=20)
        ip_entry.grid(row=0, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(conn_cfg, text="Port").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        port_entry = ttk.Entry(conn_cfg, textvariable=self.port_var, width=10)
        port_entry.grid(row=1, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(conn_cfg, text="User").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        user_entry = ttk.Entry(conn_cfg, textvariable=self.user_var, width=16)
        user_entry.grid(row=2, column=1, sticky="w", padx=6, pady=6)

        self._ui_refs["ip_entry"] = ip_entry
        self._ui_refs["port_entry"] = port_entry
        self._ui_refs["user_entry"] = user_entry

        self.roof_api_base_var = tk.StringVar(value=str(getattr(self, "roof_api_base", "")))
        self.limit_api_url_var = tk.StringVar(value=str(getattr(self, "limit_api_url", "")))
        self.log_dir_var = tk.StringVar(value=str(getattr(self, "log_dir", LOG_DIR)))

        # ---- [LEFT COL 0] Roof Settings ----
        roof_lf = ttk.LabelFrame(parent, text="Roof Settings")
        roof_lf.grid(row=1, column=0, sticky="nwe", padx=10, pady=10)
        roof_lf.columnconfigure(1, weight=1)

        ttk.Label(roof_lf, text="roof_api_base").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(roof_lf, textvariable=self.roof_api_base_var, width=55).grid(row=0, column=1, sticky="we", padx=6, pady=6)

        ttk.Label(roof_lf, text="limit_api_url").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(roof_lf, textvariable=self.limit_api_url_var, width=55).grid(row=1, column=1, sticky="we", padx=6, pady=6)

        self.prefire_open_sec_var = tk.DoubleVar(value=float(getattr(self, "roof_preopen_sec", 15)))
        self.postrest_close_sec_var = tk.DoubleVar(value=float(getattr(self, "roof_postclose_sec", 3)))

        ttk.Label(roof_lf, text="Pre-open lead (sec)").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(roof_lf, textvariable=self.prefire_open_sec_var, width=12).grid(row=2, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(roof_lf, text="Post-close delay (sec)").grid(row=3, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(roof_lf, textvariable=self.postrest_close_sec_var, width=12).grid(row=3, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(roof_lf, text="Used by auto open/close around FIRE/REST", foreground="gray")\
            .grid(row=4, column=0, columnspan=2, sticky="w", padx=6, pady=(0, 6))

        # ---- [RIGHT COL 1] Logs & System Settings ----
        logs_lf = ttk.LabelFrame(parent, text="Logs & System Settings")
        logs_lf.grid(row=1, column=1, sticky="nwe", padx=10, pady=10)
        logs_lf.columnconfigure(1, weight=1)

        ttk.Label(logs_lf, text="Logs directory").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(logs_lf, textvariable=self.log_dir_var, width=55).grid(row=0, column=1, sticky="we", padx=6, pady=6)

        def browse_dir():
            try:
                d = filedialog.askdirectory(title="Select logs directory")
                if d:
                    self.log_dir_var.set(d)
            except Exception:
                pass

        ttk.Button(logs_lf, text="Browse", command=browse_dir).grid(row=0, column=2, padx=6, pady=6)

        if not hasattr(self, "safety_fire_enabled_var"):
            self.safety_fire_enabled_var = tk.BooleanVar(value=bool(getattr(self, "safety_fire_enabled", True)))

        def _on_toggle_safety():
            self.safety_fire_enabled = bool(self.safety_fire_enabled_var.get())
            try:
                self.log(f"Safety Fire = {'ON' if self.safety_fire_enabled else 'OFF'}")
            except Exception:
                pass

        ttk.Checkbutton(
            logs_lf,
            text="Enable Safety Fire (Block FIRE when Roof != ON)",
            variable=self.safety_fire_enabled_var,
            command=_on_toggle_safety
        ).grid(row=1, column=0, columnspan=3, sticky="w", padx=6, pady=6)

        ttk.Checkbutton(
            logs_lf,
            text="Enable Monday STANDBY warmup logic",
            variable=self.monday_warmup_enabled_var
        ).grid(row=2, column=0, columnspan=3, sticky="w", padx=6, pady=(0, 4))

        ttk.Label(logs_lf, text="Warmup lead (min)").grid(row=3, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(logs_lf, textvariable=self.monday_warmup_lead_min_var, width=12).grid(row=3, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(logs_lf, text="DTEMF ready >= (°C)").grid(row=4, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(logs_lf, textvariable=self.monday_warmup_threshold_var, width=12).grid(row=4, column=1, sticky="w", padx=6, pady=6)

        # ---- [LEFT COL 0] Temp & RH Sensor Settings ----
        sensor_lf = ttk.LabelFrame(parent, text="Temp & RH Sensor Settings")
        sensor_lf.grid(row=2, column=0, sticky="nwe", padx=10, pady=(0, 10))
        sensor_lf.columnconfigure(1, weight=1)
        sensor_lf.columnconfigure(2, weight=1)

        def _toggle_sensor_from_settings():
            self._apply_sensor_enabled(bool(self.sensor_enabled_var.get()))
        ttk.Checkbutton(sensor_lf, text="Enable Temp & RH Sensor",
                         variable=self.sensor_enabled_var,
                         command=_toggle_sensor_from_settings,
        ).grid(row=0, column=0, columnspan=3, sticky="w", padx=6, pady=(6, 2))

        ttk.Label(sensor_lf, text="sensor_api_url:").grid(row=1, column=0, sticky="w", padx=6, pady=5)
        ttk.Entry(sensor_lf, textvariable=self.sensor_api_url_var, width=50).grid(row=1, column=1, sticky="we", padx=4, pady=5)
        ttk.Label(sensor_lf, text="(e.g. http://192.168.49.8:8000/api/sensor)", foreground="gray").grid(row=1, column=2, sticky="w", padx=6)

        ttk.Label(sensor_lf, text="Request timeout (s):").grid(row=2, column=0, sticky="w", padx=6, pady=5)
        ttk.Entry(sensor_lf, textvariable=self.sensor_timeout_var, width=8).grid(row=2, column=1, sticky="w", padx=4, pady=5)
        ttk.Label(sensor_lf, text="HTTP timeout ต่อ request", foreground="gray").grid(row=2, column=2, sticky="w", padx=6)

        ttk.Label(sensor_lf, text="Poll interval (s):").grid(row=3, column=0, sticky="w", padx=6, pady=5)
        ttk.Entry(sensor_lf, textvariable=self.sensor_interval_var, width=8).grid(row=3, column=1, sticky="w", padx=4, pady=5)
        ttk.Label(sensor_lf, text="ความถี่ดึงข้อมูล (แนะนำ 5–30 วินาที)", foreground="gray").grid(row=3, column=2, sticky="w", padx=6)

        ttk.Label(sensor_lf, text="Stale threshold (s):").grid(row=4, column=0, sticky="w", padx=6, pady=5)
        ttk.Entry(sensor_lf, textvariable=self.sensor_stale_var, width=8).grid(row=4, column=1, sticky="w", padx=4, pady=5)
        ttk.Label(sensor_lf, text="นานแค่ไหนก่อนแสดง N/A เมื่ออ่านไม่ได้", foreground="gray").grid(row=4, column=2, sticky="w", padx=6)

        # ---- [LEFT COL 0] Rain Sensor Settings ----
        rain_lf = ttk.LabelFrame(parent, text="Rain Sensor Settings")
        rain_lf.grid(row=3, column=0, sticky="nwe", padx=10, pady=(0, 10))
        rain_lf.columnconfigure(1, weight=1)

        def _toggle_rain_from_settings():
            self._apply_rain_enabled(bool(self.rain_enabled_var.get()))
        ttk.Checkbutton(
            rain_lf, text="Enable Rain Sensor",
            variable=self.rain_enabled_var,
            command=_toggle_rain_from_settings,
        ).grid(row=0, column=0, columnspan=3, sticky="w", padx=6, pady=(8, 2))

        ttk.Label(rain_lf, text="rain_api_url").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(rain_lf, textvariable=self.rain_api_url_var, width=50).grid(row=1, column=1, sticky="we", padx=6, pady=6)
        ttk.Label(rain_lf, text="(e.g. http://192.168.3.150:8000/api/rain)",
                  foreground="gray").grid(row=1, column=2, padx=6, sticky="w")

        rain_timing = ttk.LabelFrame(rain_lf, text="Rain Sensor Timing")
        rain_timing.grid(row=2, column=0, columnspan=3, sticky="we", padx=6, pady=(4, 6))
        rain_timing.columnconfigure(2, weight=1)

        ttk.Label(rain_timing, text="Request timeout (s):").grid(row=0, column=0, sticky="w", padx=6, pady=5)
        ttk.Entry(rain_timing, textvariable=self.rain_timeout_var, width=8).grid(row=0, column=1, sticky="w", padx=4, pady=5)
        ttk.Label(rain_timing, text="HTTP timeout ต่อ request (แนะนำ 1.5–5)",
                  foreground="gray").grid(row=0, column=2, sticky="w", padx=6)

        ttk.Label(rain_timing, text="Stale threshold (s):").grid(row=1, column=0, sticky="w", padx=6, pady=5)
        ttk.Entry(rain_timing, textvariable=self.rain_stale_var, width=8).grid(row=1, column=1, sticky="w", padx=4, pady=5)
        ttk.Label(rain_timing, text="นานแค่ไหนก่อนแสดง N/A เมื่ออ่านไม่ได้ (แนะนำ 5–30)",
                  foreground="gray").grid(row=1, column=2, sticky="w", padx=6)

        ttk.Label(rain_timing, text="Poll interval (s):").grid(row=2, column=0, sticky="w", padx=6, pady=5)
        ttk.Entry(rain_timing, textvariable=self.rain_interval_var, width=8).grid(row=2, column=1, sticky="w", padx=4, pady=5)
        ttk.Label(rain_timing, text="ความถี่ดึงข้อมูล (แนะนำ 1–5 วินาที)",
                  foreground="gray").grid(row=2, column=2, sticky="w", padx=6)

        btns = ttk.Frame(parent)
        btns.grid(row=3, column=1, sticky="se", padx=10, pady=(0, 10))
        ttk.Button(btns, text="Apply & Save", command=self._apply_and_save_config).pack(side=tk.RIGHT, padx=4)

    # ------------------------------------------------------------------ #
    #  Network Scanner Tab                                                 #
    # ------------------------------------------------------------------ #
    def _build_network_tab(self, parent):
        """Tab: Network Scanner — scan หาอุปกรณ์ใน subnet แล้วลอง Telnet port"""
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)

        # ---- Top controls ----
        ctrl = ttk.LabelFrame(parent, text="Scan Settings")
        ctrl.grid(row=0, column=0, sticky="nwe", padx=10, pady=(10, 4))
        ctrl.columnconfigure(3, weight=1)

        ttk.Label(ctrl, text="Subnet (e.g. 192.168.1):").grid(
            row=0, column=0, padx=8, pady=8, sticky="w")
        self._scan_subnet_var = tk.StringVar(value="192.168.1")
        ttk.Entry(ctrl, textvariable=self._scan_subnet_var, width=18).grid(
            row=0, column=1, padx=4, pady=8, sticky="w")

        ttk.Label(ctrl, text="Port:").grid(row=0, column=2, padx=(12, 4), sticky="w")
        self._scan_port_var = tk.IntVar(value=23)
        ttk.Entry(ctrl, textvariable=self._scan_port_var, width=7).grid(
            row=0, column=3, padx=4, sticky="w")

        ttk.Label(ctrl, text="Timeout (s):").grid(row=0, column=4, padx=(12, 4), sticky="w")
        self._scan_timeout_var = tk.DoubleVar(value=0.3)
        ttk.Entry(ctrl, textvariable=self._scan_timeout_var, width=6).grid(
            row=0, column=5, padx=4, sticky="w")

        ttk.Label(ctrl, text="Range:").grid(row=0, column=6, padx=(12, 4), sticky="w")
        self._scan_start_var = tk.IntVar(value=1)
        self._scan_end_var   = tk.IntVar(value=254)
        ttk.Entry(ctrl, textvariable=self._scan_start_var, width=5).grid(
            row=0, column=7, padx=2, sticky="w")
        ttk.Label(ctrl, text="–").grid(row=0, column=8)
        ttk.Entry(ctrl, textvariable=self._scan_end_var, width=5).grid(
            row=0, column=9, padx=2, sticky="w")

        self._scan_btn = ttk.Button(ctrl, text="▶  Start Scan", command=self._net_scan_start)
        self._scan_btn.grid(row=0, column=10, padx=(16, 8), pady=8)

        self._scan_stop_btn = ttk.Button(ctrl, text="■  Stop", state="disabled",
                                          command=self._net_scan_stop)
        self._scan_stop_btn.grid(row=0, column=11, padx=(0, 8), pady=8)

        # ---- progress + status ----
        prog_frame = ttk.Frame(parent)
        prog_frame.grid(row=1, column=0, sticky="we", padx=10, pady=2)
        prog_frame.columnconfigure(0, weight=1)

        self._scan_progress = ttk.Progressbar(prog_frame, mode="determinate", length=400)
        self._scan_progress.grid(row=0, column=0, sticky="we", padx=(0, 8))
        self._scan_status_var = tk.StringVar(value="Ready")
        ttk.Label(prog_frame, textvariable=self._scan_status_var, width=36).grid(
            row=0, column=1, sticky="w")

        # ---- results table ----
        result_frame = ttk.LabelFrame(parent, text="Found Devices")
        result_frame.grid(row=2, column=0, sticky="nswe", padx=10, pady=(4, 10))
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)
        parent.rowconfigure(2, weight=1)

        cols = ("ip", "hostname", "port", "ping_ms", "action")
        self._scan_tree = ttk.Treeview(result_frame, columns=cols,
                                        show="headings", height=14)
        for col, w, txt in [
            ("ip",       140, "IP Address"),
            ("hostname", 200, "Hostname"),
            ("port",      60, "Port"),
            ("ping_ms",   80, "Latency (ms)"),
            ("action",   120, ""),
        ]:
            self._scan_tree.heading(col, text=txt)
            self._scan_tree.column(col, width=w, minwidth=w)

        vsb = ttk.Scrollbar(result_frame, orient="vertical",
                             command=self._scan_tree.yview)
        self._scan_tree.configure(yscrollcommand=vsb.set)
        self._scan_tree.grid(row=0, column=0, sticky="nswe")
        vsb.grid(row=0, column=1, sticky="ns")

        # double-click → ใช้ IP นี้
        self._scan_tree.bind("<Double-1>", self._net_scan_use_selected)

        btn_row = ttk.Frame(result_frame)
        btn_row.grid(row=1, column=0, columnspan=2, sticky="e", padx=4, pady=4)
        ttk.Button(btn_row, text="Use Selected IP",
                   command=self._net_scan_use_selected).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_row, text="Clear Results",
                   command=self._net_scan_clear).pack(side=tk.LEFT, padx=4)
        ttk.Label(btn_row, text="Double-click a row to apply IP",
                  foreground="gray").pack(side=tk.LEFT, padx=8)

        # internal state
        self._scan_running   = False
        self._scan_stop_flag = threading.Event()

    # -- scan helpers --
    def _net_scan_clear(self):
        for row in self._scan_tree.get_children():
            self._scan_tree.delete(row)

    def _net_scan_start(self):
        if self._scan_running:
            return
        subnet  = self._scan_subnet_var.get().strip()
        port    = int(self._scan_port_var.get())
        timeout = float(self._scan_timeout_var.get())
        start   = max(1,   int(self._scan_start_var.get()))
        end     = min(254, int(self._scan_end_var.get()))

        if not subnet:
            messagebox.showwarning("Scan", "Please enter a subnet (e.g. 192.168.1)")
            return

        self._net_scan_clear()
        self._scan_running = True
        self._scan_stop_flag.clear()
        self._scan_btn.config(state="disabled")
        self._scan_stop_btn.config(state="normal")
        total = end - start + 1
        self._scan_progress.config(maximum=total, value=0)
        self._scan_status_var.set(f"Scanning {subnet}.{start} – {subnet}.{end} ...")

        def worker():
            found = 0
            for i, last in enumerate(range(start, end + 1)):
                if self._scan_stop_flag.is_set():
                    break
                ip = f"{subnet}.{last}"
                t0 = time.monotonic()
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(timeout)
                    s.connect((ip, port))
                    s.close()
                    elapsed = int((time.monotonic() - t0) * 1000)
                    # resolve hostname (best-effort)
                    try:
                        hostname = socket.gethostbyaddr(ip)[0]
                    except Exception:
                        hostname = "-"
                    self.after(0, lambda _ip=ip, _h=hostname, _p=port, _ms=elapsed:
                                self._scan_tree.insert("", tk.END,
                                    values=(_ip, _h, _p, f"{_ms} ms",
                                            "⇒ Double-click to use")))
                    found += 1
                except Exception:
                    pass

                # update progress every 5 hosts
                if i % 5 == 0 or last == end:
                    self.after(0, lambda v=i+1, f=found:
                               (self._scan_progress.config(value=v),
                                self._scan_status_var.set(
                                    f"Scanned {v}/{total}  |  Found {f} device(s)")))

            self.after(0, self._net_scan_done)

        threading.Thread(target=worker, daemon=True).start()

    def _net_scan_stop(self):
        self._scan_stop_flag.set()

    def _net_scan_done(self):
        self._scan_running = False
        self._scan_btn.config(state="normal")
        self._scan_stop_btn.config(state="disabled")
        found = len(self._scan_tree.get_children())
        self._scan_status_var.set(f"Done — {found} device(s) found")

    def _net_scan_use_selected(self, event=None):
        sel = self._scan_tree.selection()
        if not sel:
            # ลอง selection จาก double-click
            item = self._scan_tree.identify_row(
                getattr(event, "y", 0)) if event else None
            if item:
                sel = (item,)
        if not sel:
            messagebox.showinfo("Select", "Please select a device from the list.")
            return
        ip = self._scan_tree.item(sel[0])["values"][0]
        port = self._scan_tree.item(sel[0])["values"][2]
        # apply to Connection Settings tab
        if hasattr(self, "ip_var"):
            self.ip_var.set(str(ip))
        if hasattr(self, "port_var"):
            try:
                self.port_var.set(int(port))
            except Exception:
                pass
        self.log(f"Network Scanner → applied IP={ip}, Port={port}")
        messagebox.showinfo("Applied",
                            f"IP: {ip}\nPort: {port}\n\nApplied to Connection Settings.\n"
                            "Click Connect on Main tab to connect.")

    # ------------------------------------------------------------------ #
    #  Connection Settings Tab                                             #
    # ------------------------------------------------------------------ #
    def _build_conn_settings_tab(self, parent):
        """Tab: Connection Settings — IP, Port, User/Login, Password"""
        parent.columnconfigure(0, weight=1)

        # ---- Laser Connection ----
        lf_conn = ttk.LabelFrame(parent, text="Laser Connection")
        lf_conn.grid(row=0, column=0, sticky="nwe", padx=12, pady=(12, 6))
        lf_conn.columnconfigure(1, weight=1)

        self._cs_ip_var   = tk.StringVar()
        self._cs_port_var = tk.StringVar()
        self._cs_user_var = tk.StringVar()
        self._cs_pass_var = tk.StringVar()

        # sync ค่าเริ่มต้นจาก vars หลัก (ถ้ามี)
        def _sync_from_main():
            self._cs_ip_var.set(getattr(self, "ip_var",
                                        tk.StringVar(value="")).get() if hasattr(self, "ip_var") else "")
            self._cs_port_var.set(str(getattr(self, "port_var",
                                              tk.IntVar(value=23)).get()) if hasattr(self, "port_var") else "23")
            self._cs_user_var.set(getattr(self, "user_var",
                                          tk.StringVar(value="")).get() if hasattr(self, "user_var") else "")

        _sync_from_main()

        fields = [
            ("IP Address",  self._cs_ip_var,   False, 22),
            ("Port",        self._cs_port_var,  False, 8),
            ("Login User",  self._cs_user_var,  False, 20),
            ("Password",    self._cs_pass_var,  True,  20),
        ]
        self._cs_pass_entry = None
        for r, (label, var, is_pass, w) in enumerate(fields):
            ttk.Label(lf_conn, text=label + ":").grid(
                row=r, column=0, sticky="w", padx=10, pady=7)
            kw = {"show": "●"} if is_pass else {}
            ent = ttk.Entry(lf_conn, textvariable=var, width=w, **kw)
            ent.grid(row=r, column=1, sticky="w", padx=6, pady=7)
            if is_pass:
                self._cs_pass_entry = ent
                # toggle show/hide
                self._cs_show_pass = tk.BooleanVar(value=False)
                def _toggle_pass(e=ent, v=self._cs_show_pass):
                    e.config(show="" if v.get() else "●")
                ttk.Checkbutton(lf_conn, text="Show", variable=self._cs_show_pass,
                                command=_toggle_pass).grid(
                    row=r, column=2, padx=6, sticky="w")

        ttk.Label(lf_conn, text="Login format:  $LOGIN <user>  (auto on connect)",
                  foreground="gray").grid(
            row=len(fields), column=0, columnspan=3, sticky="w", padx=10, pady=(0, 8))

        # ---- Buttons ----
        btn_frame = ttk.Frame(lf_conn)
        btn_frame.grid(row=len(fields)+1, column=0, columnspan=3,
                       sticky="w", padx=8, pady=(0, 10))

        ttk.Button(btn_frame, text="Apply & Save",
                   command=self._cs_apply_save).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_frame, text="Test Connection",
                   command=self._cs_test_connect).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_frame, text="Connect Now",
                   command=self._cs_connect_now).pack(side=tk.LEFT, padx=4)

        self._cs_status_var = tk.StringVar(value="")
        ttk.Label(lf_conn, textvariable=self._cs_status_var,
                  foreground="blue").grid(
            row=len(fields)+2, column=0, columnspan=3,
            sticky="w", padx=10, pady=(0, 8))

        # ---- Quick Presets ----
        lf_preset = ttk.LabelFrame(parent, text="Quick Presets")
        lf_preset.grid(row=1, column=0, sticky="nwe", padx=12, pady=6)

        presets = [
            ("Viron Default",  "192.168.103.103", 23),
            ("Localhost Test", "127.0.0.1",        23),
        ]
        for col, (name, ip, port) in enumerate(presets):
            ttk.Button(
                lf_preset, text=name,
                command=lambda i=ip, p=port: self._cs_apply_preset(i, p),
                width=18,
            ).grid(row=0, column=col, padx=8, pady=8)

        # ---- Saved Profiles ----
        lf_prof = ttk.LabelFrame(parent, text="Saved Profiles")
        lf_prof.grid(row=2, column=0, sticky="nwe", padx=12, pady=6)
        lf_prof.columnconfigure(0, weight=1)

        prof_top = ttk.Frame(lf_prof); prof_top.pack(fill=tk.X, padx=6, pady=4)
        ttk.Label(prof_top, text="Profile name:").pack(side=tk.LEFT)
        self._cs_prof_name_var = tk.StringVar(value="Profile 1")
        ttk.Entry(prof_top, textvariable=self._cs_prof_name_var, width=18).pack(
            side=tk.LEFT, padx=4)
        ttk.Button(prof_top, text="Save Profile",
                   command=self._cs_save_profile).pack(side=tk.LEFT, padx=4)
        ttk.Button(prof_top, text="Load Selected",
                   command=self._cs_load_profile).pack(side=tk.LEFT, padx=4)
        ttk.Button(prof_top, text="Delete Selected",
                   command=self._cs_delete_profile).pack(side=tk.LEFT, padx=4)

        self._cs_prof_list = tk.Listbox(lf_prof, height=5, selectmode=tk.SINGLE)
        self._cs_prof_list.pack(fill=tk.BOTH, expand=True, padx=6, pady=(0, 6))
        self._cs_prof_list.bind("<Double-1>", lambda _: self._cs_load_profile())

        # load profiles from config
        self._cs_profiles: dict = {}
        self._cs_reload_profile_list()

    # -- Connection Settings helpers --
    def _cs_apply_preset(self, ip: str, port: int):
        self._cs_ip_var.set(ip)
        self._cs_port_var.set(str(port))
        if hasattr(self, "ip_var"):
            self.ip_var.set(ip)
        if hasattr(self, "port_var"):
            self.port_var.set(port)
        self._cs_status_var.set(f"Preset applied: {ip}:{port}")

    def _cs_apply_save(self):
        """sync ค่าจาก tab นี้ไปยัง ip_var / port_var / user_var หลัก แล้ว save"""
        try:
            ip   = self._cs_ip_var.get().strip()
            port = int(self._cs_port_var.get().strip())
            user = self._cs_user_var.get().strip()
            if hasattr(self, "ip_var"):
                self.ip_var.set(ip)
            if hasattr(self, "port_var"):
                self.port_var.set(port)
            if hasattr(self, "user_var"):
                self.user_var.set(user)
            self.save_config()
            self._cs_status_var.set(f"✅ Saved: {ip}:{port}  user={user}")
        except Exception as e:
            self._cs_status_var.set(f"❌ Error: {e}")

    def _cs_test_connect(self):
        """ทดสอบ TCP ว่าเชื่อมได้ไหม (ไม่ login)"""
        ip      = self._cs_ip_var.get().strip()
        try:
            port = int(self._cs_port_var.get().strip())
        except Exception:
            port = 23
        self._cs_status_var.set(f"Testing {ip}:{port} ...")

        def worker():
            try:
                t0 = time.monotonic()
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3.0)
                s.connect((ip, port))
                s.close()
                ms = int((time.monotonic() - t0) * 1000)
                self.after(0, lambda: self._cs_status_var.set(
                    f"✅ Reachable: {ip}:{port}  ({ms} ms)"))
            except Exception as e:
                self.after(0, lambda err=str(e): self._cs_status_var.set(
                    f"❌ Cannot connect: {err}"))

        threading.Thread(target=worker, daemon=True).start()

    def _cs_connect_now(self):
        """Apply แล้ว connect ทันที"""
        self._cs_apply_save()
        self._nb.select(self._tab_main)
        self.after(100, self.connect)

    def _cs_save_profile(self):
        name = self._cs_prof_name_var.get().strip()
        if not name:
            messagebox.showwarning("Profile", "Please enter a profile name.")
            return
        self._cs_profiles[name] = {
            "ip":   self._cs_ip_var.get().strip(),
            "port": self._cs_port_var.get().strip(),
            "user": self._cs_user_var.get().strip(),
        }
        self._cs_reload_profile_list()
        self._cs_status_var.set(f"✅ Profile '{name}' saved")
        self._save_profiles_to_config()

    def _cs_load_profile(self):
        sel = self._cs_prof_list.curselection()
        if not sel:
            messagebox.showinfo("Profile", "Please select a profile.")
            return
        name = self._cs_prof_list.get(sel[0])
        prof = self._cs_profiles.get(name)
        if not prof:
            return
        self._cs_ip_var.set(prof.get("ip", ""))
        self._cs_port_var.set(str(prof.get("port", 23)))
        self._cs_user_var.set(prof.get("user", ""))
        self._cs_status_var.set(f"✅ Profile '{name}' loaded")

    def _cs_delete_profile(self):
        sel = self._cs_prof_list.curselection()
        if not sel:
            return
        name = self._cs_prof_list.get(sel[0])
        if messagebox.askyesno("Delete", f"Delete profile '{name}'?"):
            self._cs_profiles.pop(name, None)
            self._cs_reload_profile_list()
            self._save_profiles_to_config()

    def _cs_reload_profile_list(self):
        try:
            self._cs_prof_list.delete(0, tk.END)
            for name in sorted(self._cs_profiles.keys()):
                self._cs_prof_list.insert(tk.END, name)
        except Exception:
            pass

    def _save_profiles_to_config(self):
        """บันทึก profiles ลง JSON config เดิม"""
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = {}
            data["conn_profiles"] = self._cs_profiles
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.log(f"Save profiles failed: {e}")

    def _apply_config_tab(self):
        """Apply ค่าในแท็บ API / Logs ไปใช้จริงระหว่างรัน"""
        try:
            global LOG_DIR
            self.roof_api_base = self.roof_api_base_var.get().strip()
            self.limit_api_url = self.limit_api_url_var.get().strip()
            self.rain_enabled = bool(self.rain_enabled_var.get())
            self.sensor_enabled = bool(self.sensor_enabled_var.get())
            self.sensor_api_url = self.sensor_api_url_var.get().strip()
            try:
                self.sensor_api_timeout = max(0.5, float(self.sensor_timeout_var.get()))
            except Exception: pass
            try:
                self.sensor_poll_interval = max(1, int(self.sensor_interval_var.get()))
            except Exception: pass
            try:
                self._sensor_stale_sec = max(1.0, float(self.sensor_stale_var.get()))
            except Exception: pass
            self.rain_api_url      = self.rain_api_url_var.get().strip()
            try:
                self.rain_api_timeout = max(0.5, float(self.rain_timeout_var.get()))
            except Exception:
                pass
            try:
                self.rain_stale_sec = max(1.0, float(self.rain_stale_var.get()))
            except Exception:
                pass
            try:
                self.rain_poll_interval = max(1, int(self.rain_interval_var.get()))
            except Exception:
                pass
            self.safety_fire_enabled = bool(self.safety_fire_enabled_var.get())
            self.monday_warmup_enabled = bool(self.monday_warmup_enabled_var.get())

            try:
                self.roof_preopen_sec = max(0.0, float(self.prefire_open_sec_var.get()))
            except Exception:
                self.roof_preopen_sec = float(getattr(self, "roof_preopen_sec", 15))

            try:
                self.roof_postclose_sec = max(0.0, float(self.postrest_close_sec_var.get()))
            except Exception:
                self.roof_postclose_sec = float(getattr(self, "roof_postclose_sec", 3))

            new_dir = self.log_dir_var.get().strip() or getattr(self, "log_dir", LOG_DIR) or LOG_DIR

            # update instance + global (เพื่อให้ฟังก์ชันเดิมที่อ้าง LOG_DIR ยังทำงาน)
            LOG_DIR = new_dir
            self.log_dir = new_dir
            os.makedirs(new_dir, exist_ok=True)

            # ถ้า csv_name ยังเป็นค่า default ของวัน -> ปรับให้ชี้โฟลเดอร์ใหม่
            # เพิ่มท้าย ๆ ใน _apply_config_tab() ก่อน log "Apply Config..."
            try:
                self.safety_fire_enabled = bool(self.safety_fire_enabled_var.get())
            except Exception:
                self.safety_fire_enabled = bool(getattr(self, "safety_fire_enabled", True))

            try:
                self.monday_warmup_enabled = bool(self.monday_warmup_enabled_var.get())
            except Exception:
                self.monday_warmup_enabled = bool(getattr(self, "monday_warmup_enabled", False))

            self.log(
                "Apply Config: อัปเดต roof_api_base / limit_api_url / logs directory แล้ว | "
                f"Safety Fire = {'ON' if self._is_safety_fire_enabled() else 'OFF'}"
            )
            self._update_roof_auto_label()

            try:
                cur = (self.csv_name_var.get() or "").strip()
                if not cur or os.path.basename(cur).startswith("telemetry_"):
                    self.csv_name_var.set(self._default_csv_name())
            except Exception:
                pass

            self.log("Apply Config: อัปเดต roof_api_base / limit_api_url / logs directory แล้ว")
        except Exception as e:
            try:
                self.log(f"Apply Config ล้มเหลว: {e}")
            except Exception:
                pass

    def _apply_and_save_config(self):
        self._apply_config_tab()
        self.save_config()
        try:
            messagebox.showinfo("Config", "Saved configuration successfully.")
        except Exception:
            pass

    def _start_tutorial(self):
        steps = [
            {
                "title": "Connection Info",
                "body": "Open Settings / Config and enter IP, Port, and User.",
                "widget": "ip_entry",
                "on_show": lambda: self._nb.select(self._tab_cfg),
            },
            {
                "title": "Connect",
                "body": "Click Connect to establish the laser connection.",
                "widget": "connect_btn",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Manual Control",
                "body": "Use FIRE, STANDBY, and STOP for manual control.",
                "widget": "manual_frame",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Set Time Window",
                "body": "Set Start and End times for the program.",
                "widget": "fire_entry",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Program Modes",
                "body": (
                    "Mode controls when the program runs:\n"
                    "• everyday: runs every day\n"
                    "• weekdays: runs Mon–Fri\n"
                    "• selectday: runs only on selected dates\n"
                    "• once: runs on one specific date"
                ),
                "widget": "mode_cb",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Set Durations",
                "body": "Enter Fire/Rest in minutes. Example: 1.30 or 1.3 means 1 minute 30 seconds.",
                "widget": "rest_entry",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Program Controls",
                "body": "Add, start, stop, or remove programs using these buttons.",
                "widget": "add_program_btn",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Preview",
                "body": "Use Preview Fire Times to verify the schedule before starting.",
                "widget": "preview_btn",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Start Program",
                "body": "Click Start Program to run. Status will show Firing/Resting.",
                "widget": "start_program_btn",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Temperature Control",
                "body": "Enable Temp Control and set Max Temp if you want automatic protection.",
                "widget": "temp_frame",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Roof Control",
                "body": "Use Open/Close and Auto Open/Close for the sliding roof.",
                "widget": "roof_frame",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Monitor",
                "body": "Watch Telemetry and Logs while running. Use Stop if needed.",
                "widget": "tele_frame",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Charts",
                "body": "Realtime charts show Fire/Rest state and temperature trends.",
                "widget": "charts_frame",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
            {
                "title": "Logs",
                "body": "Check logs for status messages and troubleshooting.",
                "widget": "logs_frame",
                "on_show": lambda: self._nb.select(self._tab_main),
            },
        ]
        self._tutorial = TutorialOverlay(self, steps, self._ui_refs)
        self._tutorial.start()

    def _auto_update_status(self):
        if self.laser:
            try:
                status = self.laser.get_status()  # อาจได้ None ถ้า BUSY/timeout
                if status:                        # มีค่าใหม่ค่อยอัปเดต
                    self.laser_status_var.set(f"Laser: {status}")
                    # self.log(f"STATUS → {status}")
            except Exception as e:
                self.laser_status_var.set("Laser: ERROR")
                # self.log(f"STATUS error: {e}")
        else:
            self.laser_status_var.set("Laser: -")

        # เว้น 5 วินาทีตามที่ต้องการ
        self.after(5000, self._auto_update_status)

    # ----- Program Tab Builder -----
    def add_program(self, init_data: dict | None = None):
        idx = len(self.programs)

        tab = ttk.Frame(self.prog_nb)
        self.prog_nb.add(tab, text=f"Program {idx+1}")

        pv = {
            "name": tk.StringVar(value=f"Program {idx+1}"),
            "enabled": tk.BooleanVar(value=True),
            "mode": tk.StringVar(value="everyday"),  # everyday / selectday / once
            "start": tk.StringVar(value="16:30"),
            "end": tk.StringVar(value="16:50"),
            # "fire_min": tk.IntVar(value=1),
            # "rest_min": tk.IntVar(value=1),

            "fire_ms": tk.StringVar(value="1"),   # minutes, supports M.SS (e.g., 2.30)
            "rest_ms": tk.StringVar(value="1"),

            "once_date": tk.StringVar(value=date.today().isoformat()),
            "sel_dates": set(),  # only select date (set of date)
            "edit_mode": tk.BooleanVar(value=True),

            "paused": threading.Event(),   # True = paused

        }

        def _cur_idx(v=pv):
            try:
                return self.programs.index(v)
            except ValueError:
                return -1


        if init_data:
            pv["name"].set(init_data.get("name", f"Program {idx+1}"))
            pv["enabled"].set(bool(init_data.get("enabled", True)))
            pv["mode"].set(init_data.get("mode", "everyday"))
            pv["start"].set(init_data.get("start", "16:30"))
            pv["end"].set(init_data.get("end", "16:50"))
            pv["fire_ms"].set(self._ms_to_minutes_text(int(init_data.get("fire_ms", 60000))))
            pv["rest_ms"].set(self._ms_to_minutes_text(int(init_data.get("rest_ms", 60000))))


            pv["edit_mode"] = tk.BooleanVar(value=True)  # เริ่มต้นแก้ไขได้

            if pv["mode"].get() == "once":
                d = init_data.get("once_date", date.today().isoformat())
                pv["once_date"].set(d)
            else:
                ds = init_data.get("dates", [])
                try:
                    pv["sel_dates"] = {date.fromisoformat(x) for x in ds}
                except Exception:
                    pv["sel_dates"] = set()

        # Row 0: enable + mode
        row0 = ttk.Frame(tab); row0.pack(fill=tk.X, pady=3)
        ttk.Checkbutton(row0, text="Enable", variable=pv["enabled"]).pack(side=tk.LEFT, padx=4)



        ttk.Label(row0, text="Mode").pack(side=tk.LEFT)
        mode_cb = ttk.Combobox(
            row0,
            textvariable=pv["mode"],
            width=16,
            state="readonly",
            values=["everyday", "weekdays", "selectday", "once"]
        )

        mode_cb.pack(side=tk.LEFT, padx=4)
        pv["mode_cb"] = mode_cb
        if "mode_cb" not in self._ui_refs:
            self._ui_refs["mode_cb"] = mode_cb


        # row_name = ttk.Frame(tab); row_name.pack(fill=tk.X, pady=3)
        ttk.Label(row0, text="Program Name").pack(side=tk.LEFT, padx=4)
        name_entry = ttk.Entry(row0, textvariable=pv["name"], width=15)
        name_entry.pack(side=tk.LEFT, padx=4)
        pv["name_entry"] = name_entry

        def _apply_name(_=None, i=idx):
            self._update_program_tab_titles()
            self.save_config()

        ttk.Button(row0, text="Apply", command=_apply_name).pack(side=tk.LEFT, padx=4)
        name_entry.bind("<Return>", _apply_name)

        # Row 1: time + fire/rest
        row1 = ttk.Frame(tab); row1.pack(fill=tk.X, pady=3)
        ttk.Label(row1, text="Start (HH:MM)").pack(side=tk.LEFT)
        pv["start_entry"] = ttk.Entry(row1, textvariable=pv["start"], width=8)
        pv["start_entry"].pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="End (HH:MM)").pack(side=tk.LEFT)
        pv["end_entry"] = ttk.Entry(row1, textvariable=pv["end"], width=8)
        pv["end_entry"].pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="Fire (min)").pack(side=tk.LEFT)
        pv["fire_entry"] = ttk.Entry(row1, textvariable=pv["fire_ms"], width=5)
        pv["fire_entry"].pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="Rest (min)").pack(side=tk.LEFT)
        pv["rest_entry"] = ttk.Entry(row1, textvariable=pv["rest_ms"], width=5)
        pv["rest_entry"].pack(side=tk.LEFT, padx=4)

        if "fire_entry" not in self._ui_refs:
            self._ui_refs["fire_entry"] = pv["fire_entry"]
        if "rest_entry" not in self._ui_refs:
            self._ui_refs["rest_entry"] = pv["rest_entry"]

        # Row 2: date area by mode
        date_area = ttk.Frame(tab); date_area.pack(fill=tk.X, pady=3)
        pv["date_area"] = date_area

        # once UI
        once_frm = ttk.Frame(date_area)
        ttk.Label(once_frm, text="Once date:").pack(side=tk.LEFT)
        ttk.Label(once_frm, textvariable=pv["once_date"]).pack(side=tk.LEFT, padx=6)
        ttk.Button(once_frm, text="Select Date", command=lambda v=pv: self.pick_once_date(v)).pack(side=tk.LEFT, padx=4)

        # selectday UI
        only_frm = ttk.Frame(date_area)
        ttk.Label(only_frm, text="Selected dates:").pack(side=tk.LEFT)
        lbl = ttk.Label(only_frm, text="(0)"); lbl.pack(side=tk.LEFT, padx=6)
        pv["dates_label"] = lbl
        ttk.Button(only_frm, text="Select Multiple Dates", command=lambda v=pv: self.pick_multi_dates(v)).pack(side=tk.LEFT, padx=4)

        pv["once_frm"] = once_frm
        pv["only_frm"] = only_frm

        # Row 3: preview + status + progress
        row2 = ttk.Frame(tab); row2.pack(fill=tk.X, pady=3)
        ttk.Button(row2, text="Calculate Cycles",
                command=lambda i=idx: self.preview_cycles(i)).pack(side=tk.LEFT, padx=4)

        btn_preview = ttk.Button(row2, text="Preview Fire Times",
                command=lambda i=idx: self.preview_fire_times(i))
        btn_preview.pack(side=tk.LEFT, padx=4)
        if "preview_btn" not in self._ui_refs:
            self._ui_refs["preview_btn"] = btn_preview

        cyc = ttk.Label(row2, text="LOOP = -"); cyc.pack(side=tk.LEFT, padx=8)


        status = ttk.Label(row2, text="Idle")
        status.pack(side=tk.LEFT, padx=10)
        prog = ttk.Progressbar(row2, length=200, mode="determinate", maximum=1, value=0); prog.pack(side=tk.LEFT, padx=6)
        count = ttk.Label(row2, text="", foreground="gray"); count.pack(side=tk.LEFT, padx=6)

        pv["cycle_label"] = cyc
        pv["status_lbl"] = status
        pv["progbar"] = prog
        pv["count_lbl"] = count

        # Row 4: start/stop/remove
        row3 = ttk.Frame(tab); row3.pack(fill=tk.X, pady=3)
        btn_start_prog = ttk.Button(row3, text="Start Program", command=lambda v=pv: self.start_program(_cur_idx(v)))
        btn_start_prog.pack(side=tk.LEFT, padx=4)
        if "start_program_btn" not in self._ui_refs:
            self._ui_refs["start_program_btn"] = btn_start_prog
        ttk.Button(row3, text="Stop Program",  command=lambda v=pv: self.stop_program(_cur_idx(v))).pack(side=tk.LEFT, padx=4)
        ttk.Button(row3, text="Remove Program",command=lambda v=pv: self.remove_program(_cur_idx(v))).pack(side=tk.LEFT, padx=4)
        ttk.Button(row3, text="Duplicate",     command=lambda v=pv: self.duplicate_program(_cur_idx(v))).pack(side=tk.LEFT, padx=4)

        row4 = ttk.Frame(tab); row4.pack(fill=tk.X, pady=3)
        ttk.Button(row4, text="Pause",         command=lambda v=pv: self.pause_program(_cur_idx(v))).pack(side=tk.LEFT, padx=4)
        ttk.Button(row4, text="Resume",        command=lambda v=pv: self.resume_program(_cur_idx(v))).pack(side=tk.LEFT, padx=4)
        # ttk.Button(row3, text="Edit",          command=lambda v=pv: self.edit_program(_cur_idx(v))).pack(side=tk.LEFT, padx=4)

        # runtime state
        # pv["runner"] = None
        # pv["stop_event"] = threading.Event()
        # pv["active_thread"] = None
        # pv["tab"] = tab

        pv["runner"] = None             # เธรดผู้จัดการโปรแกรม
        pv["manager_stop"] = None       # Event หยุดผู้จัดการ
        pv["active_thread"] = None      # เธรด one-shot ที่กำลังทำงาน
        pv["oneshot_stop"] = None       # Event หยุด one-shot รอบปัจจุบัน
        pv["tab"] = tab

        self.programs.append(pv)

        # react to mode change
        def on_mode_change(_=None, v=pv):
            if v["mode"].get().lower() == "once":
                v["once_date"].set(date.today().isoformat())
            self._render_date_area(v)
        mode_cb.bind("<<ComboboxSelected>>", on_mode_change)
        self._render_date_area(pv)
        self._update_program_tab_titles()

        return idx

    def remove_program(self, idx: int):
        # ✅ ต้องกัน idx ผิดก่อน (เช่น -1 หรือเกินช่วง)
        if idx is None or idx < 0 or idx >= len(self.programs):
            return

        v = self.programs[idx]

        # ✅ เคลียร์ paused ก่อน stop เพื่อไม่ให้ state ค้าง
        try:
            v["paused"].clear()
        except Exception:
            pass

        with self.active_program_lock:
            if self.active_program_idx == idx:
                self.active_program_idx = None

        # หยุดการทำงานก่อนลบแท็บ/ลบ list
        self.stop_program(idx)

        try:
            self.prog_nb.forget(v["tab"])
            v["tab"].destroy()
        except Exception:
            pass

        del self.programs[idx]
        self._update_program_tab_titles()
        self.save_config()

    def _update_program_tab_titles(self):
        for i, v in enumerate(self.programs):
            try:
                nm = (v.get("name").get().strip() if v.get("name") else "")  # type: ignore
                tab_name = nm if nm else f"Program {i+1}"
                self.prog_nb.tab(v["tab"], text=tab_name)
            except Exception:
                pass

    def duplicate_program(self, idx: int):
        if idx < 0 or idx >= len(self.programs):
            return
        v = self.programs[idx]

        # ดึงค่าปัจจุบันเป็น init_data
        init_data = {
            "name": (v["name"].get().strip() if v.get("name") else f"Program {idx+1}") + " (copy)",
            "enabled": bool(v["enabled"].get()),
            "mode": v["mode"].get().lower(),
            "start": v["start"].get(),
            "end": v["end"].get(),
            "fire_ms": self._minutes_text_to_ms(v["fire_ms"].get()),
            "rest_ms": self._minutes_text_to_ms(v["rest_ms"].get()),
        }

        if init_data["mode"] == "once":
            init_data["once_date"] = v["once_date"].get()
        elif init_data["mode"] == "selectday":
            init_data["dates"] = [d.isoformat() for d in sorted(v["sel_dates"])]

        new_idx = self.add_program(init_data)
        self.prog_nb.select(self.programs[new_idx]["tab"])
        self.save_config()

    def pause_program(self, idx: int):
        if idx < 0 or idx >= len(self.programs):
            return
        v = self.programs[idx]
        v["paused"].set()
        self._sched_log(idx, "PAUSE pressed")

        # ยกเลิก timer auto roof ของโปรแกรมนี้
        try:
            self._cancel_api_timers_for(idx)
        except Exception:
            pass

        # หยุด one-shot ที่กำลังทำงาน (ถ้ามี)
        if v.get("oneshot_stop") is not None:
            try:
                v["oneshot_stop"].set()
            except Exception:
                pass

        # บังคับ STANDBY
        self.tele_pause_until = time.monotonic() + 1.5

        try:
            self._send("$STANDBY")
            self._sched_log(idx, "Force → $STANDBY (Pause)")
        except Exception as e:
            self._sched_log(idx, f"Force STANDBY failed (Pause): {e}")

        with self.manual_lock:
            self.is_firing = False

        try:
            self.after(0, lambda: self._append_status_point(0))
        except Exception:
            pass


        # หยุด CSV ถ้ากำลังบันทึกอยู่ (ตามพฤติกรรมเดียวกับ stop_program)
        if self.tele_thread and self.tele_thread.is_alive() and self.tele_owner_idx == idx:
            self._stop_telemetry()
            self.record_var.set(False)
            self._sched_log(idx, "CSV STOP (by Pause/Stop Program)")


        self._ui_update_prog(idx, 0, 0, "Paused")

    def resume_program(self, idx: int):
        if idx < 0 or idx >= len(self.programs):
            return
        v = self.programs[idx]
        v["paused"].clear()
        self._sched_log(idx, "RESUME pressed")
        self._ui_update_prog(idx, 0, 0, "Resumed (waiting next)")

    def _render_date_area(self, v: dict):
        for w in v["date_area"].winfo_children():
            w.pack_forget()

        mode = v["mode"].get().lower()
        if mode == "everyday":
            ttk.Label(
                v["date_area"],
                text="Run every day",
                foreground="gray"
            ).pack(anchor="w")

        elif mode == "weekdays":
            ttk.Label(
                v["date_area"],
                text="Run Monday – Friday (Skip weekend)",
                foreground="gray"
            ).pack(anchor="w")

        elif mode == "once":
            v["once_frm"].pack(fill=tk.X)
            
        else:  # selectday
            cnt = len(v["sel_dates"])
            v["dates_label"].config(text=f"({cnt})")
            v["only_frm"].pack(fill=tk.X)
        
    def _ui_update_prog(self, idx: int, done: int, total: int, state: str):
        try:
            self.after(0, lambda: self._update_prog_ui(idx, done, total, state))
        except Exception:
            pass

    def pick_once_date(self, v: dict):
        dlg = CalendarDialog(self, title="Select Date (Once)", multi=False)
        if dlg.result:
            d = sorted(list(dlg.result))[0]
            v["once_date"].set(d.isoformat())

    def pick_multi_dates(self, v: dict):
        dlg = CalendarDialog(self, title="Select Multiple Dates", multi=True, initial=v["sel_dates"])
        if dlg.result is not None:
            v["sel_dates"] = set(dlg.result)
            self._render_date_area(v)

    # ---------- Plots ----------
    def _init_plots(self):
        self.fig = Figure(figsize=(8, 5.2), dpi=100)
        self.ax1 = self.fig.add_subplot(211)
        self.ax1.set_ylim(-0.2, 1.2); self.ax1.set_ylabel("FIRE (1) / REST (0)")
        self.ax1.grid(True, linestyle=":", alpha=0.5)
        self.line_x, self.line_y = [], []
        (self.line_status,) = self.ax1.plot([], [], lw=2)

        self.ax2 = self.fig.add_subplot(212)
        self.ax2.set_ylabel("DTEMF / LTEMF")
        self.ax2.grid(True, linestyle=":", alpha=0.5)
        self.tele_x, self.tele_d, self.tele_l = [], [], []
        (self.line_dtemf,) = self.ax2.plot([], [], lw=1.6, label="DTEMF")
        (self.line_ltemf,) = self.ax2.plot([], [], lw=1.6, label="LTEMF")
        self.ax2.legend(loc="upper left")
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.plot_frame)
        self.canvas.draw()
        widget = self.canvas.get_tk_widget()
        widget.pack(fill=tk.BOTH, expand=True)

        # ▼ สร้างเมนูคลิกขวาสำหรับกราฟ
        self.chart_menu = tk.Menu(widget, tearoff=0)
        self.chart_menu.add_command(label="Clear chart", command=self.clear_charts)

        # bind คลิกขวา (ปุ่ม 3) ให้แสดงเมนู
        widget.bind("<Button-3>", self._on_chart_right_click)

    def clear_charts(self):
        """ล้างข้อมูลกราฟทั้งหมด"""
        # ล้างข้อมูลที่เก็บไว้
        self.line_x.clear()
        self.line_y.clear()
        self.tele_x.clear()
        self.tele_d.clear()
        self.tele_l.clear()

        # ล้างเส้นกราฟ
        self.line_status.set_data([], [])
        self.line_dtemf.set_data([], [])
        self.line_ltemf.set_data([], [])

        # รีเซ็ตแกนให้เป็นค่า default คร่าว ๆ
        now = datetime.now(TZ)
        self.ax1.set_xlim(now - timedelta(minutes=5), now + timedelta(seconds=5))
        self.ax1.set_ylim(-0.2, 1.2)

        self.ax2.set_xlim(now - timedelta(minutes=5), now + timedelta(seconds=5))
        self.ax2.set_ylim(0, 1)  # เดี๋ยวพอมีข้อมูลใหม่ autoscale ใน `_update_clock_and_plot`

        self.canvas.draw_idle()
        self.log("Clear charts")

    def _on_chart_right_click(self, event):
        """แสดงเมนูคลิกขวาบนกราฟ"""
        try:
            self.chart_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.chart_menu.grab_release()

    def _append_status_point(self, y: int):
        now = datetime.now(TZ)
        self.line_x.append(now); self.line_y.append(y)
        cutoff = now - timedelta(hours=3)
        while self.line_x and self.line_x[0] < cutoff:
            self.line_x.pop(0); self.line_y.pop(0)

    def _append_telemetry_point(self, d: float | None, l: float | None):
        now = datetime.now(TZ)
        self.tele_x.append(now)
        self.tele_d.append(float("nan") if d is None else d)
        self.tele_l.append(float("nan") if l is None else l)
        cutoff = now - timedelta(hours=3)
        while self.tele_x and self.tele_x[0] < cutoff:
            self.tele_x.pop(0); self.tele_d.pop(0); self.tele_l.pop(0)

    def _ui_telemetry_tick(self):
        """
        อัปเดตตัวเลขบน UI และกราฟ 'ตลอดเวลา'
        - ถ้ากำลังอัด CSV อยู่: ปล่อยให้ thread CSV เป็นคนอ่าน ลดการชนกัน (แต่ยังอัปเดต label จากค่า last_* ที่มี)
        - ถ้าไม่ได้อัด CSV: อ่านแบบเบา ๆ ด้วย try_send_cmd(timeout สั้น) โดยไม่แย่งงานคำสั่งควบคุม
        """
        try:
            # ถ้า CSV thread ทำงานอยู่ ให้หลีกทาง (ไม่ query ซ้ำ)
            csv_running = bool(self.tele_thread and self.tele_thread.is_alive())

            if not csv_running:
                # อ่านแบบเบา ๆ (quiet + non-blocking)
                d = self._query_float_quiet("$DTEMF ?", timeout_s=0.35)
                l = self._query_float_quiet("$LTEMF ?", timeout_s=0.35)

                # อัปเดตค่า cache/label ถ้าอ่านได้
                if d is not None:
                    self.last_dtemf = d
                if l is not None:
                    self.last_ltemf = l

                # อัปเดตกราฟเมื่อมีค่าใหม่อย่างน้อยหนึ่งตัว
                if d is not None or l is not None:
                    self._append_telemetry_point(d if d is not None else self.last_dtemf,
                                                l if l is not None else self.last_ltemf)

            # อัปเดต Label จากค่า cache ล่าสุด (ถ้ามี)
            if self.last_dtemf is not None:
                self.lbl_dtemf.config(text=f"{self.last_dtemf}")
            if self.last_ltemf is not None:
                self.lbl_ltemf.config(text=f"{self.last_ltemf}")

            # Realtime QSDELAY (lightweight poll)
            try:
                if self.laser:
                    now = time.monotonic()
                    if now - self._qsdelay_last_poll >= 1.0:
                        self._qsdelay_last_poll = now
                        focused = False
                        try:
                            focused = (self.focus_get() == self._ui_refs.get("qs_entry"))
                        except Exception:
                            focused = False
                        if not focused:
                            resp = self.laser.try_send_cmd("$QSDELAY ?", call_timeout=0.35)
                            if resp and "QSDELAY" in resp.upper():
                                m = re.search(r"[-+]?\d+(?:\.\d+)?", resp)
                                if m and hasattr(self, "qsdelay_live_var"):
                                    self.qsdelay_live_var.set(m.group(0))
            except Exception:
                pass

            # Realtime DFREQ (lightweight poll)
            try:
                if self.laser:
                    now = time.monotonic()
                    if now - self._dfreq_last_poll >= 1.0:
                        self._dfreq_last_poll = now
                        focused = False
                        try:
                            focused = (self.focus_get() == self._ui_refs.get("df_entry"))
                        except Exception:
                            focused = False
                        if not focused:
                            resp = self.laser.try_send_cmd("$DFREQ ?", call_timeout=0.35)
                            if resp and "DFREQ" in resp.upper():
                                m = re.search(r"[-+]?\d+(?:\.\d+)?", resp)
                                if m and hasattr(self, "dfreq_live_var"):
                                    self.dfreq_live_var.set(m.group(0))
            except Exception:
                pass

        finally:
            # วนทุก 1 วินาที
            self.after(1000, self._ui_telemetry_tick)

    # ---------- Connection & Commands ----------
    def connect(self):
        host, port = self.ip_var.get().strip(), self.port_var.get()
        try:
            self.laser = LaserClient(host, port); self.laser.connect()
            self.log(f"Connected to {host}:{port}")
            self.conn_status.config(text="Connected", foreground="green")
            self._send(f"$LOGIN {self.user_var.get().strip()}")
            self.log(f"LOGIN user → {self.user_var.get().strip()}")

            # อ่านค่า QSDELAY/DFREQ จากเครื่องจริงครั้งแรก (ให้ Value ไม่อ้างอิงค่าที่กด Set)
            try:
                r1 = self.laser.try_send_cmd("$QSDELAY ?", call_timeout=0.5)
                if r1 and "QSDELAY" in r1.upper():
                    m = re.search(r"[-+]?\d+(?:\.\d+)?", r1)
                    if m:
                        self.qsdelay_live_var.set(m.group(0))
                r2 = self.laser.try_send_cmd("$DFREQ ?", call_timeout=0.5)
                if r2 and "DFREQ" in r2.upper():
                    m = re.search(r"[-+]?\d+(?:\.\d+)?", r2)
                    if m:
                        self.dfreq_live_var.set(m.group(0))
            except Exception:
                pass
            self.save_config()
        except Exception as e:
            messagebox.showerror("Connect failed", str(e))
            self.log(f"Connect failed: {e}")
            self.conn_status.config(text="Disconnected", foreground="red")

    def disconnect(self):
        # Stop all running programs before disconnecting
        try:
            self.stop_all_programs()
        except Exception:
            pass

        # ยกเลิก timer ทุกโปรแกรมก่อน
        for i in range(len(self.programs)):
            try: self._cancel_api_timers_for(i)
            except Exception: pass

        self._stop_telemetry()
        if self.laser: self.laser.close()
        self.log("Disconnected")
        self.conn_status.config(text="Disconnected", foreground="red")

    def _send(self, cmd: str):
        def worker():
            try:
                if not self.laser: raise RuntimeError("Not connected")
                resp = self.laser.send_cmd(cmd)
                self.msg_q.put(f">> {cmd}\n<< {resp}")
                return resp
            except Exception as e:
                self.msg_q.put(f">> {cmd}\n!! {e}")
                return ""
        threading.Thread(target=worker, daemon=True).start()

    # ---------- Manual controls ----------
    def cmd_fire(self):
        if not self.laser:
            try:
                messagebox.showwarning(
                    "Laser Not Connected",
                    "Laser is not connected.\nPlease click Connect before firing."
                )
            except Exception:
                pass
            try:
                self.log("FIRE blocked: laser not connected")
            except Exception:
                pass
            return

        # --- SAFETY INTERLOCK ---
        if not self._guard_fire_by_roof():
            with self.manual_lock:
                self.is_firing = False
            try:
                self._append_status_point(0)
            except Exception:
                pass
            return

        with self.manual_lock:
            self.is_firing = True
        self._append_status_point(1)
        # self._send("$FIRE")
        self._safe_fire()

        # (ส่วน auto CSV เดิมของคุณคงไว้ได้)


        # กรณีไม่มี telemetry thread → โหมด Manual ล้วน (เหมือนเดิม)
        if not (self.tele_thread and self.tele_thread.is_alive()):
            stamp = datetime.now(TZ).strftime('%Y%m%d_%H%M%S')
            manual_csv = os.path.join(getattr(self, "log_dir", LOG_DIR), f"telemetry_manual_{stamp}.csv")
            self.csv_name_var.set(manual_csv)
            self.record_var.set(True)
            self._start_telemetry()
            self.log(f"CSV START (manual) → {manual_csv}")
        else:
            # มี telemetry thread อยู่แล้ว → ส่วนใหญ่คือ CSV ของ Timer
            if self.tele_owner_idx is not None:
                # เริ่มบันทึกไฟล์ Manual parallel โดยใช้ thread เดิม
                stamp = datetime.now(TZ).strftime('%Y%m%d_%H%M%S')
                manual_csv = os.path.join(getattr(self, "log_dir", LOG_DIR), f"telemetry_manual_{stamp}.csv")
                self.manual_parallel_path = manual_csv
                self._manual_header_written = None
                self.log(f"CSV MANUAL PARALLEL START → {manual_csv}")
            # ถ้า tele_owner_idx เป็น None แสดงว่า thread นี้เป็นของ Manual อยู่แล้ว → ไม่ต้องทำอะไรเพิ่ม

    def cmd_standby(self):
        self.tele_pause_until = time.monotonic() + 1.5
        with self.manual_lock:
            self.is_firing = False
        self._append_status_point(0)
        self._send("$STANDBY")

        # ถ้ามีโปรแกรม Timer เป็นเจ้าของ CSV → ห้ามหยุด thread หลัก
        if self.tele_owner_idx is not None:
            # ปิดเฉพาะไฟล์ manual parallel (ถ้ามี)
            if self.manual_parallel_path:
                self.log(f"CSV MANUAL PARALLEL STOP → {self.manual_parallel_path}")
            self.manual_parallel_path = None
            self._manual_header_written = None
        else:
            # โหมด Manual ปกติ: STANDBY แล้วหยุด CSV ทั้งหมด
            if self.tele_thread and self.tele_thread.is_alive():
                self._stop_telemetry()
                self.record_var.set(False)
                self.log("CSV STOP (manual)")

    def cmd_stop(self):
        """Emergency STOP: หยุดยิง + หยุด CSV + ส่งคำสั่ง $STOP ไปที่เลเซอร์"""
        with self.manual_lock:
            self.is_firing = False
        self._append_status_point(0)

        # หยุดการบันทึก CSV ถ้ากำลังอัดอยู่
        try:
            if self.tele_thread and self.tele_thread.is_alive():  # FIX: ใช้ attribute จริงของคลาสนี้
                self._stop_telemetry()
                self.log("CSV STOP (manual STOP)")

            if hasattr(self, "record_var"):
                self.record_var.set(False)
            if hasattr(self, "tele_owner_idx"):
                self.tele_owner_idx = None
        except Exception as e:
            print("STOP: CSV stop error:", e)

        self._send("$STOP")

    def cmd_temp(self): self._send("$LTEMF ?")
    def cmd_qsdelay_query(self): self._send("$QSDELAY ?")
    def cmd_dfreq_query(self): self._send("$DFREQ ?")

    def apply_qsdelay(self):
        val = self.qsdelay_var.get().strip()
        if not re.fullmatch(r"\d+", val):
            messagebox.showerror("QSDELAY", "Please enter an integer value (µs)."); return
        iv = int(val)
        if not (0 <= iv <= 400):
            messagebox.showwarning("QSDELAY", "Recommended range: 0 – 400 µs.")
        self._send(f"$QSDELAY {iv}")
        self.log(f"QSDELAY → {iv} µs")
        self.save_config()

    def apply_dfreq(self):
        raw = self.freq_var.get().strip()
        m = re.fullmatch(r"(?i)\s*([0-9]+(?:\.\d+)?)\s*([kKmM]?)\s*", raw)
        if not m:
            messagebox.showerror("Frequency", "Invalid format (e.g., 20 or 1k)."); return
        val = float(m.group(1)); unit = m.group(2).lower()
        if unit == "k": val *= 1000
        elif unit == "m": val *= 1_000_000
        hz = int(val)
        if not (1 <= hz <= 22):
            messagebox.showwarning("Frequency", "Recommended range: 1 – 22 Hz.")
        self._send(f"$DFREQ {hz}")
        self.log(f"DFREQ → {hz} Hz")
        self.save_config()

    # ---------- Telemetry ----------
    def _default_csv_name(self) -> str:
        return os.path.join(getattr(self, "log_dir", LOG_DIR), f"telemetry_{datetime.now(TZ).strftime('%Y%m%d')}.csv")

    def _toggle_telemetry(self):
        if self.record_var.get(): self._start_telemetry()
        else: self._stop_telemetry()

    def _start_telemetry(self):
        if not self.laser:
            messagebox.showwarning("Telemetry", "Device not connected.")
            self.record_var.set(False); return

        path = self.csv_name_var.get().strip() or self._default_csv_name()
        if not os.path.isabs(path) and not path.startswith(getattr(self, "log_dir", LOG_DIR) + os.sep):
            path = os.path.join(getattr(self, "log_dir", LOG_DIR), path)
            self.csv_name_var.set(path)

        os.makedirs(os.path.dirname(path), exist_ok=True)

        new_file = not os.path.exists(path)
        try:
            if new_file:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([
                        "Date", "Time", "Timezone",
                        "STATUS", "QSDELAY", "DTEMF", "LTEMF", "overload", "ROOF_STATUS"
                    ])
        except Exception as e:
            messagebox.showerror("CSV", f"Cannot create CSV file: {e}")
            self.record_var.set(False); return

        if self.tele_thread and self.tele_thread.is_alive():
            self.tele_stop.set()
            try:
                self.tele_thread.join(timeout=0.5)
            except Exception:
                pass
            self.tele_stop.clear()

        def worker():
            self.log("เริ่มเก็บ Telemetry (CSV)")

            while not self.tele_stop.is_set():
                if time.monotonic() < self.tele_pause_until:
                    time.sleep(0.2)
                    continue

                ts = datetime.now(TZ).isoformat(timespec="seconds")
                d = self._query_float("$DTEMF ?")
                l = self._query_float("$LTEMF ?")

                if d is not None:
                    self.last_dtemf = d
                    self.after(0, lambda v=d: self.lbl_dtemf.config(text=f"{v}"))
                if l is not None:
                    self.last_ltemf = l
                    self.after(0, lambda v=l: self.lbl_ltemf.config(text=f"{v}"))


                with self.manual_lock:
                    status_num = 1 if self.is_firing else 0
                # qs = self.qsdelay_var.get().strip()
                qs = (self.qsdelay_live_var.get().strip() if hasattr(self, "qsdelay_live_var") else self.qsdelay_var.get().strip())
                

                try:
                    maxv = float(self.max_temp_var.get())
                except Exception:
                    maxv = None
                try:
                    max_dtemf = float(self.max_dtemf_var.get())
                except Exception:
                    max_dtemf = None

                # ===== จำค่าล่าสุดของ DTEMF / LTEMF =====
                if not hasattr(self, "last_dtemf_value"):
                    self.last_dtemf_value = None
                if not hasattr(self, "last_ltemf_value"):
                    self.last_ltemf_value = None

                if d is not None:
                    self.last_dtemf_value = d
                else:
                    d = self.last_dtemf_value

                if l is not None:
                    self.last_ltemf_value = l
                else:
                    l = self.last_ltemf_value

                # overload: ตรวจทั้ง LTEMF และ DTEMF
                try:
                    temp_enabled = bool(self.temp_ctl_enabled.get())
                except Exception:
                    temp_enabled = True
                ltemf_over = (temp_enabled and l is not None and maxv is not None and l > maxv)
                dtemf_over = (temp_enabled and d is not None and max_dtemf is not None and d > max_dtemf)
                overload = ltemf_over or dtemf_over

                try:
                    self.after(0, lambda dd=d, ll=l: self._append_telemetry_point(
                        float(dd) if dd is not None else None,
                        float(ll) if ll is not None else None
                    ))
                except Exception:
                    pass

                if overload:
                    triggered = "DTEMF" if dtemf_over else "LTEMF"
                    trig_val  = (float(d) if dtemf_over and d is not None else
                                 float(l) if l is not None else 0.0)
                    trig_max  = (float(max_dtemf) if dtemf_over and max_dtemf is not None else
                                 float(maxv) if maxv is not None else 0.0)
                    other_val = (float(l) if dtemf_over and l is not None else
                                 float(d) if d is not None else None)
                    other_max = (float(maxv) if dtemf_over and maxv is not None else
                                 float(max_dtemf) if max_dtemf is not None else 0.0)
                    self._ui_call(self._show_overheat_popup_dual,
                                  triggered, trig_val, trig_max, other_val, other_max)
                else:
                    self._ui_call(self._hide_overheat_popup)

                try:
                    # เตรียม row เดียว ใช้ได้ทั้ง main CSV และ manual parallel
                    now = datetime.now(TZ)
                    date_str = now.strftime("%Y-%m-%d")
                    time_str = now.strftime("%H:%M:%S")
                    tz_str = now.tzname() or "UTC+7"

                    roof_state = self._get_roof_status_cached()

                    row = [
                        date_str,           # Date
                        time_str,           # Time
                        tz_str,             # Timezone
                        status_num,         # STATUS (1 = Fire, 0 = Rest)
                        qs,                 # QSDELAY
                        d if d is not None else "",  # DTEMF
                        l if l is not None else "",  # LTEMF
                        overload,           # overload flag
                        roof_state          # ROOF_STATUS
                    ]

                    # เขียนไฟล์หลัก (Timer หรือ Manual ปกติ)
                    main_path = self.csv_name_var.get().strip()
                    with open(main_path, "a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow(row)

                    # ถ้ามีไฟล์ Manual parallel ให้เขียนซ้ำลงไปด้วย
                    manual_path = getattr(self, "manual_parallel_path", None)
                    if manual_path:
                        try:
                            os.makedirs(os.path.dirname(manual_path), exist_ok=True)

                            # สร้าง header ไฟล์ manual ครั้งแรก
                            if self._manual_header_written != manual_path or not os.path.exists(manual_path):
                                with open(manual_path, "w", newline="", encoding="utf-8") as mf:
                                    csv.writer(mf).writerow([
                                        "Date", "Time", "Timezone",
                                        "STATUS", "QSDELAY", "DTEMF", "LTEMF", "overload", "ROOF_STATUS"
                                    ])
                                self._manual_header_written = manual_path

                            with open(manual_path, "a", newline="", encoding="utf-8") as mf:
                                csv.writer(mf).writerow(row)
                        except Exception as e2:
                            self.log(f"บันทึก CSV manual parallel ล้มเหลว: {e2}")

                except Exception as e:
                    self.log(f"บันทึก CSV ล้มเหลว: {e}")

                for _ in range(int(self.tele_interval_sec * 10)):
                    if self.tele_stop.is_set():
                        break
                    time.sleep(0.1)
            self.log("หยุดเก็บ Telemetry (CSV)")

        self.tele_thread = threading.Thread(target=worker, daemon=True)
        self.tele_thread.start()

    def _stop_telemetry(self):
        if self.tele_thread and self.tele_thread.is_alive():
            self.tele_stop.set()
            self.tele_thread.join(timeout=0.5)
        self.tele_thread = None
        self.tele_stop.clear()
        self.tele_owner_idx = None

        # ล้างสถานะ manual parallel
        self.manual_parallel_path = None
        self._manual_header_written = None

    def _cmd_name_from(self, cmd: str) -> str:
        # "$DTEMF ?" -> "DTEMF"
        s = cmd.strip()
        if s.startswith("$"):
            s = s[1:]
        return s.split()[0].upper() if s else ""

    def _query_float(self, cmd: str) -> float | None:
        try:
            if not self.laser:
                return None
            expect = self._cmd_name_from(cmd)

            resp = self.laser.try_send_cmd(cmd, call_timeout=0.6)
            if resp is None:
                return None  # BUSY
            self.msg_q.put(f">> {cmd}\n<< {resp}")

            # ✅ กันค่าหลุด: ต้องเป็นคำตอบของคำสั่งที่เราถามจริง
            if expect and (expect not in resp.upper()):
                return None

            m = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", resp)
            return float(m.group(0)) if m else None
        except Exception:
            return None
        
    def _query_float_quiet(self, cmd: str, timeout_s: float = 0.35) -> float | None:
        """อ่านค่า float แบบเบา ๆ: non-blocking, quiet (ไม่เขียน log), timeout สั้น"""
        try:
            if not self.laser:
                return None
            if time.monotonic() < self.tele_pause_until:
                return None

            expect = self._cmd_name_from(cmd)

            resp = self.laser.try_send_cmd(cmd, call_timeout=timeout_s)
            if resp is None:
                return None  # BUSY

            # ✅ กันค่าหลุด: ต้องเป็นคำตอบของคำสั่งที่เราถามจริง
            if expect and (expect not in resp.upper()):
                return None

            m = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", resp)
            return float(m.group(0)) if m else None
        except Exception:
            return None
       
    def _parse_float_safe(self, s: str) -> float | None:
        """ดึงค่าทศนิยมตัวแรกจากสตริง เช่น '$LTEMF=33.2C' -> 33.2"""
        if s is None:
            return None
        m = re.search(r"([-+]?\d+(?:\.\d+)?)", str(s))
        try:
            return float(m.group(1)) if m else None
        except Exception:
            return None

    def _query_ltemf(self) -> float | None:
        """คืน LTEMF (°C): พยายาม query แบบเร็ว ถ้า busy ใช้ค่าล่าสุดจาก telemetry"""
        # 1) ถ้า telemetry เพิ่งอัปเดต มีค่าล่าสุด ใช้เลยเพื่อลดชนกับคำสั่งควบคุม
        if self.last_ltemf is not None and (time.monotonic() >= self.tele_pause_until):
            return self.last_ltemf
        # 2) ลองถามแบบ timeout สั้น
        try:
            if not self.laser:
                return None
            resp = self.laser.try_send_cmd("$LTEMF ?", call_timeout=0.6)
            if resp is None:
                return self.last_ltemf  # busy → คืนค่าล่าสุดที่มี
            m = re.search(r"[-+]?\d+(?:\.\d+)?", resp)
            return float(m.group(0)) if m else self.last_ltemf
        except Exception:
            return self.last_ltemf

    def _temp_monitor_tick(self):
        """เช็คอุณหภูมิทุก 1 วินาที
        - DTEMF > max_dtemf → STANDBY ทันที (Diode อันตราย)
        - LTEMF > max_ltemf → STANDBY ทันที (ตัวเครื่องร้อน)
        - CSV ยังทำงานต่อในทั้งสองกรณี
        """
        try:
            if self.temp_ctl_enabled.get():
                hysteresis = 0.3

                # ---- อ่านค่าทั้งสองแบบ quiet (non-blocking) ----
                lval = self.last_ltemf   # ใช้ cache จาก telemetry tick
                dval = self.last_dtemf

                try:
                    max_ltemf = float(self.max_temp_var.get())
                except Exception:
                    max_ltemf = 32.5
                try:
                    max_dtemf = float(self.max_dtemf_var.get())
                except Exception:
                    max_dtemf = 35.0

                # ---- ตรวจ DTEMF (สำคัญกว่า ทำก่อน) ----
                if dval is not None:
                    if dval > max_dtemf and not self._dtemf_alarm_active:
                        self._dtemf_alarm_active = True
                        with self.manual_lock:
                            self.is_firing = False
                        self._append_status_point(0)
                        self._send("$STANDBY")
                        self.log(f"⚠ Over-Temp DTEMF: {dval:.2f} °C > {max_dtemf:.2f} °C → STANDBY")
                        self.after(5000, self._delayed_roof_close)
                        self._ui_call(self._show_overheat_popup_dual,
                                      "DTEMF", dval, max_dtemf, lval, max_ltemf)
                    elif dval <= (max_dtemf - hysteresis) and self._dtemf_alarm_active:
                        self._dtemf_alarm_active = False
                        self.log(f"✅ DTEMF กลับสู่ปกติ: {dval:.2f} °C")
                        if not self._temp_alarm_active:
                            self._ui_call(self._hide_overheat_popup)

                # ---- ตรวจ LTEMF ----
                if lval is not None:
                    if lval > max_ltemf and not self._temp_alarm_active:
                        self._temp_alarm_active = True
                        with self.manual_lock:
                            self.is_firing = False
                        self._append_status_point(0)
                        self._send("$STANDBY")
                        self.log(f"⚠ Over-Temp LTEMF: {lval:.2f} °C > {max_ltemf:.2f} °C → STANDBY")
                        self.after(5000, self._delayed_roof_close)
                        self._ui_call(self._show_overheat_popup_dual,
                                      "LTEMF", lval, max_ltemf, dval, max_dtemf)
                    elif lval <= (max_ltemf - hysteresis) and self._temp_alarm_active:
                        self._temp_alarm_active = False
                        self.log(f"✅ LTEMF กลับสู่ปกติ: {lval:.2f} °C")
                        if not self._dtemf_alarm_active:
                            self._ui_call(self._hide_overheat_popup)

            else:
                # Temp Control ปิด → reset alarm ทั้งคู่
                self._temp_alarm_active  = False
                self._dtemf_alarm_active = False
                try:
                    self._ui_call(self._hide_overheat_popup)
                except Exception:
                    pass
        finally:
            self.after(1000, self._temp_monitor_tick)

    # ---------- Logs & clock ----------
    def clear_terminal(self): self.log_text.delete("1.0", tk.END)
    def clear_sched_terminal(self): self.sched_log_text.delete("1.0", tk.END)

    def log(self, msg: str):
        stamp = datetime.now(TZ).strftime("%H:%M:%S")
        self.msg_q.put(f"[{stamp}] {msg}")

    def _sched_log(self, idx, msg: str):
        stamp = datetime.now(TZ).strftime("%H:%M:%S")
        label = f"SCHED#{idx+1}" if idx is not None else "SCHED"
        self.msg_q.put(f"[{label}] [{stamp}] {msg}")

    def _drain_logs(self):
        try:
            while True:
                msg = self.msg_q.get_nowait()
                if msg.startswith("[SCHED#"):
                    self.sched_log_text.insert(tk.END, msg + "\n"); self.sched_log_text.see(tk.END)
                else:
                    self.log_text.insert(tk.END, msg + "\n"); self.log_text.see(tk.END)
        except queue.Empty:
            pass
        self.after(200, self._drain_logs)

    def _update_clock_and_plot(self):
        # อัปเดตเส้นกราฟ
        self.line_status.set_data(self.line_x, self.line_y)
        if self.line_x:
            self.ax1.set_xlim(self.line_x[0], self.line_x[-1] + timedelta(seconds=5))
        self.line_dtemf.set_data(self.tele_x, self.tele_d)
        self.line_ltemf.set_data(self.tele_x, self.tele_l)
        if self.tele_x:
            self.ax2.set_xlim(self.tele_x[0], self.tele_x[-1] + timedelta(seconds=5))
            vals = [v for v in (self.tele_d + self.tele_l) if v == v]
            if vals:
                ymin, ymax = min(vals), max(vals)
                if ymin == ymax: ymin -= 1; ymax += 1
                pad = (ymax - ymin) * 0.1
                self.ax2.set_ylim(ymin - pad, ymax + pad)
        self.canvas.draw_idle()

        # จุดสถานะ (อัพเดตทุกวินาที)
        with self.manual_lock:
            y = 1 if self.is_firing else 0
        self._append_status_point(y)

        # ⏰ อัปเดตข้อความเวลา
        now = datetime.now(TZ)
        utc_off = now.utcoffset().total_seconds()/3600 if now.utcoffset() else 7
        if hasattr(self, "clock_var"):
            self.clock_var.set(f"Time: {now.strftime('%Y-%m-%d %H:%M:%S')} (UTC{utc_off:+.0f})")

        self.after(1000, self._update_clock_and_plot)

    # ---------- Program logic ----------
    def _parse_hhmm_into(self, base_date: date, hhmm: str) -> datetime:
        hh, mm = [int(x) for x in hhmm.strip().split(":")]
        return datetime(base_date.year, base_date.month, base_date.day, hh, mm, tzinfo=TZ)

    def preview_cycles(self, idx: int):
        if idx < 0 or idx >= len(self.programs): return
        v = self.programs[idx]
        try:
            start_dt = self._parse_hhmm_into(date.today(), v["start"].get())
            end_dt = self._parse_hhmm_into(date.today(), v["end"].get())
            if end_dt <= start_dt: end_dt += timedelta(days=1)
            fire_td = timedelta(milliseconds=self._minutes_text_to_ms(v["fire_ms"].get()))
            rest_td = timedelta(milliseconds=self._minutes_text_to_ms(v["rest_ms"].get()))
            n = FireRestScheduler.count_fire_cycles(start_dt, end_dt, fire_td, rest_td)
            v["cycle_label"].config(text=f"LOOP = {n} cycles")
            self._sched_log(idx, f"Preview cycles: {start_dt} → {end_dt}, fire={fire_td}, rest={rest_td} → {n} cycles")
        except Exception as e:
            messagebox.showerror("Invalid inputs", str(e))

    def preview_fire_times(self, idx: int):
        # """Show fire times for the current start/end window (example for 1 day)."""
        """ see: FireRestScheduler.compute_fire_times """
        if idx < 0 or idx >= len(self.programs):
            return
        v = self.programs[idx]
        try:
            start_dt = self._parse_hhmm_into(date.today(), v["start"].get())
            end_dt = self._parse_hhmm_into(date.today(), v["end"].get())
            if end_dt <= start_dt:
                end_dt += timedelta(days=1)

            fire_td = timedelta(milliseconds=self._minutes_text_to_ms(v["fire_ms"].get()))
            rest_td = timedelta(milliseconds=self._minutes_text_to_ms(v["rest_ms"].get()))

            if fire_td.total_seconds() <= 0:
                raise ValueError("Fire duration must be greater than 0 minutes.")
            if rest_td.total_seconds() < 0:
                raise ValueError("Rest duration must not be negative.")

            times = []
            cur = start_dt
            max_events = 500   # safety limit to prevent huge lists
            while cur < end_dt and len(times) < max_events:
                times.append(cur)
                cur += fire_td + rest_td

            if not times:
                messagebox.showinfo(
                    "Preview fire times",
                    "No fire times found in the selected window.\nPlease check start/end times and fire/rest values.",
                )
                return

            # lines = [f"{i+1:02d}) {t.strftime('%H:%M')}" for i, t in enumerate(times)]
            lines = [
                f"{i+1:02d}) {t.strftime('%H:%M:%S')}.{int(t.microsecond/1000):03d}"
                for i, t in enumerate(times)
            ]

            msg = "Fire times for the current window (based on start/end):\n\n" + "\n".join(lines)

            # If there are too many entries, show only the first 100.
            if len(lines) > 100:
                msg += f"\n\n... total {len(lines)} entries (showing only the first 100)"

            messagebox.showinfo("Preview fire times", msg)
        except Exception as e:
            messagebox.showerror("Invalid inputs", str(e))

    def _update_prog_ui(self, idx: int, done: int, total: int, state: str):
        if idx < 0 or idx >= len(self.programs): return
        v = self.programs[idx]
        v["progbar"].configure(maximum=max(1, total), value=done)
        v["count_lbl"].config(text=f"{done} / {total} times" if total > 0 else "")
        v["status_lbl"].config(text=state)

    def _parse_minutes_text(self, text: str) -> float:
        s = (text or "").strip().replace(",", ".")
        if not s:
            raise ValueError("Duration is required.")
        if "." in s:
            left, right = s.split(".", 1)
            if right.isdigit() and len(right) in (1, 2):
                minutes = int(left) if left else 0
                seconds = int(right) * 10 if len(right) == 1 else int(right)
                if seconds >= 60:
                    raise ValueError("Seconds must be between 00 and 59.")
                return minutes + (seconds / 60.0)
        return float(s)

    def _minutes_text_to_ms(self, text: str) -> int:
        return int(round(self._parse_minutes_text(text) * 60000))

    def _ms_to_minutes_text(self, ms: int) -> str:
        total_sec = int(round(ms / 1000.0))
        minutes = total_sec // 60
        seconds = total_sec % 60
        if seconds == 0:
            return f"{minutes}"
        return f"{minutes}.{seconds:02d}"

    def _next_fire_time(self, now_dt: datetime, start_dt: datetime, end_dt: datetime, fire_ms: int, rest_ms: int):
        if now_dt <= start_dt:
            return start_dt
        cycle = timedelta(milliseconds=fire_ms + rest_ms)
        if cycle.total_seconds() <= 0:
            return None
        elapsed = now_dt - start_dt
        cycles = int(elapsed // cycle)
        next_start = start_dt + (cycle * (cycles + 1))
        if next_start >= end_dt:
            return None
        return next_start

    def clear_dtemf_cache(self):
        self.last_dtemf = None
        try:
            self.lbl_dtemf.config(text="-")
        except Exception:
            pass

    def _reset_monday_warmup_state(self, idx: int):
        self._monday_warmup_sent[idx] = False
        self._monday_ready[idx] = False
        self._monday_last_poll[idx] = 0.0

    def _maybe_run_monday_warmup(self, idx: int, mode_now: str, start_dt: datetime):
        if mode_now not in ("weekdays", "weekday"):
            return
        if not bool(self.monday_warmup_enabled_var.get()):
            return
        if start_dt.weekday() != 0:
            return

        lead_min = max(0, int(self.monday_warmup_lead_min_var.get()))
        threshold = float(self.monday_warmup_threshold_var.get())
        warmup_dt = start_dt - timedelta(minutes=lead_min)
        now_dt = datetime.now(TZ)

        if now_dt < warmup_dt:
            return

        if not self._monday_warmup_sent.get(idx, False):
            self.clear_dtemf_cache()
            self.tele_pause_until = time.monotonic() + 1.5
            try:
                self._send("$STANDBY")
                self._sched_log(idx, f"Monday warmup: send STANDBY at {now_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                self._sched_log(idx, f"Monday warmup: STANDBY failed: {e}")
            self._monday_warmup_sent[idx] = True

        last_poll = float(self._monday_last_poll.get(idx, 0.0))
        if (time.monotonic() - last_poll) < 5.0:
            return

        self._monday_last_poll[idx] = time.monotonic()
        d = self._query_float_quiet("$DTEMF ?", timeout_s=0.35)
        if d is None:
            return

        self.last_dtemf = d
        try:
            self.after(0, lambda v=d: self.lbl_dtemf.config(text=f"{v}"))
        except Exception:
            pass

        if d >= threshold:
            if not self._monday_ready.get(idx, False):
                self._sched_log(idx, f"Monday warmup READY: DTEMF={d:.2f} >= {threshold:.2f}")
            self._monday_ready[idx] = True
        else:
            self._sched_log(idx, f"Monday warmup WARMING: DTEMF={d:.2f} < {threshold:.2f}")

    def _finalize_friday_weekday_window(self, idx: int, mode_now: str, start_dt: datetime):
        if mode_now not in ("weekdays", "weekday"):
            return
        if not bool(self.monday_warmup_enabled_var.get()):
            return
        if start_dt.weekday() != 4:
            return

        self.tele_pause_until = time.monotonic() + 1.5
        try:
            self._send("$STOP")
            self._sched_log(idx, "Friday final cycle done -> STOP")
        except Exception as e:
            self._sched_log(idx, f"Friday final cycle STOP failed: {e}")

        self.clear_dtemf_cache()
        self._reset_monday_warmup_state(idx)
        self._sched_log(idx, "Friday final cycle done -> DTEMF cache cleared")

    def compute_next_occurrence(self, idx: int, now_dt: datetime):
        if idx < 0 or idx >= len(self.programs):
            return None, None

        v = self.programs[idx]
        mode = v["mode"].get().lower()
        start_hhmm = v["start"].get().strip()
        end_hhmm = v["end"].get().strip()

        def mk_se(d: date):
            s = self._parse_hhmm_into(d, start_hhmm)
            e = self._parse_hhmm_into(d, end_hhmm)
            if e <= s:  # รองรับช่วงข้ามเที่ยงคืน เช่น 23:00 → 01:00
                e += timedelta(days=1)
            return s, e

        if mode == "everyday":
            today = now_dt.date()
            s, e = mk_se(today)

            if now_dt < s:
                # ยังไม่ถึงเวลาเริ่มของวันนี้ → รอวันนี้
                return s, e
            elif s <= now_dt < e:
                # Inside today's window -> return window start, caller will align to next fire time
                return s, e
            else:
                # เลยช่วงของวันนี้แล้ว → ไปวันถัดไป
                return mk_se(today + timedelta(days=1))

        elif mode == "once":
            try:
                d = date.fromisoformat(v["once_date"].get())
            except Exception:
                return None, None
            s, e = mk_se(d)

            if now_dt < s:
                return s, e
            if s <= now_dt < e:
                return s, e
            return None, None

        elif mode in ("weekdays", "weekday"):
            d = now_dt.date()

            # ถ้าวันนี้เป็น เสาร์/อาทิตย์ ให้เลื่อนไปวันจันทร์ถัดไป
            while d.weekday() >= 5:
                d += timedelta(days=1)

            s, e = mk_se(d)

            if now_dt < s:
                return s, e
            elif s <= now_dt < e:
                return s, e
            else:
                # ไปวันทำงานถัดไป
                d += timedelta(days=1)
                while d.weekday() >= 5:
                    d += timedelta(days=1)
                return mk_se(d)


        else:  # selectday
            if not v["sel_dates"]:
                return None, None

            # ถ้าวันนี้ถูกเลือก และตอนนี้อยู่ในหน้าต่างเวลา → เริ่มต่อได้ทันที
            today = now_dt.date()
            if today in v["sel_dates"]:
                s, e = mk_se(today)
                if s <= now_dt < e:
                    return s, e

            for d in sorted(v["sel_dates"]):
                s, e = mk_se(d)
                if s > now_dt:
                    return s, e
            return None, None

    def _set_program_editable(self, v: dict, editable: bool):
        state = "normal" if editable else "disabled"

        # widget หลัก
        for key in ("start_entry", "end_entry", "fire_entry", "rest_entry", "mode_cb", "name_entry"):
            w = v.get(key)
            if w:
                try:
                    w.config(state=state)
                except Exception:
                    pass

        # ปุ่ม/องค์ประกอบใน date_area (Once/Selectday)
        try:
            for child in v["date_area"].winfo_children():
                for w in child.winfo_children():
                    try:
                        w.config(state=state)
                    except Exception:
                        pass
        except Exception:
            pass

        v["edit_mode"].set(editable)

    def edit_program(self, idx: int):
        if idx < 0 or idx >= len(self.programs):
            return
        v = self.programs[idx]

        # ถ้ากำลังรันอยู่ ต้อง Stop ก่อน
        if v.get("runner") and v["runner"].is_alive():
            messagebox.showwarning("Program running", "Please stop the program before editing.")
            return

        self._set_program_editable(v, True)
        self._sched_log(idx, "Program unlocked (Edit)")

    def start_program(self, idx: int):
        if idx < 0 or idx >= len(self.programs): 
            return
        v = self.programs[idx]

        if not self.laser:
            try:
                messagebox.showwarning(
                    "Laser Not Connected",
                    "Laser is not connected.\nPlease click Connect before starting the program."
                )
            except Exception:
                pass
            try:
                self._sched_log(idx, "Start blocked: Laser not connected")
            except Exception:
                pass
            return

        v["paused"].clear()

        # ตรวจฝนก่อน: ถ้ามีฝนอยู่จะยังไม่ start (แต่ถ้า start แล้วจะ pause อัตโนมัติ)
        if self.is_raining_now():
            try:
                messagebox.showwarning(
                    "Rain Detected",
                    "Cannot start the scheduled program while it is raining.\n"
                    "Please wait until rain stops before starting."
                )
            except Exception:
                pass
            self._sched_log(idx, "⛔ Start blocked: Rain sensor active")
            return

        if not v["enabled"].get():
            self._sched_log(idx, "โปรแกรมถูกปิดการทำงาน (Enable=OFF)")
            return

        # เคลียร์ของเก่า
        self.stop_program(idx)

        fire_ms = self._minutes_text_to_ms(v["fire_ms"].get())
        rest_ms = self._minutes_text_to_ms(v["rest_ms"].get())

        self._set_program_editable(v, False)
        self._sched_log(idx, "Program locked (Start)")
        self._reset_monday_warmup_state(idx)

        # ใช้ event ที่เก็บใน dict (จะได้สั่งหยุดจาก stop_program ได้)
        v["manager_stop"] = threading.Event()

        def runner():
            self._sched_log(idx, "MANAGER START")
            while not v["manager_stop"].is_set():
                # ป้องกันโดนลบระหว่างทำงาน
                while (not v["manager_stop"].is_set()) and v["paused"].is_set():
                    self._ui_update_prog(idx, 0, 0, "Paused")

                    time.sleep(0.2)

                    if v["manager_stop"].is_set():
                        break

                if idx < 0 or idx >= len(self.programs):
                    self._sched_log(idx, "โปรแกรมถูกลบออกไปแล้ว (manager exit)")
                    break

                now_dt = datetime.now(TZ)

                s_dt, e_dt = self.compute_next_occurrence(idx, now_dt)

                if not s_dt:
                    self._sched_log(idx, "ไม่มีรอบถัดไป (จบโปรแกรมตามเงื่อนไข)")
                    break

                # If already inside the window, align to the next fire time instead of starting immediately
                if s_dt <= now_dt < e_dt:
                    aligned = self._next_fire_time(now_dt, s_dt, e_dt, fire_ms, rest_ms)
                    if aligned is None:
                        self._sched_log(idx, "No remaining fire time in the current window")
                        break
                    s_dt = aligned

                try:
                    self._schedule_prefire_api(idx, s_dt)
                except Exception as e:
                    self._sched_log(idx, f"ตั้ง auto-open ไม่สำเร็จ: {e}")


                total = FireRestScheduler.count_fire_cycles(
                    s_dt, e_dt,
                    timedelta(milliseconds=fire_ms),
                    timedelta(milliseconds=rest_ms)
                )


                done = 0
                self._ui_update_prog(idx, 0, 0, f"Waiting {s_dt.strftime('%Y-%m-%d %H:%M:%S')} (Active={self.active_program_idx})")


                LEAD = 20  # วินาที กันเริ่ม telemetry ใกล้เวลาเริ่มจริง
                while True:
                    if v["manager_stop"].is_set():
                        break

                    while (not v["manager_stop"].is_set()) and v["paused"].is_set():
                        self._ui_update_prog(idx, 0, 0, "Paused")

                        time.sleep(0.2)

                    now2 = datetime.now(TZ)
                    try:
                        self._maybe_run_monday_warmup(idx, v["mode"].get().lower(), s_dt)
                    except Exception as e:
                        self._sched_log(idx, f"Monday warmup logic error: {e}")

                    if now2 >= (s_dt - timedelta(seconds=LEAD)):
                        break

                    time.sleep(0.2)

                if v["manager_stop"].is_set():
                    break

                # ===== ถึงเวลาเริ่มแล้ว ค่อย claim active program =====
                with self.active_program_lock:
                    if self.active_program_idx is None:
                        self.active_program_idx = idx
                    elif self.active_program_idx != idx:
                        self._sched_log(idx, f"Blocked: active program = P{self.active_program_idx+1}")
                        self._ui_update_prog(idx, 0, 0, f"Blocked (Active=P{self.active_program_idx+1})")

                        time.sleep(1.0)
                        continue

                # ===== เริ่ม CSV/telemetry ของโปรแกรมนี้ (หลัง claim active เท่านั้น) =====
                stamp = s_dt.strftime('%Y%m%d_%H%M%S')
                csvname = os.path.join(getattr(self, "log_dir", LOG_DIR), f"telemetry_sched_P{idx+1}_{stamp}.csv")
                self.csv_name_var.set(csvname)
                self.record_var.set(True)
                self._start_telemetry()
                self.tele_owner_idx = idx
                self._sched_log(idx, f"CSV START → {csvname}")

                # one-shot ของช่วงนี้
                local_stop = threading.Event()
                v["oneshot_stop"] = local_stop


                def on_fire():
                    nonlocal done

                    # หน่วงป้อง telemetry overlap
                    self.tele_pause_until = time.monotonic() + 1.5

                    # --- SAFETY INTERLOCK: Roof ต้อง ON เท่านั้น ---
                    if not self._guard_fire_by_roof():
                        # บล็อกการยิง: ต้องทำให้สถานะกลับไปเป็นไม่ยิงด้วย
                        with self.manual_lock:
                            self.is_firing = False

                        # อัปเดต UI/กราฟสถานะผ่าน main thread
                        try:
                            self.after(0, lambda: self._append_status_point(0))
                            self.after(0, lambda: self._update_prog_ui(idx, done, total, f"Blocked (Roof Closed) ({done}/{total})"))
                        except Exception:
                            pass

                        # ไม่เพิ่ม done และไม่ยิง
                        return

                    # --- ผ่าน interlock แล้ว ค่อยยิง ---
                    with self.manual_lock:
                        self.is_firing = True

                    done += 1
                    # (UI update ควรผ่าน after เพื่อชัวร์ว่าอยู่ main thread)
                    try:
                        self.after(0, lambda: self._update_prog_ui(idx, done, total, f"Firing ({done}/{total})"))
                        self.after(0, lambda: self._append_status_point(1))
                    except Exception:
                        pass

                    # self._send("$FIRE")
                    self._safe_fire()


                def on_rest(is_last: bool = False):
                    # หน่วงป้อง telemetry overlap
                    self.tele_pause_until = time.monotonic() + 1.5

                    with self.manual_lock:
                        self.is_firing = False

                    # ---------- UI ----------
                    status_txt = f"Resting ({done}/{total})"
                    if is_last:
                        status_txt = f"Resting FINAL ({done}/{total})"

                    # self._update_prog_ui(idx, done, total, status_txt)
                    # self._append_status_point(0)
                    def _ui_rest():
                        self._update_prog_ui(idx, done, total, status_txt)
                        self._append_status_point(0)

                    try:
                        self.after(0, _ui_rest)   # หรือ self._ui_call(_ui_rest) ถ้าคุณมี _ui_call ที่ใช้ self.after แล้ว
                    except Exception:
                        pass


                    # ---------- ส่งคำสั่งเลเซอร์พัก ----------
                    self._send("$STANDBY")

                    # ---------- postrest เหมือนเดิม (delay +3s เปิด/ปิดตามระบบคุณ) ----------
                    # ❗ คุณต้องการให้ final rest ก็มี postrest เช่นกัน
                    self._schedule_postrest_api(idx)

                    # ---------- ถ้าเป็น REST ครั้งสุดท้าย ----------
                    if is_last:
                        self._sched_log(idx, "FINAL REST → roof close scheduled")
                        try:
                            # ปรับตามฟังก์ชันปิดหลังคาที่คุณใช้จริง
                            self._schedule_roof_close_if_open()
                        except Exception as e:
                            self._sched_log(idx, f"roof close error: {e}")
                        return   # ❗ ห้ามตั้ง prefire ต่อ

                    # ---------- Rest ปกติ → ตั้ง prefire รอบถัดไป ----------
                    try:
                        next_fire_start = datetime.now(TZ) + timedelta(milliseconds=rest_ms)
                        if next_fire_start < e_dt:  
                            self._schedule_prefire_api(idx, next_fire_start)
                    except Exception as e:
                        self._sched_log(idx, f"ตั้ง auto-open รอบถัดไปไม่สำเร็จ: {e}")


                fr = FireRestScheduler(
                    start_time=s_dt,
                    end_time=e_dt,
                    fire_ms=fire_ms,
                    rest_ms=rest_ms,
                    on_fire=on_fire,
                    on_rest=on_rest,                 # ← ไม่ใช้ lambda แบบเดิมแล้ว
                    on_tick=lambda _now: None,
                    stop_event=local_stop,
                )

                v["active_thread"] = fr
                fr.start()
                fr.join()

                try:
                    self._finalize_friday_weekday_window(idx, v["mode"].get().lower(), s_dt)
                except Exception as e:
                    self._sched_log(idx, f"Friday finalize logic error: {e}")

                self._cancel_api_timers_for(idx)    # << ใส่บรรทัดนี้


                # ปิด CSV ถ้ายังเป็นของโปรแกรมนี้
                if self.tele_owner_idx == idx:
                    self._stop_telemetry()
                    self.record_var.set(False)
                    self._sched_log(idx, "CSV STOP (end of schedule)")

                v["active_thread"] = None
                v["oneshot_stop"] = None

                with self.active_program_lock:
                    if self.active_program_idx == idx:
                        self.active_program_idx = None

                # ตรวจโหมดโปรแกรมปัจจุบัน
                mode_now = self.programs[idx]["mode"].get().lower() if (0 <= idx < len(self.programs)) else "once"

                # เตรียมตัวแปรสำหรับดูว่ามีรอบถัดไปไหม
                next_s = None

                # ถ้าเป็นโหมดที่มีรอบหลายวัน (everyday หรือ select day) และยังไม่ถูกสั่งหยุด → คำนวณรอบถัดไป
                # if not v["manager_stop"].is_set() and mode_now in ("everyday", "select day"):
                if not v["manager_stop"].is_set() and mode_now in ("everyday", "weekdays", "selectday"):
                    try:
                        next_s, _ = self.compute_next_occurrence(idx, datetime.now(TZ))
                    except Exception as e:
                        self._sched_log(idx, f"คำนวณรอบถัดไปไม่สำเร็จ: {e}")
                        next_s = None

                    if next_s:
                        # ข้อความหน้า UI ให้ต่างกันเล็กน้อย
                        if mode_now == "everyday":
                            state_txt = f"Done (next run {next_s.strftime('%Y-%m-%d %H:%M')})"
                        else:  # select day
                            state_txt = f"Done (next selected day {next_s.strftime('%Y-%m-%d %H:%M')})"
                    else:
                        state_txt = "Done"
                else:
                    # โหมด Once หรือถูกสั่งหยุด → จบแค่รอบนี้
                    state_txt = "Done"

                self._ui_update_prog(idx, done, total, state_txt)


                # เงื่อนไขออกจาก manager loop:
                # 1) มีการสั่งหยุด (Stop Program / Stop All)
                # 2) ไม่มีรอบถัดไปแล้ว (next_s เป็น None)  → เช่น Select Day ครบทุกวันที่เลือกแล้ว / Once ทำครบแล้ว
                if v["manager_stop"].is_set() or next_s is None:
                    break


            self._sched_log(idx, "MANAGER STOP")

        v["runner"] = threading.Thread(target=runner, daemon=True)
        v["runner"].start()

    def stop_program(self, idx: int):
        if idx < 0 or idx >= len(self.programs):
            return
        v = self.programs[idx]

        # ยกเลิก timer auto roof ของโปรแกรมนี้
        try:
            self._cancel_api_timers_for(idx)
        except Exception:
            pass

        try:
            v["paused"].clear()
        except Exception:
            pass

        self._reset_monday_warmup_state(idx)

        # 1) หยุดเธรดผู้จัดการ
        if v.get("manager_stop") is not None:
            v["manager_stop"].set()

        if v.get("runner") and v["runner"].is_alive():
            # ไม่ต้อง join นาน ปล่อยให้จบเองเพราะมี fr.join อยู่ใน runner
            pass
        v["runner"] = None

        # 2) หยุด one-shot รอบที่กำลังทำงาน (ถ้ามี)
        if v.get("oneshot_stop") is not None:
            try:
                v["oneshot_stop"].set()
            except Exception:
                pass

        th = v.get("active_thread")
        if th and getattr(th, "is_alive", lambda: False)():
            try:
                th.join(timeout=0.8)
            except Exception:
                pass
        v["active_thread"] = None
        v["oneshot_stop"] = None

        # 3) อัปเดต UI
        self._ui_update_prog(idx, 0, 0, "Stopped")

        self._sched_log(idx, "Stop Program pressed → stop scheduler")

        # 4) บังคับ STANDBY และเคลียร์สถานะในแอป
        try:
            self._send("$STANDBY")
            self._sched_log(idx, "Force → $STANDBY")
        except Exception as e:
            self._sched_log(idx, f"Force STANDBY failed: {e}")

        if not getattr(self, "_batch_stopping", False):
            try:
                self._schedule_roof_close_if_open("Stop Program")
            except Exception:
                pass

        with self.manual_lock:
            self.is_firing = False
        self._append_status_point(0)

        with self.active_program_lock:
            if self.active_program_idx == idx:
                self.active_program_idx = None

        self._set_program_editable(v, True)
        self._sched_log(idx, "Program unlocked (Stop)")

        if self.tele_thread and self.tele_thread.is_alive() and self.tele_owner_idx == idx:
            self._stop_telemetry()
            self.record_var.set(False)
            self._sched_log(idx, "CSV STOP (by Stop Program)")
        

    def start_all(self):
        for i, v in enumerate(self.programs):
            # ถ้ากำลังรันอยู่ ให้ข้าม (ไม่ restart)
            if v.get("runner") and v["runner"].is_alive():
                self._sched_log(i, "Start All: already running → skip")
                continue
            self.start_program(i)

    def stop_all_programs(self):
        """หยุดทุกโปรแกรม + ยกเลิก timer ทั้งหมด แล้วค่อยเช็คปิดหลังคาหลัง 5 วินาที"""
        # บอก stop_program ว่ากำลังหยุดแบบ batch จะได้ไม่สั่งปิดหลังคาซ้ำหลายครั้ง
        self._batch_stopping = True
        try:
            # หยุดโปรแกรมทุกตัว (จะส่ง STANDBY + หยุด CSV ผ่าน stop_program)
            for i in reversed(range(len(self.programs))):
                self.stop_program(i)
        finally:
            self._batch_stopping = False

        # ยกเลิก timer auto roof ของทุกโปรแกรม
        # for i in range(len(self.programs)):
        for i in reversed(range(len(self.programs))):
            try:
                self._cancel_api_timers_for(i)
            except Exception:
                pass

        with self.active_program_lock:
            self.active_program_idx = None

        # หลังจากหยุดหมดแล้ว ถ้าหลังคายังเปิดอยู่ → สั่งปิดหลัง 5 วินาที
        try:
            self._schedule_roof_close_if_open("Stop All")
        except Exception:
            pass

    def remove_all_programs(self):
        # หยุดทั้งหมดก่อน (จะส่ง STANDBY และหยุด CSV ผ่าน stop_program ของแต่ละโปรแกรม)
        self.stop_all_programs()

        # ลบจากท้ายมาหน้าเพื่อหลีกเลี่ยง index shift
        for i in reversed(range(len(self.programs))):
            self.remove_program(i)

        # เพื่อความชัวร์ ปิดบันทึก CSV/เจ้าของให้ว่าง (เผื่อไม่มีโปรแกรมไหนเป็นเจ้าของอยู่แล้ว)
        try:
            self._stop_telemetry()
        except Exception:
            pass
        self.tele_owner_idx = None
        if hasattr(self, "record_var"):
            self.record_var.set(False)

    # ---------- Config ----------
    def save_config(self):
        try:
            data = {
                "ip": self.ip_var.get().strip(),
                "port": int(self.port_var.get()),
                "user": self.user_var.get().strip(),
                "qsdelay": self.qsdelay_var.get().strip(),
                "freq": self.freq_var.get().strip(),
                "roof_api_base": getattr(self, "roof_api_base", ""),
                "limit_api_url": getattr(self, "limit_api_url", ""),
                "rain_enabled":      bool(getattr(self, "rain_enabled", True)),
                "sensor_enabled":    bool(getattr(self, "sensor_enabled", True)),
                "sensor_api_url":    getattr(self, "sensor_api_url", ""),
                "sensor_api_timeout": float(getattr(self, "sensor_api_timeout", 5.0)),
                "sensor_poll_interval": int(getattr(self, "sensor_poll_interval", 5)),
                "sensor_stale_sec":  float(getattr(self, "_sensor_stale_sec", 30.0)),
                "rain_api_url":      getattr(self, "rain_api_url", ""),
                "rain_api_timeout":  float(getattr(self, "rain_api_timeout", 2.5)),
                "rain_stale_sec":    float(getattr(self, "rain_stale_sec", 10.0)),
                "rain_poll_interval": int(getattr(self, "rain_poll_interval", 1)),
                "log_dir": getattr(self, "log_dir", LOG_DIR),
                "safety_fire_enabled": bool(self._is_safety_fire_enabled()),
                "roof_auto_ctrl_enabled": bool(self.roof_auto_ctrl_var.get()),  # FIX: เพิ่ม save
                "max_ltemf": float(self.max_temp_var.get()),
                "max_dtemf": float(self.max_dtemf_var.get()),
                "monday_warmup_enabled": bool(self.monday_warmup_enabled_var.get()),
                "monday_warmup_lead_min": int(self.monday_warmup_lead_min_var.get()),
                "monday_warmup_threshold": float(self.monday_warmup_threshold_var.get()),
                "prefire_open_sec": float(getattr(self, "roof_preopen_sec", 15)),
                "postrest_close_sec": float(getattr(self, "roof_postclose_sec", 3)),
                "programs": []
            }
            for v in self.programs:
                item = {
                    "name": v["name"].get().strip() if v.get("name") else "",
                    "enabled": bool(v["enabled"].get()),
                    "mode": v["mode"].get().lower(),  
                    "start": v["start"].get(),
                    "end": v["end"].get(),
                    "fire_ms": self._minutes_text_to_ms(v["fire_ms"].get()),
                    "rest_ms": self._minutes_text_to_ms(v["rest_ms"].get()),

                }
                if item["mode"] == "once":
                    item["once_date"] = v["once_date"].get()
                elif item["mode"] == "selectday":
                    item["dates"] = [d.isoformat() for d in sorted(v["sel_dates"])]
                data["programs"].append(item)
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.log("บันทึกการตั้งค่าแล้ว")
        except Exception as e:
            self.log(f"บันทึกการตั้งค่าล้มเหลว: {e}")

    def _load_config_into_ui(self):
        global LOG_DIR

        if not os.path.exists(CONFIG_FILE):
            return

        # ---------- load json ----------
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self.log(f"อ่านการตั้งค่าล้มเหลว: {e}")
            return

        try:
            # ---------- Main tab ----------
            self.ip_var.set(data.get("ip", self.ip_var.get()))
            self.port_var.set(int(data.get("port", self.port_var.get())))
            self.user_var.set(data.get("user", self.user_var.get()))
            self.qsdelay_var.set(data.get("qsdelay", self.qsdelay_var.get()))
            self.freq_var.set(data.get("freq", self.freq_var.get()))

            # ---------- API / Logs tab ----------
            self.roof_api_base = data.get(
                "roof_api_base",
                getattr(self, "roof_api_base", "")
            )
            self.limit_api_url = data.get(
                "limit_api_url",
                getattr(self, "limit_api_url", "")
            )
            self.rain_enabled      = bool(data.get("rain_enabled", getattr(self, "rain_enabled", True)))
            self.sensor_enabled    = bool(data.get("sensor_enabled", getattr(self, "sensor_enabled", True)))
            self.sensor_api_url    = data.get("sensor_api_url", getattr(self, "sensor_api_url", ""))
            self.sensor_api_timeout = float(data.get("sensor_api_timeout", getattr(self, "sensor_api_timeout", 5.0)))
            self.sensor_poll_interval = int(data.get("sensor_poll_interval", getattr(self, "sensor_poll_interval", 5)))
            self._sensor_stale_sec = float(data.get("sensor_stale_sec", getattr(self, "_sensor_stale_sec", 30.0)))
            self.rain_api_url      = data.get("rain_api_url", getattr(self, "rain_api_url", ""))
            self.rain_api_timeout  = float(data.get("rain_api_timeout",  getattr(self, "rain_api_timeout", 2.5)))
            self.rain_stale_sec    = float(data.get("rain_stale_sec",    getattr(self, "rain_stale_sec", 10.0)))
            self.rain_poll_interval = int(data.get("rain_poll_interval", getattr(self, "rain_poll_interval", 1)))
            self.log_dir = data.get(
                "log_dir",
                getattr(self, "log_dir", LOG_DIR)
            )
            self.roof_preopen_sec = float(data.get("prefire_open_sec", getattr(self, "roof_preopen_sec", 15)))
            self.roof_postclose_sec = float(data.get("postrest_close_sec", getattr(self, "roof_postclose_sec", 3)))

            # FIX: load roof_auto_ctrl_var
            roof_auto_ctrl = bool(data.get("roof_auto_ctrl_enabled", True))
            if hasattr(self, "roof_auto_ctrl_var"):
                self.roof_auto_ctrl_var.set(roof_auto_ctrl)

            # load temp thresholds
            if hasattr(self, "max_temp_var"):
                self.max_temp_var.set(float(data.get("max_ltemf", 32.5)))
            if hasattr(self, "max_dtemf_var"):
                self.max_dtemf_var.set(float(data.get("max_dtemf", 35.0)))

            # sync vars (กรณี UI tab 2 ถูก build แล้ว)
            if hasattr(self, "roof_api_base_var"):
                self.roof_api_base_var.set(self.roof_api_base)
            if hasattr(self, "limit_api_url_var"):
                self.limit_api_url_var.set(self.limit_api_url)
            if hasattr(self, "rain_enabled_var"):
                self.rain_enabled_var.set(self.rain_enabled)
            if hasattr(self, "sensor_enabled_var"):
                self.sensor_enabled_var.set(self.sensor_enabled)
                self.sensor_api_url_var.set(self.sensor_api_url)
                self.sensor_timeout_var.set(self.sensor_api_timeout)
                self.sensor_interval_var.set(self.sensor_poll_interval)
                self.sensor_stale_var.set(self._sensor_stale_sec)
            if hasattr(self, "rain_api_url_var"):
                self.rain_api_url_var.set(self.rain_api_url)
            # ถ้า config บอก disabled → หยุด poll และแสดง Disabled
            if not self.rain_enabled:
                self._rain_poll_stop = True
                try:
                    self.rain_status_var.set("Disabled")
                    self.rain_status_lbl.configure(foreground="gray")
                    self.rain_intensity_var.set("-")
                    self.rain_total_var.set("-")
                    self.rain_ts_var.set("-")
                except Exception:
                    pass
            if not self.sensor_enabled:
                self._sensor_poll_stop = True
                try:
                    for v in (self.sensor_in_temp_var, self.sensor_in_humi_var,
                              self.sensor_in_dew_var, self.sensor_out_temp_var,
                              self.sensor_out_humi_var, self.sensor_out_dew_var,
                              self.sensor_ts_var):
                        v.set("-")
                except Exception:
                    pass
            if hasattr(self, "rain_timeout_var"):
                self.rain_timeout_var.set(self.rain_api_timeout)
            if hasattr(self, "rain_stale_var"):
                self.rain_stale_var.set(self.rain_stale_sec)
            if hasattr(self, "rain_interval_var"):
                self.rain_interval_var.set(self.rain_poll_interval)
            if hasattr(self, "log_dir_var"):
                self.log_dir_var.set(self.log_dir)
            if hasattr(self, "prefire_open_sec_var"):
                self.prefire_open_sec_var.set(self.roof_preopen_sec)
            if hasattr(self, "postrest_close_sec_var"):
                self.postrest_close_sec_var.set(self.roof_postclose_sec)

            LOG_DIR = self.log_dir

            # ---------- Safety Fire (ตัวเดียว คุมทั้งระบบ) ----------
            enabled = bool(data.get("safety_fire_enabled", True))

            # runtime flag (เผื่อโค้ดเก่า)
            self.safety_fire_enabled = enabled

            monday_enabled = bool(data.get("monday_warmup_enabled", False))
            monday_lead_min = int(data.get("monday_warmup_lead_min", 30))
            monday_threshold = float(data.get("monday_warmup_threshold", 26.90))

            # UI variable (checkbox)
            if hasattr(self, "safety_fire_enabled_var"):
                self.safety_fire_enabled_var.set(enabled)
            else:
                self.safety_fire_enabled_var = tk.BooleanVar(value=enabled)

            if hasattr(self, "monday_warmup_enabled_var"):
                self.monday_warmup_enabled_var.set(monday_enabled)
            else:
                self.monday_warmup_enabled_var = tk.BooleanVar(value=monday_enabled)

            if hasattr(self, "monday_warmup_lead_min_var"):
                self.monday_warmup_lead_min_var.set(monday_lead_min)
            else:
                self.monday_warmup_lead_min_var = tk.IntVar(value=monday_lead_min)

            if hasattr(self, "monday_warmup_threshold_var"):
                self.monday_warmup_threshold_var.set(monday_threshold)
            else:
                self.monday_warmup_threshold_var = tk.DoubleVar(value=monday_threshold)

            try:
                self.log(
                    f"Config loaded: Safety Fire = {'ON' if enabled else 'OFF'} | "
                    f"Monday Warmup = {'ON' if monday_enabled else 'OFF'}"
                )
            except Exception:
                pass
            self._update_roof_auto_label()

            # load connection profiles (Connection Settings tab)
            if hasattr(self, "_cs_profiles"):
                profiles = data.get("conn_profiles", {})
                if isinstance(profiles, dict):
                    self._cs_profiles = profiles
                    self._cs_reload_profile_list()

        except Exception as e:
            self.log(f"โหลดค่า config เข้า UI ล้มเหลว: {e}")

    # ---- Sliding Roof helpers (moved HTTP to api_clients.py) ----
    def _roof_set_status(self, text: str):
        self.after(0, lambda: self.roof_status_var.set(f"Status: {text}"))

    def _on_roof_result(self, res: RoofResult):
        """callback จาก SlidingRoofClient (ทำงานใน thread) -> อัปเดต UI ผ่าน after()"""
        def apply():
            if res.ok and res.state in ("ON", "OFF"):
                try:
                    self._apply_roof_status(res.state)
                except Exception:
                    pass
            elif not res.ok:
                # cooldown กัน log "Roof API error" ซ้ำ — แสดงทุก 30s เท่านั้น
                now_ts = time.monotonic()
                last = getattr(self, "_last_roof_api_err_ts", 0.0)
                if now_ts - last >= 30.0:
                    self._last_roof_api_err_ts = now_ts
                    if hasattr(self, "log"):
                        self.log(f"Roof API error: {res.error} (suppress ถัดไป 30s)")
        self.after(0, apply)

    # ---- Sliding Roof public actions ----
    def roof_open(self):
        if self.is_raining_now():
            def _warn():
                try:
                    messagebox.showwarning(
                        "Rain Detected",
                        "Cannot open the roof while it is raining.\n"
                        "Please wait until rain stops."
                    )
                except Exception:
                    pass
            self.after(0, _warn)
            self.log("⛔ Roof OPEN blocked: Rain sensor reports active rainfall.")
            return
        self.roof_client.post_open(on_result=self._on_roof_result)

    def roof_close(self):
        self.roof_client.post_close(on_result=self._on_roof_result)

    def roof_refresh(self):
        self.roof_client.get_status(on_result=self._on_roof_result)

    def roof_toggle_auto(self):
        """เปิด/ปิดการ polling สถานะ roof/limit ทุก 1 วินาที"""
        want = self.roof_auto_var.get()
        if want:
            self._roof_poll_stop = False
            self._poll_roof_status()
        else:
            self._roof_poll_stop = True
            self._roof_polling = False

    def _external_on(self):
        try:
            self.roof_open()
            self.log("AUTO: Roof OPEN (prefire)")
        except Exception as e:
            self.log(f"AUTO: Roof OPEN failed :: {e}")

    def _external_off(self):
        try:
            self.roof_close()
            self.log("AUTO: Roof CLOSE (postrest)")
        except Exception as e:
            self.log(f"AUTO: Roof CLOSE failed :: {e}")

    def _update_roof_auto_label(self):
        try:
            pre = float(getattr(self, "roof_preopen_sec", 15))
            post = float(getattr(self, "roof_postclose_sec", 3))
            text = f"Enable auto open (T-{pre:g}s) / auto close (+{post:g}s)"
            if hasattr(self, "roof_auto_sched_cb"):
                self.roof_auto_sched_cb.config(text=text)
        except Exception:
            pass

    def _schedule_roof_close_if_open(self, reason: str = ""):
        # """
        # ปิดหลังคาอัตโนมัติหลัง 5 วินาที ถ้า roof_auto_ctrl_var = True เท่านั้น
        # """
        # ⚠ ถ้าไม่เปิดโหมดควบคุมหลังคาอัตโนมัติ → ไม่ทำงาน
        if not self.roof_auto_ctrl_var.get():
            self.log(f"Auto roof OFF → ไม่ปิดหลังคาอัตโนมัติ ({reason})")
            return

        try:
            state = self._get_roof_status_cached()
        except Exception:
            state = "N/A"

        if state == "ON":
            try:
                self.log(f"Roof still OPEN after {reason} → schedule CLOSE in 5s")
            except Exception:
                pass
            try:
                self.after(5000, lambda: self._delayed_roof_close())
            except Exception as e:
                try:
                    self.log(f"schedule roof_close failed ({reason}): {e}")
                except Exception:
                    pass
        else:
            self.log(f"Roof already closed ({state}) → ไม่ต้องสั่งปิด ({reason})")

    def _delayed_roof_close(self):
        # """เรียกปิดหลังคาหลังหน่วงเวลา"""
        try:
            self.log("Executing delayed roof_close ...")
            self.roof_close()
            self.log("roof_close executed successfully (after delay)")
        except Exception as e:
            self.log(f"roof_close error after delay: {e}")

    def _schedule_prefire_api(self, idx: int, start_dt: datetime) -> None:
        # cancel old
        t_old = self._prefire_timers.pop(idx, None)
        if t_old and getattr(t_old, 'is_alive', lambda: False)():
            try: t_old.cancel()
            except Exception: pass
        if not self.roof_auto_sched_var.get():
            return
        if not self.laser:              # ยังไม่ connect → ไม่ตั้ง
            return
        
        now = datetime.now(TZ)
        lead = (start_dt - now).total_seconds() - float(getattr(self, "roof_preopen_sec", 15))
        delay = max(0.0, lead)

        def _go():
            if self.roof_auto_sched_var.get():
                self._external_on()

            if not self._is_safety_fire_enabled():
                self.log("Safety Fire = OFF: Roof not ON (prefire popup suppressed, allow firing)")
                return
        
            if not self._wait_roof_on(timeout=12.0, interval=0.5):
                def _warn():
                    try:
                        self._warn_roof(
                            "Roof Closed!",
                            "Roof closed during laser firing.\nThe laser was stopped for safety."
                            )
                    except Exception:
                        pass

                try:
                    self.after(0, _warn)
                except Exception:
                    pass

                self.log("❌ ยกเลิกการยิงอัตโนมัติ: Roof ไม่เปิดตามกำหนดเวลา")
                return

        t = threading.Timer(delay, _go)
        t.daemon = True
        t.start()
        self._prefire_timers[idx] = t

    def _schedule_postrest_api(self, idx: int) -> None:
        t_old = self._postrest_timers.pop(idx, None)
        if t_old and getattr(t_old, 'is_alive', lambda: False)():
            try: t_old.cancel()
            except Exception: pass
        if not self.roof_auto_sched_var.get():
            return
        def _go():
            if self.roof_auto_sched_var.get():
                self._external_off()
        t = threading.Timer(float(getattr(self, "roof_postclose_sec", 3)), _go)
        t.daemon = True
        t.start()
        self._postrest_timers[idx] = t

    def _cancel_api_timers_for(self, idx: int) -> None:
        for d in (self._prefire_timers, self._postrest_timers):
            t = d.pop(idx, None)
            if t and getattr(t, 'is_alive', lambda: False)():
                try: t.cancel()
                except Exception: pass

    def on_close(self):
        try:
            self._roof_poll_stop = True
            self._rain_poll_stop = True
            self.stop_all_programs()
            self._stop_telemetry()
            if self.laser: self.laser.close()
        except Exception:
            pass
        self.destroy()

    def _ui_call(self, fn, *args, **kwargs):
        try:
            self.after(0, lambda: fn(*args, **kwargs))
        except Exception:
            pass

    def _show_overheat_popup_dual(self, triggered_by: str, trig_val: float, trig_max: float,
                                   other_val, other_max: float):
        """แสดง popup เตือนร้อนเกิน พร้อมแสดงค่าทั้ง DTEMF และ LTEMF"""
        other_name = "LTEMF" if triggered_by == "DTEMF" else "DTEMF"

        if getattr(self, "overheat_win", None) is None or not self.overheat_win.winfo_exists():
            self.overheat_win = tk.Toplevel(self)
            self.overheat_win.title("Overheat Warning")
            self.overheat_win.attributes("-topmost", True)
            self.overheat_win.geometry("400x220+120+120")
            self.overheat_win.resizable(False, False)

            frm = tk.Frame(self.overheat_win, bg="#8B0000", padx=14, pady=14)
            frm.pack(fill="both", expand=True)

            self.lbl_over_title = tk.Label(
                frm, text="⚠  Laser temperature exceeded limit!",
                font=("Segoe UI", 13, "bold"), fg="white", bg="#8B0000")
            self.lbl_over_title.pack(anchor="w")

            self.lbl_over_trig = tk.Label(
                frm, text="", font=("Consolas", 22, "bold"),
                fg="#FFD700", bg="#8B0000")
            self.lbl_over_trig.pack(anchor="center", pady=(6, 2))

            self.lbl_over_other = tk.Label(
                frm, text="", font=("Consolas", 14),
                fg="#FFB3B3", bg="#8B0000")
            self.lbl_over_other.pack(anchor="center", pady=(0, 6))

            self.lbl_over_hint = tk.Label(
                frm,
                text="STANDBY sent automatically.\nWindow closes when temperature drops below Max.",
                font=("Segoe UI", 9), fg="white", bg="#8B0000", justify="left")
            self.lbl_over_hint.pack(anchor="w")

            tk.Button(frm, text="Close", command=self._hide_overheat_popup,
                      cursor="hand2").pack(anchor="e", pady=(8, 0))

        # อัปเดตค่า
        try:
            self.lbl_over_trig.config(
                text=f"{triggered_by}: {trig_val:.2f} °C  (Max {trig_max:.2f} °C)  ← TRIGGERED")
            other_text = (f"{other_name}: {other_val:.2f} °C  (Max {other_max:.2f} °C)"
                          if other_val is not None else f"{other_name}: N/A")
            self.lbl_over_other.config(text=other_text)
        except Exception:
            pass

        try:
            self.overheat_win.deiconify()
            self.overheat_win.lift()
            self.overheat_win.attributes("-topmost", True)
        except Exception:
            pass

    def _show_overheat_popup(self, ltemf: float, maxv: float):
        """backward-compat wrapper"""
        self._show_overheat_popup_dual("LTEMF", ltemf, maxv, self.last_dtemf,
                                       self.max_dtemf_var.get())

    def _hide_overheat_popup(self):
        win = getattr(self, "overheat_win", None)
        if win is not None and win.winfo_exists():
            try:
                win.destroy()
            except Exception:
                pass
        self.overheat_win = None

    # ================================================================== #
    #  Rain Sensor – Multitask polling + control logic                    #
    # ================================================================== #

    def _fetch_rain_data(self, timeout: float = 3.0) -> dict | None:
        """GET /api/rain คืน dict หรือ None ถ้าล้มเหลว"""
        import urllib.request
        try:
            url = getattr(self, "rain_api_url", "").strip()
            if not url:
                return None
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
                return json.loads(raw)
        except Exception:
            return None

    def _format_rain_timestamp_utc7(self, ts: object) -> str:
        """Format rain API timestamp for display in UTC+7."""
        raw = str(ts or "").strip()
        if not raw:
            return "-"
        try:
            iso = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            dt = dt.astimezone(TZ)
            return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} (UTC+7)"
        except Exception:
            text = raw[:19].replace("T", " ")
            return f"{text} (UTC+7)" if text else "-"

    def _apply_rain_enabled(self, enabled: bool):
        """เปิด/ปิด Rain Sensor – เรียกได้จาก main thread เท่านั้น"""
        self.rain_enabled = enabled
        # sync ทั้งสอง checkbox (Main + Settings) ให้ตรงกัน
        try:
            self.rain_enabled_var.set(enabled)
        except Exception:
            pass

        if enabled:
            # เริ่ม poll ใหม่ถ้าหยุดอยู่
            self._rain_poll_stop = False
            self._rain_poll_inflight = False
            self._rain_last_ok_ts = None
            self._rain_fail_count = 0
            interval_ms = max(1, int(getattr(self, "rain_poll_interval", 1))) * 1000
            self.after(interval_ms, self._poll_rain_sensor)
            self.log("🌧 Rain Sensor: ENABLED")
            # คืนสถานะ UI
            self._show_rain_na()
        else:
            # หยุด poll
            self._rain_poll_stop = True
            self._rain_is_raining = False
            # แสดง UI ว่าปิดอยู่
            try:
                self.rain_status_var.set("Disabled")
                self.rain_status_lbl.configure(foreground="gray")
                self.rain_intensity_var.set("-")
                self.rain_total_var.set("-")
                self.rain_ts_var.set("-")
            except Exception:
                pass
            self.log("🌧 Rain Sensor: DISABLED")

    def _poll_rain_sensor(self):
        """เรียกทุก 1 วินาที – spawn daemon thread; ไม่บล็อก UI
        เมื่อ API ล้มเหลว: เก็บค่าล่าสุดไว้ (grace period 10 วิ) ก่อนแสดง N/A
        """
        if not getattr(self, "rain_enabled", True):
            return
        if getattr(self, "_rain_poll_stop", False):
            return
        if getattr(self, "_rain_poll_inflight", False):
            interval_ms = max(1, int(getattr(self, "rain_poll_interval", 1))) * 1000
            self.after(interval_ms, self._poll_rain_sensor)
            return

        self._rain_poll_inflight = True
        RAIN_STALE_SEC = float(getattr(self, "rain_stale_sec", 10.0))

        def worker():
            timeout = float(getattr(self, "rain_api_timeout", 2.5))
            data = self._fetch_rain_data(timeout=timeout)
            try:
                if data and data.get("ok"):
                    is_raining = bool(data.get("is_raining", False))
                    intensity  = float((data.get("intensity") or {}).get("value", 0.0))
                    total      = float(((data.get("accumulation") or {}).get("total") or {}).get("value", 0.0))
                    ts         = self._format_rain_timestamp_utc7(data.get("timestamp", ""))

                    prev_raining = getattr(self, "_rain_is_raining", False)

                    # บันทึก cache
                    self._rain_is_raining = is_raining
                    self._rain_intensity  = intensity
                    self._rain_total      = total
                    self._rain_last_ts    = ts
                    self._rain_last_ok_ts = time.monotonic()
                    self._rain_fail_count = 0

                    self.after(0, lambda r=is_raining, i=intensity, t=total, s=ts:
                               self._update_rain_ui(r, i, t, s, stale=False))

                    if is_raining and not prev_raining:
                        self.after(0, self._on_rain_started)

                else:
                    # API ล้มเหลว: อย่าลบค่าล่าสุดทันที
                    self._rain_fail_count = getattr(self, "_rain_fail_count", 0) + 1
                    last_ok = getattr(self, "_rain_last_ok_ts", None)
                    age = (time.monotonic() - last_ok) if last_ok is not None else float("inf")

                    if last_ok is None or age > RAIN_STALE_SEC:
                        self.after(0, self._show_rain_na)
                    else:
                        # grace period: แสดงค่าล่าสุด + ↻ บอกว่ากำลัง retry
                        r = getattr(self, "_rain_is_raining", False)
                        i = getattr(self, "_rain_intensity",  0.0)
                        t = getattr(self, "_rain_total",       0.0)
                        s = getattr(self, "_rain_last_ts",    "-")
                        self.after(0, lambda r=r, i=i, t=t, s=s:
                                   self._update_rain_ui(r, i, t, s, stale=True))
            except Exception:
                pass
            finally:
                self._rain_poll_inflight = False

        threading.Thread(target=worker, daemon=True, name="RainPollThread").start()
        interval_ms = max(1, int(getattr(self, "rain_poll_interval", 1))) * 1000
        self.after(interval_ms, self._poll_rain_sensor)

    def _show_rain_na(self):
        """แสดง N/A เมื่อข้อมูลขาดหายเกิน grace period"""
        try:
            self.rain_status_var.set("Rain: N/A")
            self.rain_status_lbl.configure(foreground="gray")
            self.rain_intensity_var.set("-")
            self.rain_total_var.set("-")
            self.rain_ts_var.set("-")
        except Exception:
            pass

    def _update_rain_ui(self, is_raining: bool, intensity: float, total: float,
                        ts: str, stale: bool = False):
        """อัปเดต Rain Sensor labels – เรียกจาก main thread เท่านั้น
        stale=True  → ยังแสดงค่าล่าสุด + ↻ (กำลัง retry)
        stale=False → ข้อมูลสด
        """
        try:
            retry_mark = " ↻" if stale else ""
            if is_raining:
                self.rain_status_var.set("\U0001f327 RAINING" + retry_mark)
                self.rain_status_lbl.configure(foreground="#2255cc" if stale else "blue")
            else:
                self.rain_status_var.set("\u2600 No Rain" + retry_mark)
                self.rain_status_lbl.configure(foreground="#448844" if stale else "green")
            self.rain_intensity_var.set(f"{intensity:.1f} mm/hr")
            self.rain_total_var.set(f"{total:.1f} mm")
            ts_text = ts + (" (retrying...)" if stale else "")
            try:
                self.rain_ts_var.set(ts_text)
            except Exception:
                pass
        except Exception:
            pass
    # ================================================================== #
    #  Temp & RH Sensor – polling + UI update                            #
    # ================================================================== #

    def _apply_sensor_enabled(self, enabled: bool):
        """เปิด/ปิด Temp & RH Sensor – เรียกจาก main thread เท่านั้น"""
        self.sensor_enabled = enabled
        try:
            self.sensor_enabled_var.set(enabled)
        except Exception:
            pass

        if enabled:
            self._sensor_poll_stop     = False
            self._sensor_poll_inflight = False
            self._sensor_last_ok_ts    = None
            interval_ms = max(1, int(getattr(self, "sensor_poll_interval", 5))) * 1000
            self.after(interval_ms, self._poll_sensor)
            self.log("🌡 Temp/RH Sensor: ENABLED")
            self._show_sensor_na()
        else:
            self._sensor_poll_stop = True
            for v in (self.sensor_in_temp_var, self.sensor_in_humi_var, self.sensor_in_dew_var,
                      self.sensor_out_temp_var, self.sensor_out_humi_var, self.sensor_out_dew_var,
                      self.sensor_ts_var):
                try: v.set("-")
                except Exception: pass
            self.log("🌡 Temp/RH Sensor: DISABLED")

    def _fetch_sensor_data(self, timeout: float = 5.0) -> dict | None:
        """GET /api/sensor คืน dict หรือ None"""
        import urllib.request
        try:
            url = getattr(self, "sensor_api_url", "").strip()
            if not url:
                return None
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8", errors="ignore"))
        except Exception:
            return None

    def _poll_sensor(self):
        """เรียกทุก N วินาที – ไม่บล็อก UI"""
        if not getattr(self, "sensor_enabled", True):
            return
        if getattr(self, "_sensor_poll_stop", False):
            return
        if getattr(self, "_sensor_poll_inflight", False):
            interval_ms = max(1, int(getattr(self, "sensor_poll_interval", 5))) * 1000
            self.after(interval_ms, self._poll_sensor)
            return

        self._sensor_poll_inflight = True
        stale_sec = float(getattr(self, "_sensor_stale_sec", 30.0))
        tout = float(getattr(self, "sensor_api_timeout", 5.0))

        def worker():
            data = self._fetch_sensor_data(timeout=tout)
            try:
                if data and data.get("ok"):
                    indoor  = data.get("indoor", {})  or {}
                    outdoor = data.get("outdoor", {}) or {}

                    it = float(indoor.get("temp",     0.0))
                    ih = float(indoor.get("humi",     0.0))
                    id_ = float(indoor.get("dewpoint", 0.0))
                    ot = float(outdoor.get("temp",     0.0))
                    oh = float(outdoor.get("humi",     0.0))
                    od = float(outdoor.get("dewpoint", 0.0))
                    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

                    self._sensor_in_temp  = it;  self._sensor_in_humi  = ih;  self._sensor_in_dew  = id_
                    self._sensor_out_temp = ot;  self._sensor_out_humi = oh;  self._sensor_out_dew = od
                    self._sensor_last_ok_ts = time.monotonic()

                    self.after(0, lambda: self._update_sensor_ui(
                        it, ih, id_, ot, oh, od, ts, stale=False))
                else:
                    age = (time.monotonic() - self._sensor_last_ok_ts)                           if self._sensor_last_ok_ts is not None else float("inf")
                    if self._sensor_last_ok_ts is None or age > stale_sec:
                        self.after(0, self._show_sensor_na)
                    else:
                        ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
                        it  = getattr(self, "_sensor_in_temp",  0.0)
                        ih  = getattr(self, "_sensor_in_humi",  0.0)
                        id_ = getattr(self, "_sensor_in_dew",   0.0)
                        ot  = getattr(self, "_sensor_out_temp", 0.0)
                        oh  = getattr(self, "_sensor_out_humi", 0.0)
                        od  = getattr(self, "_sensor_out_dew",  0.0)
                        self.after(0, lambda: self._update_sensor_ui(
                            it, ih, id_, ot, oh, od, ts, stale=True))
            except Exception:
                pass
            finally:
                self._sensor_poll_inflight = False

        threading.Thread(target=worker, daemon=True, name="SensorPollThread").start()
        interval_ms = max(1, int(getattr(self, "sensor_poll_interval", 5))) * 1000
        self.after(interval_ms, self._poll_sensor)

    def _update_sensor_ui(self, it: float, ih: float, id_: float,
                           ot: float, oh: float, od: float,
                           ts: str, stale: bool = False):
        """อัปเดต Temp/RH labels – main thread เท่านั้น"""
        retry = " ↻" if stale else ""
        try:
            self.sensor_in_temp_var.set(f"{it:.1f}°C")
            self.sensor_in_humi_var.set(f"{ih:.1f}%")
            self.sensor_in_dew_var.set(f"{id_:.1f}°C")
            self.sensor_out_temp_var.set(f"{ot:.1f}°C")
            self.sensor_out_humi_var.set(f"{oh:.1f}%")
            self.sensor_out_dew_var.set(f"{od:.1f}°C")
            self.sensor_ts_var.set(ts + (" (retrying...)" if stale else ""))
        except Exception:
            pass

    def _show_sensor_na(self):
        """แสดง N/A เมื่อเกิน stale threshold"""
        try:
            for v in (self.sensor_in_temp_var, self.sensor_in_humi_var, self.sensor_in_dew_var,
                      self.sensor_out_temp_var, self.sensor_out_humi_var, self.sensor_out_dew_var):
                v.set("N/A")
            self.sensor_ts_var.set("N/A")
        except Exception:
            pass


    def _on_rain_started(self):
        """
        เรียกเมื่อตรวจพบฝนใหม่ (เปลี่ยนจาก False → True):
          1. หยุดโปรแกรมตั้งเวลาทุกโปรแกรมที่กำลังรัน (pause)
          2. ถ้าหลังคาเปิดอยู่ → ปิดหลังคาทันที
        """
        self.log("🌧 Rain detected! Pausing all scheduled programs & closing roof if open.")

        # 1. Pause ทุกโปรแกรมที่กำลังรัน
        for idx, v in enumerate(self.programs):
            if v.get("runner") and v["runner"].is_alive():
                v["paused"].set()
                self._sched_log(idx, "⛔ Paused by Rain Sensor")
                try:
                    self.after(0, lambda i=idx: self._ui_update_prog(i, 0, 0, "⛔ Paused (Rain)"))
                except Exception:
                    pass

        # หยุดเลเซอร์ถ้ากำลังยิงอยู่
        if getattr(self, "is_firing", False):
            try:
                self.tele_pause_until = time.monotonic() + 1.5
                with self.manual_lock:
                    self.is_firing = False
                self._send("$STANDBY")
                self.log("🌧 Rain: Laser STANDBY sent")
            except Exception as e:
                self.log(f"Rain: STANDBY error: {e}")

        # 2. ปิดหลังคาถ้าเปิดอยู่
        try:
            state = self._get_roof_status_cached()
            if state == "ON":
                self.roof_close()
                self.log("🌧 Rain: Roof CLOSE commanded immediately")
        except Exception as e:
            self.log(f"Rain: Roof close error: {e}")

        # แสดง warning ผ่าน main thread
        def _warn():
            try:
                now_txt = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
                messagebox.showwarning(
                    "Rain Detected!",
                    f"Rain sensor detected rainfall.\n"
                    f"All scheduled programs have been PAUSED.\n"
                    f"Roof has been closed (if it was open).\n\n"
                    f"Detected at: {now_txt} (UTC+7)\n\n"
                    "Programs will remain paused while it is raining.\n"
                    "Resume manually after rain stops."
                )
            except Exception:
                pass
        self.after(100, _warn)

    def is_raining_now(self) -> bool:
        """Helper – คืน True ถ้าเซ็นเซอร์รายงานว่ามีฝน และ Rain Sensor เปิดอยู่"""
        if not getattr(self, "rain_enabled", True):
            return False
        return bool(getattr(self, "_rain_is_raining", False))

    # # ---------- Roof/Limit status via API ----------
    def _fetch_limit_state(self, timeout=2.0) -> str:
        """GET limit/status แล้วคืนค่า 'ON'/'OFF' หรือ 'N/A' (ย้ายไป LimitStatusClient)"""
        try:
            return self.limit_client.fetch_state(timeout=timeout)
        except Exception:
            return "N/A"

    def _apply_roof_status(self, state: str):
        # โชว์ตรงตำแหน่ง Status: ... ในกล่อง Control Sliding Roof
        self.roof_status_var.set(state)
        # ทำสีให้อ่านง่าย
        if state == "ON":
            self.roof_status_lbl.configure(foreground="green")
        elif state == "OFF":
            self.roof_status_lbl.configure(foreground="red")
        else:
            self.roof_status_lbl.configure(foreground="gray")

    def _poll_roof_status(self):
        if getattr(self, "_roof_poll_stop", False):
            return

        # กัน request ซ้อน
        if getattr(self, "_limit_poll_inflight", False):
            self.after(2000, self._poll_roof_status)
            return

        self._limit_poll_inflight = True  # ต้อง set ก่อน start thread (กัน race)
        fail_count = getattr(self, "_limit_fail_count", 0)

        def on_result(state: str):
            try:
                self._roof_state_cached = state
                self._roof_state_ts = time.monotonic()
                self.after(0, lambda s=state: self._apply_roof_status(s))
                # reset fail count เมื่อสำเร็จ
                if state != "N/A":
                    self._limit_fail_count = 0
                else:
                    self._limit_fail_count = getattr(self, "_limit_fail_count", 0) + 1
            finally:
                self._limit_poll_inflight = False

        # async fetch — ไม่บล็อก thread, timeout 3s (server ช้าแต่ตอบ)
        self.limit_client.fetch_state_async(on_result=on_result, timeout=3.0)

        # backoff: 2s → 4s → 8s → max 20s เมื่อ fail ซ้ำ
        delay = min(2000 * (2 ** fail_count), 20000)
        self.after(delay, self._poll_roof_status)

    def _check_roof_status_now(self):
        # state = self._fetch_limit_state()
        state = self._get_roof_status_cached()
        self._apply_roof_status(state)
        # ถ้ามี logger ในแอป
        if hasattr(self, "log"):
            self.log(f"SlidingRoof Status = {state}")

    def _is_safety_fire_enabled(self) -> bool:
        """แหล่งเดียวของสถานะ Safety Fire (checkbox เป็นตัวจริง)"""
        try:
            if hasattr(self, "safety_fire_enabled_var"):
                return bool(self.safety_fire_enabled_var.get())
        except Exception:
            pass
        return bool(getattr(self, "safety_fire_enabled", True))
   
    def _guard_fire_by_roof(self, timeout=1.5) -> bool:
        if not self._is_safety_fire_enabled():
            return True
        """คืน True ถ้ายิงได้ (Roof = ON), คืน False ถ้ายิงไม่ได้ (เตือนผ่าน main thread)"""
        try:
            # state = self._fetch_limit_state(timeout=timeout)  # "ON"/"OFF"/"N/A"
            state = self._get_roof_status_cached()
        except Exception:
            state = "N/A"

        if state != "ON":
            # อัปเดต label สี/ข้อความ (ทำได้จาก thread ไหนก็ได้ แต่ให้ชัวร์เรียกผ่าน after)
            try:
                self.after(0, lambda s=state: self._apply_roof_status(s))
            except Exception:
                pass

            # เตือนผ่าน main thread เท่านั้น
            def _warn():
                messagebox.showwarning(
                    "Roof Closed",
                    "Laser firing is blocked.\nRoof status (DI1) = %s.\n\nPlease open the roof (Roof = ON)." % state
                )
            try:
                self.after(0, _warn)
            except Exception:
                pass

            return False

        return True
  
    def _safe_fire(self) -> bool:
        # """เรียกยิงแบบมีการ์ด ตรวจ Roof ก่อนเสมอ"""
        if not self._guard_fire_by_roof():
            return False
        try:
            self._send("$FIRE")  # ← ตรงนี้คือคำสั่งยิงเลเซอร์เดิมของคุณ
            return True
        except Exception as e:
            try:
                self.after(
                    0,
                    lambda err=str(e): messagebox.showerror(
                        "Fire Error",
                        f"สั่งยิงไม่สำเร็จ:\n{err}"
                    )
                )
            except Exception:
                pass
            return False

    def _wait_roof_on(self, timeout: float = 12.0, interval: float = 0.5) -> bool:
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            state = self._get_roof_status_cached()   # ใช้ cache แทนยิง API
            last = state
            if state == "ON":
                self.after(0, lambda s=state: self._apply_roof_status(s))
                return True
            time.sleep(interval)

        self.after(0, lambda s=(last or "N/A"): self._apply_roof_status(s))
        return False

    def _monitor_roof_during_fire(self):
        try:
            # ถ้าไม่ได้กำลังยิง ไม่ต้องตรวจ
            if not getattr(self, "is_firing", False):
                self.after(1000, self._monitor_roof_during_fire)
                return

            # ปิด safety => ไม่ enforce ระหว่างยิง
            if not self._is_safety_fire_enabled():
                self.after(1000, self._monitor_roof_during_fire)
                return

            # อ่านจาก cache เท่านั้น (ห้ามยิง API ตรง)
            state = self._get_roof_status_cached()

            if state == "OFF":
                # หยุดเลเซอร์ทันที
                try:
                    self.tele_pause_until = time.monotonic() + 1.5
                    with self.manual_lock:
                        self.is_firing = False

                    self.after(0, lambda: self._append_status_point(0))
                    self._send("$STANDBY")

                    self.log("⚠ Roof ปิดขณะยิง → สั่งหยุดเลเซอร์ทันที")
                    self.after(
                        0,
                        lambda: self._warn_roof(
                            "Roof Closed!",
                            "Roof closed during laser firing.\nThe laser was stopped immediately for safety."
                        )
                    )
                except Exception as e:
                    self.log(f"Error while stopping laser: {e}")

        except Exception as e:
            self.log(f"Roof monitor error: {e}")

        # ตรวจซ้ำทุก 1 วินาที (loop เดียว)
        self.after(1000, self._monitor_roof_during_fire)

    def _get_roof_status_cached(self) -> str:
        """คืนสถานะจาก cache; ถ้า cache เก่าเกินไปให้คืน N/A"""
        try:
            s = str(getattr(self, "_roof_state_cached", "N/A")).strip().upper()
            age = time.monotonic() - float(getattr(self, "_roof_state_ts", 0.0))
            if not s:
                return "N/A"
            # ถ้าไม่อัปเดตเกิน 5 วินาที ให้ถือว่าอ่านไม่ได้
            if age > 5.0:
                return "N/A"
            return s
        except Exception:
            return "N/A"

    def _warn_roof(self, title: str, message: str, cooldown_sec: float = 10.0):
        """เตือนเรื่อง roof โดยเคารพ Safety Fire + ใส่วันเวลา + กัน popup ซ้ำถี่ ๆ"""

        # ปิด safety => ไม่ต้องเตือน
        if not self._is_safety_fire_enabled():
            return

        import time
        from datetime import datetime

        # cooldown กันเด้งถี่
        now_ts = time.time()
        last = getattr(self, "_last_roof_warn_ts", 0.0)
        if (now_ts - last) < cooldown_sec:
            return
        self._last_roof_warn_ts = now_ts

        # เวลา ณ ตอนตรวจพบ (พยายามใช้ TZ ของแอป ถ้ามี)
        try:
            tz = getattr(self, "TZ", None)  # ถ้าคุณมีตัวแปร TZ อยู่ในคลาส
            dt = datetime.now(tz) if tz else datetime.now()
            ts_text = dt.strftime("%Y-%m-%d %H:%M:%S")
            if tz:
                # ถ้า TZ มีอยู่ โดยทั่วไปคุณแสดง (UTC+7) อยู่แล้ว
                ts_text += " (UTC+7)"
        except Exception:
            ts_text = ""

        msg = message
        if ts_text:
            msg = f"{message}\n\nDetected at: {ts_text}"

        try:
            messagebox.showwarning(title, msg)
        except Exception:
            pass

    def _patch_messagebox_with_timestamp(self):
        """Append timestamp to all messageboxes (once)."""
        import tkinter.messagebox as messagebox
        from datetime import datetime

        # กัน patch ซ้ำ
        if getattr(messagebox, "_ts_patched", False):
            return
        messagebox._ts_patched = True

        # เก็บของเดิมไว้
        messagebox._orig_showwarning = messagebox.showwarning
        messagebox._orig_showinfo = messagebox.showinfo
        messagebox._orig_showerror = messagebox.showerror

        def _now_text():
            try:
                tz = getattr(self, "TZ", None)
                dt = datetime.now(tz) if tz else datetime.now()
                s = dt.strftime("%Y-%m-%d %H:%M:%S")
                # ถ้าคุณใช้ TZ = UTC+7 อยู่แล้ว ให้แสดงท้ายด้วย
                if tz:
                    s += " (UTC+7)"
                return s
            except Exception:
                return ""

        def _append_ts(msg: str) -> str:
            # Avoid duplicates: if message already contains "Detected at", do not append.
            if not isinstance(msg, str):
                msg = str(msg)
            if "Detected at:" in msg:
                return msg
            ts = _now_text()
            if not ts:
                return msg
            return f"{msg}\n\nDetected at: {ts}"

        def showwarning(title, message, *args, **kwargs):
            return messagebox._orig_showwarning(title, _append_ts(message), *args, **kwargs)

        def showinfo(title, message, *args, **kwargs):
            return messagebox._orig_showinfo(title, _append_ts(message), *args, **kwargs)

        def showerror(title, message, *args, **kwargs):
            return messagebox._orig_showerror(title, _append_ts(message), *args, **kwargs)

        # override
        messagebox.showwarning = showwarning
        messagebox.showinfo = showinfo
        messagebox.showerror = showerror


if __name__ == "__main__":
    app = App()
    app.mainloop()
