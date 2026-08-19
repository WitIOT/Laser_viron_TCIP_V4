# app3.py
from __future__ import annotations
import socket, threading, queue, time, csv, os, re, json, calendar
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone, date
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        TZ = ZoneInfo("Asia/Bangkok")
    except ZoneInfoNotFoundError:
        TZ = timezone(timedelta(hours=7))
except Exception:
    TZ = timezone(timedelta(hours=7))

import tkinter as tk
from tkinter import ttk, messagebox

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from tkinter import messagebox
import re  # ใช้ parse ค่าตัวเลขจากคำตอบ LTEMF?


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
    def try_send_cmd(self, cmd: str, call_timeout: float | None = None) -> str | None:
        if not self.sock:
            raise RuntimeError("Not connected")

        # พยายามจับล็อกแบบไม่บล็อก
        locked = self.lock.acquire(blocking=False)
        if not locked:
            return None  # BUSY: มีคนใช้อยู่ (เช่น Telemetry/คำสั่งอื่น)

        try:
            # ตั้ง timeout เฉพาะครั้งนี้ (สั้นๆ เพื่อลดเวลาถือครองล็อก)
            s = self.sock
            orig_to = self.timeout
            if call_timeout is not None:
                s.settimeout(call_timeout)
            else:
                s.settimeout(orig_to)

            s.sendall((cmd.strip() + "\n").encode())
            chunks = []
            try:
                while True:
                    b = s.recv(1024)
                    if not b:
                        break
                    chunks.append(b)
                    if b.endswith(b"\n"):
                        break
            except socket.timeout:
                # ปล่อยตามมีตามเกิด: ถ้าหมดเวลาจะคืนสิ่งที่อ่านได้ (ถ้ามี)
                pass

            # คืนค่า และกู้คืน timeout เดิม
            return b"".join(chunks).decode(errors="ignore").strip()
        finally:
            try:
                # กู้คืน timeout เดิม (กัน side effect)
                if call_timeout is not None and self.sock:
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

        ready = "Not Ready" if state & 0b00000001 else "Ready"
        return f"{mode} ({ready})"


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
            self.sock.sendall((cmd.strip() + "\n").encode())
            self.sock.settimeout(self.timeout)
            chunks = []
            try:
                while True:
                    b = self.sock.recv(1024)
                    if not b: break
                    chunks.append(b)
                    if b.endswith(b"\n"): break
            except socket.timeout:
                pass
            return b"".join(chunks).decode(errors="ignore").strip()


# ---------------- One-shot Scheduler (single occurrence) ----------------
class FireRestScheduler(threading.Thread):
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
        on_done=None,
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
        ttk.Button(frm_bot, text="วันนี้", command=self.go_today).pack(side=tk.LEFT)
        ttk.Button(frm_bot, text="ยกเลิก", command=self.cancel).pack(side=tk.RIGHT)
        ttk.Button(frm_bot, text="ตกลง", command=self.ok).pack(side=tk.RIGHT, padx=6)

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
        # self.roof_api_base = "http://192.168.3.154:8080/api/gpio/23"
        self.roof_api_base = "http://192.168.49.8:8000/door/"
        self._roof_polling = False
        self._roof_poll_stop = threading.Event()
        self.title("Laser Software v3.14")
        self.geometry("1460x1000")

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


        # --- Temp Control state (ต้องประกาศก่อน _build_ui) ---
        self.temp_ctl_enabled = tk.BooleanVar(value=False)
        self.max_temp_var     = tk.DoubleVar(value=32.5)
        self._temp_alarm_active = False

        self._batch_stopping = False
        self.roof_auto_var = tk.BooleanVar(value=False)

        # --- roof auto flag (ป้องกัน AttributeError) ---
        self.roof_auto_var = tk.BooleanVar(value=False)

        self.roof_auto_ctrl_var = tk.BooleanVar(value=True)  # เปิดอัตโนมัติ/ปิดตามสถานะเลเซอร์
        self.roof_preopen_sec = 15  # เปิดก่อน FIRE กี่วินาที

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
        
        self._roof_poll_stop = False
        self._poll_roof_status()
        self._monitor_roof_during_fire()

        

        # Load saved settings
        self._load_config_into_ui()
        if not self.programs:  # อย่างน้อย 1 โปรแกรม
            self.add_program()

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # ---------- UI ----------
    def _build_ui(self):
        root = ttk.Frame(self); root.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Connection
        conn = ttk.LabelFrame(root, text="การเชื่อมต่อ")
        conn.grid(row=0, column=0, columnspan=2, sticky="nwe", padx=5, pady=5)
        ttk.Label(conn, text="IP").grid(row=0, column=0)
        self.ip_var = tk.StringVar(value="127.0.0.1")
        ttk.Entry(conn, textvariable=self.ip_var, width=16).grid(row=0, column=1, padx=5)
        ttk.Label(conn, text="Port").grid(row=0, column=2)
        self.port_var = tk.IntVar(value=2323)
        ttk.Entry(conn, textvariable=self.port_var, width=8).grid(row=0, column=3, padx=5)
        ttk.Label(conn, text="User").grid(row=0, column=4)
        self.user_var = tk.StringVar(value="VR70AB07")
        ttk.Entry(conn, textvariable=self.user_var, width=14).grid(row=0, column=5, padx=5)
        ttk.Button(conn, text="Connect", command=self.connect).grid(row=0, column=6, padx=5)
        ttk.Button(conn, text="Disconnect", command=self.disconnect).grid(row=0, column=7, padx=5)
        self.conn_status = ttk.Label(conn, text="Disconnected", foreground="red")
        self.conn_status.grid(row=0, column=8, padx=10)

        # Manual controls
        man = ttk.LabelFrame(root, text="ควบคุมด้วยมือ")
        man.grid(row=1, column=0, sticky="nwe", padx=5, pady=5)
        ttk.Button(man, text="FIRE", command=self.cmd_fire, width=10).grid(row=0, column=0, padx=5, pady=2)
        
        ttk.Button(man, text="STANDBY", command=self.cmd_standby, width=10).grid(row=0, column=1, padx=5, pady=2)
        # ttk.Button(man, text="TEMP?", command=self.cmd_temp, width=10).grid(row=0, column=2, padx=5, pady=2)
        ttk.Button(man, text="STOP", command=self.cmd_stop, width=10).grid(row=0, column=3, padx=5, pady=2)

        self.laser_status_var = tk.StringVar(value="Laser: -")
        ttk.Label(conn, textvariable=self.laser_status_var, foreground="blue").grid(
            row=2, column=0, columnspan=9, sticky="w", padx=10, pady=4
        )

        
        # Telemetry
        tele = ttk.LabelFrame(root, text="Telemetry – DTEMF / LTEMF")
        tele.grid(row=1, column=1, sticky="nwe", padx=5, pady=5)
        ttk.Label(tele, text="DTEMF:").grid(row=0, column=0, sticky="e")
        self.lbl_dtemf = ttk.Label(tele, text="-"); self.lbl_dtemf.grid(row=0, column=1, sticky="w", padx=5)
        ttk.Label(tele, text="LTEMF:").grid(row=0, column=2, sticky="e")
        self.lbl_ltemf = ttk.Label(tele, text="-"); self.lbl_ltemf.grid(row=0, column=3, sticky="w", padx=5)
        self.record_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(tele, text="บันทึก CSV", variable=self.record_var, command=self._toggle_telemetry)\
            .grid(row=1, column=0, sticky="w", pady=4)
        ttk.Label(tele, text="ไฟล์:").grid(row=1, column=1, sticky="e")
        self.csv_name_var = tk.StringVar(value=self._default_csv_name())
        ttk.Entry(tele, textvariable=self.csv_name_var, width=44)\
            .grid(row=1, column=2, columnspan=2, sticky="we", padx=5)

        # Setting
        setting = ttk.LabelFrame(root, text="Setting")
        setting.grid(row=2, column=0, sticky="nwe", padx=5, pady=5)
        f_qs = ttk.Frame(setting); f_qs.pack(fill=tk.X, pady=2)
        ttk.Label(f_qs, text="QSDELAY (µs):").pack(side=tk.LEFT, padx=5)
        self.qsdelay_var = tk.StringVar(value="220")
        qs_entry = ttk.Entry(f_qs, textvariable=self.qsdelay_var, width=10); qs_entry.pack(side=tk.LEFT)
        ttk.Button(f_qs, text="Set", command=self.apply_qsdelay).pack(side=tk.LEFT, padx=4)
        ttk.Button(f_qs, text="QSDELAY?", command=self.cmd_qsdelay_query).pack(side=tk.LEFT, padx=2)
        ttk.Label(f_qs, text="recommend: 0 – 400", foreground="gray").pack(side=tk.LEFT, padx=8)
        qs_entry.bind("<Return>", lambda _: self.apply_qsdelay())

        f_df = ttk.Frame(setting); f_df.pack(fill=tk.X, pady=2)
        ttk.Label(f_df, text="Frequency (Hz):").pack(side=tk.LEFT, padx=5)
        self.freq_var = tk.StringVar(value="20")
        fr_entry = ttk.Entry(f_df, textvariable=self.freq_var, width=10); fr_entry.pack(side=tk.LEFT)
        ttk.Button(f_df, text="Set", command=self.apply_dfreq).pack(side=tk.LEFT, padx=4)
        ttk.Button(f_df, text="DFREQ?", command=self.cmd_dfreq_query).pack(side=tk.LEFT, padx=2)
        ttk.Label(f_df, text="recommend: 1 – 22", foreground="gray").pack(side=tk.LEFT, padx=8)
        fr_entry.bind("<Return>", lambda _: self.apply_dfreq())

        ttk.Button(setting, text="บันทึกค่า", command=self.save_config).pack(anchor="e", padx=5, pady=4)
        # ===== Temp Control (วางใน Setting) =====
        tempf = ttk.LabelFrame(setting, text="Temp Control")
        tempf.pack(fill=tk.X, padx=4, pady=4)

        ttk.Checkbutton(tempf, text="Enable", variable=self.temp_ctl_enabled)\
            .grid(row=0, column=0, padx=5, pady=5, sticky="w")

        ttk.Label(tempf, text="Max Temp (°C):")\
            .grid(row=0, column=1, padx=5, pady=5, sticky="e")
        ttk.Entry(tempf, textvariable=self.max_temp_var, width=8)\
            .grid(row=0, column=2, padx=5, pady=5, sticky="w")

        ttk.Label(tempf, text="เมื่อ LTEMF > Max จะสั่ง StandBy อัตโนมัติ")\
            .grid(row=1, column=0, columnspan=3, padx=5, pady=(0,5), sticky="w")

        # ---- Control Sliding Roof (under Setting) ----
        roof_group = ttk.LabelFrame(setting, text="Control Sliding Roof")
        roof_group.pack(fill=tk.X, padx=4, pady=4)
        # roof_group.grid(row=3, column=0, sticky="nwe", padx=4, pady=4)

        frm_roof = ttk.Frame(roof_group); frm_roof.pack(fill=tk.X, padx=6, pady=6)

        ttk.Button(frm_roof, text="Open", width=16,
                   command=self.roof_open).pack(side=tk.LEFT, padx=3)
        ttk.Button(frm_roof, text="Close", width=16,
                   command=self.roof_close).pack(side=tk.LEFT, padx=3)
        # ttk.Button(frm_roof, text="Refresh status", width=18,
        #            command=self.roof_refresh).pack(side=tk.LEFT, padx=3)

        # self.roof_status_var = tk.StringVar(value="Status: -")
        # ttk.Label(frm_roof, textvariable=self.roof_status_var).pack(side=tk.LEFT, padx=10)
        # ttk.Label(f_roof, text="Status: ").pack(side=tk.LEFT, padx=(12,0))

        self.roof_status_var = tk.StringVar(value="UNKNOWN")
        self.roof_status_lbl = ttk.Label(frm_roof, textvariable=self.roof_status_var)
        self.roof_status_lbl.pack(side=tk.LEFT, padx=2)

        ttk.Checkbutton(frm_roof, text="Enable auto open (T-15s) / auto close (+3s)", variable=self.roof_auto_sched_var).pack(side=tk.RIGHT, padx=6)

        # self.roof_auto_var = tk.BooleanVar(value=True)
        # ttk.Checkbutton(frm_roof, text="Auto-refresh (5s)",
        #                 variable=self.roof_auto_var,
        #                 command=self.roof_toggle_auto).pack(side=tk.RIGHT, padx=3)

        # Programs group
        prog_box = ttk.LabelFrame(root, text="ตั้งเวลา & รอบยิง/พัก (Programs)")
        prog_box.grid(row=2, column=1, sticky="nwe", padx=5, pady=5)

        toolbar = ttk.Frame(prog_box); toolbar.pack(fill=tk.X, pady=3)
        ttk.Button(toolbar, text="+ Add Program", command=self.add_program).pack(side=tk.LEFT, padx=4)
        ttk.Button(toolbar, text="Start All", command=self.start_all).pack(side=tk.LEFT, padx=4)
        ttk.Button(toolbar, text="Stop All", command=self.stop_all_programs).pack(side=tk.LEFT, padx=4)

        ttk.Button(toolbar, text="Remove All", command=self.remove_all_programs).pack(side=tk.LEFT, padx=4)

        # 👇 นาฬิกามุมขวา
        self.clock_var = tk.StringVar(value="เวลา: -")
        ttk.Label(toolbar, textvariable=self.clock_var).pack(side=tk.RIGHT, padx=6)

        self.prog_nb = ttk.Notebook(prog_box); self.prog_nb.pack(fill=tk.BOTH, expand=True)

        # Plots + Logs
        vis = ttk.Panedwindow(root, orient=tk.HORIZONTAL); vis.grid(row=3, column=0, columnspan=2, sticky="nswe", padx=5, pady=5)
        self.plot_frame = ttk.LabelFrame(vis, text="Realtime Charts")
        logs_container = ttk.LabelFrame(vis, text="Logs")
        vis.add(self.plot_frame, weight=3); vis.add(logs_container, weight=2)
        nb = ttk.Notebook(logs_container); nb.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        tab_all = ttk.Frame(nb); nb.add(tab_all, text="All except Schedule")
        ttk.Button(tab_all, text="Clear", command=self.clear_terminal).pack(anchor="ne", padx=6, pady=4)
        self.log_text = tk.Text(tab_all, height=16); self.log_text.pack(fill=tk.BOTH, expand=True)

        tab_sched = ttk.Frame(nb); nb.add(tab_sched, text="Schedule Logs")
        ttk.Button(tab_sched, text="Clear", command=self.clear_sched_terminal).pack(anchor="ne", padx=6, pady=4)
        self.sched_log_text = tk.Text(tab_sched, height=16); self.sched_log_text.pack(fill=tk.BOTH, expand=True)

        root.columnconfigure(0, weight=1); root.columnconfigure(1, weight=1)
        root.rowconfigure(3, weight=1)

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
        self.after(500, self._auto_update_status)


    # ----- Program Tab Builder -----
    def add_program(self, init_data: dict | None = None):
        idx = len(self.programs)

        tab = ttk.Frame(self.prog_nb)
        self.prog_nb.add(tab, text=f"Program {idx+1}")

        vars = {
            "enabled": tk.BooleanVar(value=True),
            "mode": tk.StringVar(value="everyday"),  # everyday / onlydates / once
            "start": tk.StringVar(value="16:30"),
            "end": tk.StringVar(value="16:50"),
            "fire_min": tk.IntVar(value=1),
            "rest_min": tk.IntVar(value=1),

            "once_date": tk.StringVar(value=date.today().isoformat()),
            "sel_dates": set(),  # only select date (set of date)
        }

        if init_data:
            vars["enabled"].set(bool(init_data.get("enabled", True)))
            vars["mode"].set(init_data.get("mode", "everyday"))
            vars["start"].set(init_data.get("start", "16:30"))
            vars["end"].set(init_data.get("end", "16:50"))
            vars["fire_min"].set(int(init_data.get("fire_min", 1)))
            vars["rest_min"].set(int(init_data.get("rest_min", 1)))
            if vars["mode"].get() == "once":
                d = init_data.get("once_date", date.today().isoformat())
                vars["once_date"].set(d)
            else:
                ds = init_data.get("dates", [])
                try:
                    vars["sel_dates"] = {date.fromisoformat(x) for x in ds}
                except Exception:
                    vars["sel_dates"] = set()

        # Row 0: enable + mode
        row0 = ttk.Frame(tab); row0.pack(fill=tk.X, pady=3)
        ttk.Checkbutton(row0, text="Enable", variable=vars["enabled"]).pack(side=tk.LEFT, padx=4)

        ttk.Label(row0, text="โหมด").pack(side=tk.LEFT)
        mode_cb = ttk.Combobox(row0, textvariable=vars["mode"], width=16, state="readonly",
                               values=["Everyday","Select Day", "Once"])
        mode_cb.pack(side=tk.LEFT, padx=4)

        # Row 1: time + fire/rest
        row1 = ttk.Frame(tab); row1.pack(fill=tk.X, pady=3)
        ttk.Label(row1, text="เริ่ม (HH:MM)").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=vars["start"], width=8).pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="สิ้นสุด (HH:MM)").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=vars["end"], width=8).pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="ยิง (นาที)").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=vars["fire_min"], width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row1, text="พัก (นาที)").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=vars["rest_min"], width=6).pack(side=tk.LEFT, padx=4)

        # Row 2: date area by mode
        date_area = ttk.Frame(tab); date_area.pack(fill=tk.X, pady=3)
        vars["date_area"] = date_area

        # once UI
        once_frm = ttk.Frame(date_area)
        ttk.Label(once_frm, text="Once date:").pack(side=tk.LEFT)
        ttk.Label(once_frm, textvariable=vars["once_date"]).pack(side=tk.LEFT, padx=6)
        ttk.Button(once_frm, text="เลือกวัน", command=lambda v=vars: self.pick_once_date(v)).pack(side=tk.LEFT, padx=4)

        # onlydates UI
        only_frm = ttk.Frame(date_area)
        ttk.Label(only_frm, text="Selected dates:").pack(side=tk.LEFT)
        lbl = ttk.Label(only_frm, text="(0)"); lbl.pack(side=tk.LEFT, padx=6)
        vars["dates_label"] = lbl
        ttk.Button(only_frm, text="เลือกหลายวัน", command=lambda v=vars: self.pick_multi_dates(v)).pack(side=tk.LEFT, padx=4)

        vars["once_frm"] = once_frm
        vars["only_frm"] = only_frm

        # Row 3: preview + status + progress
        row2 = ttk.Frame(tab); row2.pack(fill=tk.X, pady=3)
        ttk.Button(row2, text="คำนวณจำนวนครั้ง",
                command=lambda i=idx: self.preview_cycles(i)).pack(side=tk.LEFT, padx=4)

        ttk.Button(row2, text="แสดงเวลา",
                command=lambda i=idx: self.preview_fire_times(i)).pack(side=tk.LEFT, padx=4)

        cyc = ttk.Label(row2, text="LOOP = -"); cyc.pack(side=tk.LEFT, padx=8)


        status = ttk.Label(row2, text="Idle")
        status.pack(side=tk.LEFT, padx=10)
        prog = ttk.Progressbar(row2, length=200, mode="determinate", maximum=1, value=0); prog.pack(side=tk.LEFT, padx=6)
        count = ttk.Label(row2, text="", foreground="gray"); count.pack(side=tk.LEFT, padx=6)

        vars["cycle_label"] = cyc
        vars["status_lbl"] = status
        vars["progbar"] = prog
        vars["count_lbl"] = count

        # Row 4: start/stop/remove
        row3 = ttk.Frame(tab); row3.pack(fill=tk.X, pady=3)
        ttk.Button(row3, text="Start Program", command=lambda i=idx: self.start_program(i)).pack(side=tk.LEFT, padx=4)
        ttk.Button(row3, text="Stop Program", command=lambda i=idx: self.stop_program(i)).pack(side=tk.LEFT, padx=4)
        ttk.Button(row3, text="Remove Program", command=lambda i=idx: self.remove_program(i)).pack(side=tk.LEFT, padx=4)

        # runtime state
        # vars["runner"] = None
        # vars["stop_event"] = threading.Event()
        # vars["active_thread"] = None
        # vars["tab"] = tab

        vars["runner"] = None             # เธรดผู้จัดการโปรแกรม
        vars["manager_stop"] = None       # Event หยุดผู้จัดการ
        vars["active_thread"] = None      # เธรด one-shot ที่กำลังทำงาน
        vars["oneshot_stop"] = None       # Event หยุด one-shot รอบปัจจุบัน
        vars["tab"] = tab

        self.programs.append(vars)

        # react to mode change
        def on_mode_change(_=None, v=vars):
            if v["mode"].get().lower() == "once":
                v["once_date"].set(date.today().isoformat())
            self._render_date_area(v)
        mode_cb.bind("<<ComboboxSelected>>", on_mode_change)
        self._render_date_area(vars)
        self._update_program_tab_titles()

        return idx

    def remove_program(self, idx: int):
        if idx < 0 or idx >= len(self.programs):
            return

        # หยุดโปรแกรมก่อนลบ
        self.stop_program(idx)

        # กันซ้ำ: ถ้า CSV ยังเป็นของโปรแกรมนี้ให้หยุด
        if self.tele_owner_idx == idx:
            self._stop_telemetry()
            self.record_var.set(False)
            self._sched_log(idx, "CSV STOP (remove program)")

        v = self.programs[idx]
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
                self.prog_nb.tab(v["tab"], text=f"Program {i+1}")
            except Exception:
                pass

    def _render_date_area(self, v: dict):
        for w in v["date_area"].winfo_children():
            w.pack_forget()
        mode = v["mode"].get().lower()
        if mode == "everyday":
            pass
        elif mode == "once":
            v["once_frm"].pack(fill=tk.X)
        else:  # onlydates
            cnt = len(v["sel_dates"])
            v["dates_label"].config(text=f"({cnt})")
            v["only_frm"].pack(fill=tk.X)

    def pick_once_date(self, v: dict):
        dlg = CalendarDialog(self, title="เลือกวัน (Once)", multi=False)
        if dlg.result:
            d = sorted(list(dlg.result))[0]
            v["once_date"].set(d.isoformat())

    def pick_multi_dates(self, v: dict):
        dlg = CalendarDialog(self, title="เลือกหลายวัน (Only select date)", multi=True, initial=v["sel_dates"])
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
            self.save_config()
        except Exception as e:
            messagebox.showerror("Connect failed", str(e))
            self.log(f"Connect failed: {e}")
            self.conn_status.config(text="Disconnected", foreground="red")

    def disconnect(self):
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
        if not self._guard_fire_by_roof():
            # ถ้าหลังคายังปิด (state != "ON") ให้ยกเลิกการยิง
            return
        self.tele_pause_until = time.monotonic() + 1.5
        with self.manual_lock:
            self.is_firing = True
        self._append_status_point(1)
        self._send("$FIRE")

        # กรณีไม่มี telemetry thread → โหมด Manual ล้วน (เหมือนเดิม)
        if not (self.tele_thread and self.tele_thread.is_alive()):
            stamp = datetime.now(TZ).strftime('%Y%m%d_%H%M%S')
            manual_csv = os.path.join(LOG_DIR, f"telemetry_manual_{stamp}.csv")
            self.csv_name_var.set(manual_csv)
            self.record_var.set(True)
            self._start_telemetry()
            self.log(f"CSV START (manual) → {manual_csv}")
        else:
            # มี telemetry thread อยู่แล้ว → ส่วนใหญ่คือ CSV ของ Timer
            if self.tele_owner_idx is not None:
                # เริ่มบันทึกไฟล์ Manual parallel โดยใช้ thread เดิม
                stamp = datetime.now(TZ).strftime('%Y%m%d_%H%M%S')
                manual_csv = os.path.join(LOG_DIR, f"telemetry_manual_{stamp}.csv")
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
        # อัปเดตสถานะในแอปให้หยุดยิงทันที (กราฟลง 0)
        with self.manual_lock:
            self.is_firing = False
        self._append_status_point(0)

        # หยุดการบันทึก CSV ถ้ากำลังอัดอยู่
        try:
            # ถ้าใช้แฟลก/เมธอดเดิมของโปรเจ็กต์ให้เรียกตรงนี้
            if getattr(self, "telemetry_running", False) or getattr(self, "tele_is_open", False):
                self._stop_telemetry()          # เมธอดหยุดเขียนไฟล์ CSV เดิมของโปรเจ็กต์
                # self._sched_log(None, "CSV STOP (manual STOP)")  # บันทึกลง log (ถ้ามี)
                self.log("CSV STOP (manual STOP)")

            # ปรับสถานะปุ่ม/ตัวแปร UI
            if hasattr(self, "record_var"):
                self.record_var.set(False)
            # ถ้าโปรเจ็กต์มีตัวเจ้าของไฟล์ CSV อยู่ ให้เคลียร์ทิ้ง
            if hasattr(self, "tele_owner_idx"):
                self.tele_owner_idx = None
        except Exception as e:
            # กันพังเงียบๆ แต่ยังคงส่ง STOP ต่อ
            print("STOP: CSV stop error:", e)

        # ส่งคำสั่ง STOP ไปยังเลเซอร์
        self._send("$STOP")


    def cmd_temp(self): self._send("$LTEMF ?")
    def cmd_qsdelay_query(self): self._send("$QSDELAY ?")
    def cmd_dfreq_query(self): self._send("$DFREQ ?")

    def apply_qsdelay(self):
        val = self.qsdelay_var.get().strip()
        if not re.fullmatch(r"\d+", val):
            messagebox.showerror("QSDELAY", "กรุณาใส่ตัวเลขจำนวนเต็ม (µs)"); return
        iv = int(val)
        if not (0 <= iv <= 400):
            messagebox.showwarning("QSDELAY", "ช่วงที่แนะนำ 0 – 400 µs")
        self._send(f"$QSDELAY {iv}")
        self.log(f"QSDELAY → {iv} µs")
        self.save_config()

    def apply_dfreq(self):
        raw = self.freq_var.get().strip()
        m = re.fullmatch(r"(?i)\s*([0-9]+(?:\.\d+)?)\s*([kKmM]?)\s*", raw)
        if not m:
            messagebox.showerror("Frequency", "รูปแบบไม่ถูกต้อง (เช่น 20 หรือ 1k)"); return
        val = float(m.group(1)); unit = m.group(2).lower()
        if unit == "k": val *= 1000
        elif unit == "m": val *= 1_000_000
        hz = int(val)
        if not (1 <= hz <= 22):
            messagebox.showwarning("Frequency", "ช่วงที่แนะนำ 1 – 22 Hz")
        self._send(f"$DFREQ {hz}")
        self.log(f"DFREQ → {hz} Hz")
        self.save_config()

    # ---------- Telemetry ----------
    def _default_csv_name(self) -> str:
        return os.path.join(LOG_DIR, f"telemetry_{datetime.now(TZ).strftime('%Y%m%d')}.csv")

    def _toggle_telemetry(self):
        if self.record_var.get(): self._start_telemetry()
        else: self._stop_telemetry()

    def _start_telemetry(self):
        if not self.laser:
            messagebox.showwarning("Telemetry", "ยังไม่ได้เชื่อมต่ออุปกรณ์")
            self.record_var.set(False); return

        path = self.csv_name_var.get().strip() or self._default_csv_name()
        if not os.path.isabs(path) and not path.startswith(LOG_DIR + os.sep):
            path = os.path.join(LOG_DIR, path)
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
            messagebox.showerror("CSV", f"ไม่สามารถสร้างไฟล์ CSV: {e}")
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
            roof_state = self._get_roof_status_cached()

            while not self.tele_stop.is_set():
                if time.monotonic() < self.tele_pause_until:
                    time.sleep(0.2)
                    continue

                ts = datetime.now(TZ).isoformat(timespec="seconds")
                d = self._query_float("$DTEMF ?")
                l = self._query_float("$LTEMF ?")

                if d is not None:
                    self.last_dtemf = d
                    self.lbl_dtemf.config(text=f"{d}")
                if l is not None:
                    self.last_ltemf = l
                    self.lbl_ltemf.config(text=f"{l}")

                with self.manual_lock:
                    status_num = 1 if self.is_firing else 0
                qs = self.qsdelay_var.get().strip()

                try:
                    maxv = float(self.max_temp_var.get())
                except Exception:
                    maxv = None

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

                # overload ตาม LTEMF > max
                overload = (l is not None and maxv is not None and l > maxv)

                self._append_telemetry_point(
                    float(d) if d is not None else None,
                    float(l) if l is not None else None
                )

                if overload:
                    self._ui_call(self._show_overheat_popup, float(l), float(maxv))
                else:
                    self._ui_call(self._hide_overheat_popup)

                try:
                    # เตรียม row เดียว ใช้ได้ทั้ง main CSV และ manual parallel
                    now = datetime.now(TZ)
                    date_str = now.strftime("%Y-%m-%d")
                    time_str = now.strftime("%H:%M:%S")
                    tz_str = now.tzname() or "UTC+7"

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


    def _query_float(self, cmd: str) -> float | None:
        try:
            if not self.laser:
                return None
            # ใช้ non-blocking + timeout สั้น เพื่อลดการกีดขวางคำสั่งควบคุม
            resp = self.laser.try_send_cmd(cmd, call_timeout=0.6)
            if resp is None:
                return None  # BUSY: มีคำสั่งสำคัญใช้งาน socket อยู่ → ข้ามรอบนี้
            self.msg_q.put(f">> {cmd}\n<< {resp}")
            m = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", resp)
            return float(m.group(0)) if m else None
        except Exception:
            return None
        
    def _query_float_quiet(self, cmd: str, timeout_s: float = 0.35) -> float | None:
        """อ่านค่า float แบบเบา ๆ: non-blocking, quiet (ไม่เขียน log), timeout สั้น เพื่อลดผลกระทบคำสั่งควบคุม"""
        try:
            if not self.laser:
                return None
            # เคารพการเว้นช่วงหลังสั่ง FIRE/STANDBY/STOP
            if time.monotonic() < self.tele_pause_until:
                return None
            resp = self.laser.try_send_cmd(cmd, call_timeout=timeout_s)
            if resp is None:
                return None  # BUSY → ไม่แย่ง lock กับคำสั่งควบคุม
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
        """เช็คอุณหภูมิเป็นระยะ ถ้าเกิน max -> STANDBY + popup (CSV ยังทำงานต่อ)"""
        try:
            if self.temp_ctl_enabled.get():
                val = self._query_ltemf()
                if val is not None:
                    maxv = float(self.max_temp_var.get())
                    hysteresis = 0.3  # กันเด้งซ้ำ

                    if val > maxv and not self._temp_alarm_active:
                        # ทริกครั้งแรก: ตั้งธง ป้องกันแจ้งซ้ำ
                        self._temp_alarm_active = True

                        # ✅ สั่ง STANDBY แบบ "ไม่หยุด CSV"
                        # (อย่าเรียก cmd_standby() เพราะมันหยุด CSV)
                        with self.manual_lock:
                            self.is_firing = False
                        self._append_status_point(0)
                        self._send("$STANDBY")

                        delay_ms = 5000
                        self.log(f"Over-Temp: LTEMF={val:.2f} > Max={maxv:.2f} → STANDBY, will close roof in {delay_ms/1000:.1f}s")
                        self.after(delay_ms, lambda: self._delayed_roof_close())   

                        # แจ้งเตือน
                        # self.log(f"Over-Temp: LTEMF={val:.2f} > Max={maxv:.2f} → STANDBY (CSV continues)")
                        try:
                            messagebox.showwarning("Over-Temperature",
                                                f"LTEMF = {val:.2f} °C > Max {maxv:.2f} °C\nระบบส่ง STANDBY แล้ว (ยังบันทึก CSV ต่อ)")
                        except Exception:
                            print(f"Over-Temperature: LTEMF={val:.2f} °C > {maxv:.2f} °C (STANDBY sent)")

                    elif val <= (maxv - hysteresis) and self._temp_alarm_active:
                        # อุณหภูมิลดลงพอแล้ว: เคลียร์ธงเพื่อให้แจ้งได้อีกครั้งหากเกินซ้ำ
                        self._temp_alarm_active = False
        finally:
            try:
                self.after(1000, self._temp_monitor_tick)
            except Exception:
                self.root.after(1000, self._temp_monitor_tick)



    # ---------- Logs & clock ----------
    def clear_terminal(self): self.log_text.delete("1.0", tk.END)
    def clear_sched_terminal(self): self.sched_log_text.delete("1.0", tk.END)

    def log(self, msg: str):
        stamp = datetime.now(TZ).strftime("%H:%M:%S")
        self.msg_q.put(f"[{stamp}] {msg}")

    def _sched_log(self, idx: int, msg: str):
        stamp = datetime.now(TZ).strftime("%H:%M:%S")
        self.msg_q.put(f"[SCHED#{idx+1}] [{stamp}] {msg}")

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
            self.clock_var.set(f"เวลา: {now.strftime('%Y-%m-%d %H:%M:%S')} (UTC{utc_off:+.0f})")

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
            fire_td = timedelta(minutes=int(v["fire_min"].get()))
            rest_td = timedelta(minutes=int(v["rest_min"].get()))
            n = FireRestScheduler.count_fire_cycles(start_dt, end_dt, fire_td, rest_td)
            v["cycle_label"].config(text=f"LOOP = {n} ครั้ง")
            self._sched_log(idx, f"Preview cycles: {start_dt} → {end_dt}, fire={fire_td}, rest={rest_td} → {n} ครั้ง")
        except Exception as e:
            messagebox.showerror("Invalid inputs", str(e))

    def preview_fire_times(self, idx: int):
        """แสดงเวลาที่ยิงตามช่วงเวลาเริ่ม/สิ้นสุด และค่า ยิง/พัก (ตัวอย่างใน 1 วัน)"""
        if idx < 0 or idx >= len(self.programs):
            return
        v = self.programs[idx]
        try:
            start_dt = self._parse_hhmm_into(date.today(), v["start"].get())
            end_dt = self._parse_hhmm_into(date.today(), v["end"].get())
            if end_dt <= start_dt:
                end_dt += timedelta(days=1)

            fire_td = timedelta(minutes=int(v["fire_min"].get()))
            rest_td = timedelta(minutes=int(v["rest_min"].get()))

            if fire_td.total_seconds() <= 0:
                raise ValueError("ระยะเวลายิงต้องมากกว่า 0 นาที")
            if rest_td.total_seconds() < 0:
                raise ValueError("ระยะเวลาพักต้องไม่ติดลบ")

            times = []
            cur = start_dt
            max_events = 500   # กันลิสต์ยาวเกิน
            while cur < end_dt and len(times) < max_events:
                times.append(cur)
                cur += fire_td + rest_td

            if not times:
                messagebox.showinfo(
                    "Preview fire times",
                    "ไม่มีเวลายิงในช่วงที่กำหนด\nโปรดตรวจสอบเวลาเริ่ม/สิ้นสุด และ ยิง/พัก",
                )
                return

            lines = [f"{i+1:02d}) {t.strftime('%H:%M')}" for i, t in enumerate(times)]
            msg = "เวลาที่ยิงในหนึ่งช่วง (ตามค่าเริ่ม/สิ้นสุด ปัจจุบัน):\n\n" + "\n".join(lines)

            # ถ้าเยอะมาก แสดงเฉพาะ 100 แถวแรก
            if len(lines) > 100:
                msg += f"\n\n... ทั้งหมด {len(lines)} ครั้ง (แสดงเฉพาะ 100 ครั้งแรก)"

            messagebox.showinfo("Preview fire times", msg)
        except Exception as e:
            messagebox.showerror("Invalid inputs", str(e))

    def _update_prog_ui(self, idx: int, done: int, total: int, state: str):
        if idx < 0 or idx >= len(self.programs): return
        v = self.programs[idx]
        v["progbar"].configure(maximum=max(1, total), value=done)
        v["count_lbl"].config(text=f"{done} / {total} ครั้ง" if total > 0 else "")
        v["status_lbl"].config(text=state)

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
                # อยู่ในช่วงของวันนี้ → เริ่มทันที ณ เวลาปัจจุบัน และจบตอน e
                start_now = now_dt.replace(second=0, microsecond=0)
                return start_now, e
            else:
                # เลยช่วงของวันนี้แล้ว → ไปวันถัดไป
                return mk_se(today + timedelta(days=1))

        elif mode == "once":
            try:
                d = date.fromisoformat(v["once_date"].get())
            except Exception:
                return None, None
            s, e = mk_se(d)
            if s <= now_dt:
                return None, None
            return s, e

        else:  # onlydates
            if not v["sel_dates"]:
                return None, None
            for d in sorted(v["sel_dates"]):
                s, e = mk_se(d)
                if s > now_dt:
                    return s, e
            return None, None


    def start_program(self, idx: int):
        if idx < 0 or idx >= len(self.programs): 
            return
        v = self.programs[idx]
        if not v["enabled"].get():
            self._sched_log(idx, "โปรแกรมถูกปิดการทำงาน (Enable=OFF)")
            return

        # เคลียร์ของเก่า
        self.stop_program(idx)

        fire_m = int(v["fire_min"].get())
        rest_m = int(v["rest_min"].get())

        # ใช้ event ที่เก็บใน dict (จะได้สั่งหยุดจาก stop_program ได้)
        v["manager_stop"] = threading.Event()

        def runner():
            self._sched_log(idx, "MANAGER START")
            while not v["manager_stop"].is_set():
                # ป้องกันโดนลบระหว่างทำงาน
                if idx < 0 or idx >= len(self.programs):
                    self._sched_log(idx, "โปรแกรมถูกลบออกไปแล้ว (manager exit)")
                    break

                now_dt = datetime.now(TZ)
                s_dt, e_dt = self.compute_next_occurrence(idx, now_dt)

                # ตั้ง auto-open T-15s
                try:
                    self._schedule_prefire_api(idx, s_dt)
                except Exception as e:
                    self._sched_log(idx, f"ตั้ง auto-open ไม่สำเร็จ: {e}")

                if not s_dt:
                    self._sched_log(idx, "ไม่มีรอบถัดไป (จบโปรแกรมตามเงื่อนไข)")
                    break

                total = FireRestScheduler.count_fire_cycles(
                    s_dt, e_dt, timedelta(minutes=fire_m), timedelta(minutes=rest_m)
                )
                done = 0
                self._update_prog_ui(idx, done, total, f"Waiting (เริ่ม {s_dt.strftime('%Y-%m-%d %H:%M')})")

                # เริ่ม CSV ของโปรแกรมนี้
                stamp = s_dt.strftime('%Y%m%d_%H%M%S')
                csvname = os.path.join(LOG_DIR, f"telemetry_sched_P{idx+1}_{stamp}.csv")
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
                    self.tele_pause_until = time.monotonic() + 1.5
                    with self.manual_lock:
                        self.is_firing = True
                    done += 1
                    self._update_prog_ui(idx, done, total, f"Firing ({done}/{total})")
                    self._append_status_point(1)
                    self._send("$FIRE")

                def on_rest(is_last: bool = False):
                    # หน่วงป้อง telemetry overlap
                    self.tele_pause_until = time.monotonic() + 1.5

                    with self.manual_lock:
                        self.is_firing = False

                    # ---------- UI ----------
                    status_txt = f"Resting ({done}/{total})"
                    if is_last:
                        status_txt = f"Resting FINAL ({done}/{total})"

                    self._update_prog_ui(idx, done, total, status_txt)
                    self._append_status_point(0)

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
                        next_fire_start = datetime.now(TZ) + timedelta(minutes=rest_m)
                        if next_fire_start < e_dt:  
                            self._schedule_prefire_api(idx, next_fire_start)
                    except Exception as e:
                        self._sched_log(idx, f"ตั้ง auto-open รอบถัดไปไม่สำเร็จ: {e}")


                fr = FireRestScheduler(
                    start_time=s_dt,
                    end_time=e_dt,
                    fire_minutes=fire_m,
                    rest_minutes=rest_m,
                    on_fire=on_fire,
                    on_rest=on_rest,                 # ← ไม่ใช้ lambda แบบเดิมแล้ว
                    on_tick=lambda _now: None,
                    stop_event=local_stop,
                )

                v["active_thread"] = fr
                fr.start()
                fr.join()
                self._cancel_api_timers_for(idx)    # << ใส่บรรทัดนี้


                # ปิด CSV ถ้ายังเป็นของโปรแกรมนี้
                if self.tele_owner_idx == idx:
                    self._stop_telemetry()
                    self.record_var.set(False)
                    self._sched_log(idx, "CSV STOP (end of schedule)")

                v["active_thread"] = None
                v["oneshot_stop"] = None

                # ตรวจโหมดโปรแกรมปัจจุบัน
                mode_now = self.programs[idx]["mode"].get().lower() if (0 <= idx < len(self.programs)) else "once"

                # เตรียมตัวแปรสำหรับดูว่ามีรอบถัดไปไหม
                next_s = None

                # ถ้าเป็นโหมดที่มีรอบหลายวัน (everyday หรือ select day) และยังไม่ถูกสั่งหยุด → คำนวณรอบถัดไป
                if not v["manager_stop"].is_set() and mode_now in ("everyday", "select day"):
                    try:
                        next_s, _ = self.compute_next_occurrence(idx, datetime.now(TZ))
                    except Exception as e:
                        self._sched_log(idx, f"คำนวณรอบถัดไปไม่สำเร็จ: {e}")
                        next_s = None

                    if next_s:
                        # ข้อความหน้า UI ให้ต่างกันเล็กน้อย
                        if mode_now == "everyday":
                            state_txt = f"Done (รอรอบถัดไป {next_s.strftime('%Y-%m-%d %H:%M')})"
                        else:  # select day
                            state_txt = f"Done (รอวันถัดไปที่เลือก {next_s.strftime('%Y-%m-%d %H:%M')})"
                    else:
                        state_txt = "Done"
                else:
                    # โหมด Once หรือถูกสั่งหยุด → จบแค่รอบนี้
                    state_txt = "Done"

                self._update_prog_ui(idx, done, total, state_txt)

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
        self._update_prog_ui(idx, done=0, total=0, state="Stopped")
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

        # 5) หยุดบันทึก CSV ทันที (ถ้าเจ้าของไฟล์เป็นโปรแกรมนี้หรือมีการบันทึกอยู่)
        if self.tele_thread and self.tele_thread.is_alive():
            self._stop_telemetry()
            self.record_var.set(False)
            self._sched_log(idx, "CSV STOP (by Stop Program)")


    def start_all(self):
        for i in range(len(self.programs)):
            self.start_program(i)

    def stop_all_programs(self):
        """หยุดทุกโปรแกรม + ยกเลิก timer ทั้งหมด แล้วค่อยเช็คปิดหลังคาหลัง 5 วินาที"""
        # บอก stop_program ว่ากำลังหยุดแบบ batch จะได้ไม่สั่งปิดหลังคาซ้ำหลายครั้ง
        self._batch_stopping = True
        try:
            # หยุดโปรแกรมทุกตัว (จะส่ง STANDBY + หยุด CSV ผ่าน stop_program)
            for i in range(len(self.programs)):
                self.stop_program(i)
        finally:
            self._batch_stopping = False

        # ยกเลิก timer auto roof ของทุกโปรแกรม
        for i in range(len(self.programs)):
            try:
                self._cancel_api_timers_for(i)
            except Exception:
                pass

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
                "programs": []
            }
            for v in self.programs:
                item = {
                    "enabled": bool(v["enabled"].get()),
                    "mode": v["mode"].get().lower(),  
                    "start": v["start"].get(),
                    "end": v["end"].get(),
                    "fire_min": int(v["fire_min"].get()),
                    "rest_min": int(v["rest_min"].get()),
                }
                if item["mode"] == "once":
                    item["once_date"] = v["once_date"].get()
                elif item["mode"] == "onlydates":
                    item["dates"] = [d.isoformat() for d in sorted(v["sel_dates"])]
                data["programs"].append(item)
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.log("บันทึกการตั้งค่าแล้ว")
        except Exception as e:
            self.log(f"บันทึกการตั้งค่าล้มเหลว: {e}")

    def _load_config_into_ui(self):
        if not os.path.exists(CONFIG_FILE): return
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self.log(f"อ่านการตั้งค่าล้มเหลว: {e}")
            return

        try:
            self.ip_var.set(data.get("ip", self.ip_var.get()))
            self.port_var.set(int(data.get("port", self.port_var.get())))
            self.user_var.set(data.get("user", self.user_var.get()))
            self.qsdelay_var.set(data.get("qsdelay", self.qsdelay_var.get()))
            self.freq_var.set(data.get("freq", self.freq_var.get()))
        except Exception:
            pass

        # ล้างโปรแกรมเดิม (ถ้ามี)
        # หยุดทุก thread และลบ tab อย่างปลอดภัย
        for i in reversed(range(len(self.programs))):
            self.remove_program(i)

        # สร้างตาม config
        for p in data.get("programs", []):
            self.add_program(p)
    # ---- Sliding Roof helpers ----
    def _roof_set_status(self, text: str):
        self.after(0, lambda: self.roof_status_var.set(f"Status: {text}"))

    def _roof_http_post(self, url: str):
        def worker():
            try:
                req = urllib.request.Request(url, method="POST")
                req.add_header("Content-Type", "application/json")
                with urllib.request.urlopen(req, timeout=4) as resp:
                    data = resp.read().decode("utf-8", errors="ignore").strip()

                # พยายามดึงค่า ON/OFF จาก JSON
                try:
                    obj = json.loads(data)
                    msg = str(obj.get("message", "")).upper()
                    if msg in ["ON", "OFF"]:
                        self._roof_set_status(msg)
                    else:
                        self._roof_set_status("UNKNOWN")
                except Exception:
                    self._roof_set_status("UNKNOWN")

                self.log(f"Roof POST OK: {url} -> {data}")
            except Exception as e:
                self._roof_set_status("POST failed")
                self.log(f"Roof POST failed: {e}")
        threading.Thread(target=worker, daemon=True).start()

    def _roof_http_get(self, url: str):
        def worker():
            try:
                with urllib.request.urlopen(url, timeout=4) as resp:
                    body = resp.read().decode("utf-8", errors="ignore").strip()

                # พยายามตีความ ON/OFF จาก JSON
                try:
                    obj = json.loads(body)
                    msg = str(obj.get("message", "")).upper()
                    if msg in ["ON", "OFF"]:
                        self._roof_set_status(msg)
                    else:
                        self._roof_set_status("UNKNOWN")
                except Exception:
                    self._roof_set_status("UNKNOWN")

                self.log(f"Roof GET OK: {url} -> {body}")
            except Exception as e:
                self._roof_set_status("GET failed")
                self.log(f"Roof GET failed: {e}")
        threading.Thread(target=worker, daemon=True).start()


    # ---- Sliding Roof public actions ----
    def roof_open(self):
        self._roof_http_post(f"{self.roof_api_base}open")

    def roof_close(self):
        self._roof_http_post(f"{self.roof_api_base}close")

    def roof_refresh(self):
        self._roof_http_get(f"{self.roof_api_base}status")

    def roof_toggle_auto(self):
        want = self.roof_auto_var.get()
        if want and not self._roof_polling:
            self._roof_polling = True
            self._roof_poll_stop.clear()
            self._roof_start_poll()
        elif not want and self._roof_polling:
            self._roof_poll_stop.set()
            self._roof_polling = False
    # ---- Auto roof by scheduler helpers ----
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
        lead = (start_dt - now).total_seconds() - 15.0
        delay = max(0.0, lead)

        def _go():
            if self.roof_auto_sched_var.get():
                self._external_on()

                if not self._wait_roof_on(timeout=12.0, interval=0.5):
                    # ไม่เปิดสำเร็จ → ยกเลิกการยิงรอบนี้
                    try:
                        messagebox.showwarning(
                            "Roof not open",
                            "ยกเลิกการยิงอัตโนมัติ:\nหลังคายังไม่เปิด (DI1 != ON)"
                        )
                    except Exception:
                        pass
                    self.log("❌ ยกเลิกการยิงอัตโนมัติ: Roof ไม่เป็น ON ภายในเวลา")
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
        t = threading.Timer(3.0, _go)
        t.daemon = True
        t.start()
        self._postrest_timers[idx] = t

    def _cancel_api_timers_for(self, idx: int) -> None:
        for d in (self._prefire_timers, self._postrest_timers):
            t = d.pop(idx, None)
            if t and getattr(t, 'is_alive', lambda: False)():
                try: t.cancel()
                except Exception: pass


    def _roof_start_poll(self):
        def worker():
            while not self._roof_poll_stop.is_set():
                self.roof_refresh()
                for _ in range(50):
                    if self._roof_poll_stop.is_set(): break
                    time.sleep(0.1)
        threading.Thread(target=worker, daemon=True).start()


    # ---------- Close ----------
    def on_close(self):
        try:
            self._roof_poll_stop = True
            # self._roof_poll_stop.set()
            self.stop_all_programs()
            self._stop_telemetry()
            if self.laser: self.laser.close()
        except Exception:
            pass
        self.destroy()

    def _ui_call(self, fn, *args, **kwargs):
    # เรียกฟังก์ชัน UI จากเธรดใดๆ ผ่าน mainloop
        try:
            self.root.after(0, lambda: fn(*args, **kwargs))
        except Exception:
            pass

    def _show_overheat_popup(self, ltemf: float, maxv: float):
        # ถ้ายังไม่มีหน้าต่าง ให้สร้าง
        if getattr(self, "overheat_win", None) is None or not self.overheat_win.winfo_exists():
            import tkinter as tk
            self.overheat_win = tk.Toplevel(self.root)
            self.overheat_win.title("Overheat Warning")
            self.overheat_win.attributes("-topmost", True)
            self.overheat_win.geometry("360x160+120+120")
            self.overheat_win.resizable(False, False)

            # สไตล์พื้นฐาน
            frm = tk.Frame(self.overheat_win, bg="#8B0000", padx=14, pady=14)
            frm.pack(fill="both", expand=True)

            self.lbl_over_title = tk.Label(frm, text="อุณหภูมิเลเซอร์สูงเกินกำหนด!", 
                                        font=("Segoe UI", 14, "bold"), fg="white", bg="#8B0000")
            self.lbl_over_title.pack(anchor="w")

            self.lbl_over_val = tk.Label(frm, text="", font=("Segoe UI", 28, "bold"), 
                                        fg="white", bg="#8B0000")
            self.lbl_over_val.pack(anchor="center", pady=(6, 8))

            self.lbl_over_hint = tk.Label(frm, 
                text="ระบบสั่ง STANDBY อัตโนมัติ\nหน้าต่างนี้จะปิดเองเมื่ออุณหภูมิลดลงต่ำกว่า Max",
                font=("Segoe UI", 9), fg="white", bg="#8B0000", justify="left")
            self.lbl_over_hint.pack(anchor="w")

            # ปุ่มปิดด้วยมือ (ไม่บังคับใช้ แต่ให้มี)
            btn = tk.Button(frm, text="ปิดหน้าต่างนี้", command=self._hide_overheat_popup, cursor="hand2")
            btn.pack(anchor="e", pady=(8, 0))

        # อัปเดตค่าปัจจุบัน
        try:
            self.lbl_over_val.config(text=f"LTEMF: {ltemf:.1f} °C  (Max {maxv:.1f} °C)")
        except Exception:
            pass

        # ดันขึ้นหน้าเสมอ
        try:
            self.overheat_win.deiconify()
            self.overheat_win.lift()
            self.overheat_win.attributes("-topmost", True)
        except Exception:
            pass


    def _hide_overheat_popup(self):
        win = getattr(self, "overheat_win", None)
        if win is not None and win.winfo_exists():
            try:
                win.destroy()
            except Exception:
                pass
        self.overheat_win = None

    # ---------- Roof/Limit status via API ----------
    def _fetch_limit_state(self, timeout=2.0) -> str:
        # """GET http://192.168.3.124:8085/limit/status แล้วคืนค่า 'ON'/'OFF' หรือ 'N/A'"""
        url = "http://192.168.49.8:8000/limit/status"
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                raw = resp.read()
            data = json.loads(raw.decode("utf-8", errors="ignore"))
            limit = data.get("limit", {})
            state = str(limit.get("state", "")).upper().strip()
            if state in ("ON", "OFF"):
                return state
            return "N/A"
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
        """อัปเดตสถานะทุก 1 วินาที (non-blocking)"""
        if getattr(self, "_roof_poll_stop", False):
            return
        def worker():
            state = self._fetch_limit_state()
            try:
                self.after(0, lambda: self._apply_roof_status(state))
            except Exception:
                pass
        threading.Thread(target=worker, daemon=True).start()
        self.after(1000, self._poll_roof_status)

    def _check_roof_status_now(self):
        state = self._fetch_limit_state()
        self._apply_roof_status(state)
        # ถ้ามี logger ในแอป
        if hasattr(self, "log"):
            self.log(f"SlidingRoof Status = {state}")
    
    def _guard_fire_by_roof(self, timeout=1.5) -> bool:
        # """คืน True ถ้ายิงได้ (Roof = ON), คืน False ถ้ายิงไม่ได้และจะแจ้งเตือน"""
        try:
            state = self._fetch_limit_state(timeout=timeout)  # "ON"/"OFF"/"N/A"
        except Exception:
            state = "N/A"

        if state != "ON":
            # บล็อคการยิง และแจ้งเตือน
            messagebox.showwarning(
                "Roof is Closed",
                "ไม่อนุญาตให้ยิงเลเซอร์:\nสถานะหลังคา (DI1) = %s\n\nกรุณาเปิดหลังคาก่อน (Roof = ON)." % state
            )
            # อัปเดตป้ายสถานะบน UI ด้วย (ถ้ามีตัวแปร)
            try:
                self._apply_roof_status(state)
            except Exception:
                pass
            return False
        return True
    
    def _safe_fire(self) -> bool:
        # """เรียกยิงแบบมีการ์ด ตรวจ Roof ก่อนเสมอ"""
        if not self._guard_fire_by_roof():
            return False
        try:
            self.send_cmd("$FIRE")  # ← ตรงนี้คือคำสั่งยิงเลเซอร์เดิมของคุณ
            return True
        except Exception as e:
            messagebox.showerror("Fire Error", f"สั่งยิงไม่สำเร็จ:\n{e}")
            return False

    def _wait_roof_on(self, timeout: float = 12.0, interval: float = 0.5) -> bool:
        # """รอจน state == 'ON' ภายใน timeout วินาที; คืน True ถ้าเปิดสำเร็จ"""
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            state = self._fetch_limit_state(timeout=1.0)  # "ON"/"OFF"/"N/A"
            last = state
            if state == "ON":
                try: self._apply_roof_status(state)
                except Exception: pass
                return True
            time.sleep(interval)
        try: self._apply_roof_status(last or "N/A")
        except Exception: pass
        return False
    
    def _monitor_roof_during_fire(self):
        # """ตรวจสอบทุก 1 วินาที ถ้า Fire อยู่แต่ Roof OFF → หยุดเลเซอร์"""
        try:
            # ถ้าไม่กำลังยิง ไม่ต้องตรวจ
            if not getattr(self, "is_firing", False):
                self.after(1000, self._monitor_roof_during_fire)
                return

            state = self._fetch_limit_state(timeout=1.0)
            if state == "OFF":
                # หยุดเลเซอร์ทันที
                try:
                    # self._send("$STANDBY")
                    self.cmd_standby()
                    self.log("⚠️ Roof ปิดขณะยิง → สั่งหยุดเลเซอร์ทันที")
                    messagebox.showwarning(
                        "Roof Closed!",
                        "ตรวจพบว่าหลังคาปิดระหว่างการยิงเลเซอร์\nระบบได้สั่งหยุดเลเซอร์ทันทีเพื่อความปลอดภัย"
                    )
                except Exception as e:
                    self.log(f"Error while stopping laser: {e}")
                # อัปเดตสถานะ Fire ให้เป็น False
                self.is_firing = False

            # นัดตรวจซ้ำอีกทุก 1 วินาที
            self.after(1000, self._monitor_roof_during_fire)

        except Exception as e:
            self.log(f"Roof monitor error: {e}")
            self.after(1000, self._monitor_roof_during_fire)

    def _get_roof_status_cached(self) -> str:
        try:
            s = self.roof_status_var.get().strip().upper()
            return s if s else "N/A"
        except Exception:
            return "N/A"

if __name__ == "__main__":
    app = App()
    app.mainloop()