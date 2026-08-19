from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class TutorialOverlay:
    """
    คู่มือแบบ step-by-step:
      - กล่องคำอธิบายเลื่อนไปวางใกล้ widget ที่กำลังพูดถึง
      - วาด "วงกลมสีแดง" (mark circle) ล้อมรอบ widget นั้น ผ่าน overlay โปร่งใส
      - แสดงลำดับ Step X / N + ปุ่ม Back / Next / Close

    แต่ละ step เป็น dict:
      {
        "title": str,
        "body":  str,               # อธิบายละเอียด (รองรับหลายบรรทัด)
        "widget": "<ref key>",      # คีย์ใน widget_refs ที่จะวงกลม (ไม่มี = ไม่วง)
        "on_show": callable|None,   # เรียกก่อน render เช่น สลับแท็บ
        "shape": "oval"|"rect",     # รูปทรงวง (ค่าเริ่มต้น oval)
      }
    """

    RING_COLOR = "#ff2d2d"
    RING_WIDTH = 4
    TRANSPARENT = "#ff00ff"   # สีที่จะทำให้โปร่งใส (magenta — ไม่ถูกใช้ในวงกลม)

    def __init__(self, app: tk.Tk, steps: list[dict], widget_refs: dict,
                 on_close=None):
        self.app = app
        self.steps = steps
        self.widget_refs = widget_refs
        self.on_close = on_close          # เรียกตอนปิดคู่มือ (ใช้คืนค่า demo)
        self.idx = 0
        self.win: tk.Toplevel | None = None
        self._circle_win: tk.Toplevel | None = None
        self._pulse_after = None

    # ------------------------------------------------------------------ #
    def start(self):
        self._teardown_windows()   # ล้างของเก่า (ไม่ยิง on_close)
        self._build_window()
        self.idx = 0
        self._render()

    def _build_window(self):
        self.win = tk.Toplevel(self.app)
        self.win.title("Tutorial")
        self.win.attributes("-topmost", True)
        self.win.resizable(False, False)
        try:
            self.win.configure(bg="#0f2b46")
        except Exception:
            pass

        container = tk.Frame(self.win, bg="#0f2b46", padx=16, pady=14)
        container.pack(fill="both", expand=True)

        self.step_var = tk.StringVar(value="")
        self.title_var = tk.StringVar(value="")
        self.body_var = tk.StringVar(value="")

        tk.Label(container, textvariable=self.step_var, font=("Segoe UI", 9, "bold"),
                 fg="#7fc4ff", bg="#0f2b46").pack(anchor="w")
        tk.Label(container, textvariable=self.title_var, font=("Segoe UI", 14, "bold"),
                 fg="white", bg="#0f2b46", justify="left", wraplength=440).pack(anchor="w", pady=(4, 4))
        tk.Label(container, textvariable=self.body_var, font=("Segoe UI", 10),
                 fg="#e8eef6", bg="#0f2b46", justify="left", wraplength=440).pack(anchor="w")

        btns = tk.Frame(container, bg="#0f2b46")
        btns.pack(anchor="e", pady=(12, 0))

        self.btn_back = tk.Button(btns, text="◀ Back", command=self._prev,
                                  relief="flat", bg="#1c3f60", fg="white",
                                  activebackground="#26527d", activeforeground="white",
                                  padx=12, pady=3, cursor="hand2")
        self.btn_next = tk.Button(btns, text="Next ▶", command=self._next,
                                  relief="flat", bg="#2e7d32", fg="white",
                                  activebackground="#3a9a3f", activeforeground="white",
                                  padx=12, pady=3, cursor="hand2")
        self.btn_skip = tk.Button(btns, text="Close", command=self.close,
                                  relief="flat", bg="#3a3f44", fg="white",
                                  activebackground="#4a5057", activeforeground="white",
                                  padx=12, pady=3, cursor="hand2")

        self.btn_back.pack(side=tk.LEFT, padx=4)
        self.btn_next.pack(side=tk.LEFT, padx=4)
        self.btn_skip.pack(side=tk.LEFT, padx=4)

    # ------------------------------------------------------------------ #
    def _teardown_windows(self):
        """ทำลายหน้าต่าง/วงกลม — ไม่เรียก on_close"""
        self._clear_circle()
        if self._pulse_after is not None:
            try:
                self.app.after_cancel(self._pulse_after)
            except Exception:
                pass
            self._pulse_after = None
        if self.win is not None:
            try:
                if self.win.winfo_exists():
                    self.win.destroy()
            except Exception:
                pass
        self.win = None

    def close(self):
        """ปิดคู่มือจริง (ปุ่ม Close / เสร็จสิ้น) — เรียก on_close เพื่อคืนค่า demo"""
        self._teardown_windows()
        if callable(self.on_close):
            cb = self.on_close
            self.on_close = None   # กันเรียกซ้ำ
            try:
                cb()
            except Exception:
                pass

    def _prev(self):
        self.idx = max(0, self.idx - 1)
        self._render()

    def _next(self):
        self.idx = min(len(self.steps) - 1, self.idx + 1)
        self._render()

    # ------------------------------------------------------------------ #
    def _render(self):
        if not self.steps:
            return
        step = self.steps[self.idx]

        on_show = step.get("on_show")
        if callable(on_show):
            try:
                on_show()
            except Exception:
                pass

        # ให้ layout/แท็บ settle ก่อนวัดตำแหน่ง widget
        try:
            self.app.update_idletasks()
        except Exception:
            pass

        n = len(self.steps)
        self.step_var.set(f"ขั้นที่ {self.idx + 1} / {n}")
        self.title_var.set(step.get("title", ""))
        self.body_var.set(step.get("body", ""))

        self.btn_back.config(state=("disabled" if self.idx == 0 else "normal"))
        self.btn_next.config(text=("เสร็จสิ้น ✓" if self.idx == n - 1 else "Next ▶"))

        w = self._get_widget(step.get("widget"))
        self._show_circle(w, step.get("shape", "oval"))
        self._position_popup(w)

    def _get_widget(self, key):
        if not key:
            return None
        return self.widget_refs.get(key)

    # ---------- วงกลม (mark circle) ---------- #
    def _clear_circle(self):
        if self._circle_win is not None:
            try:
                if self._circle_win.winfo_exists():
                    self._circle_win.destroy()
            except Exception:
                pass
            self._circle_win = None

    def _show_circle(self, widget, shape="oval"):
        """วาดวงแดงล้อมรอบ widget ผ่าน overlay โปร่งใส (Windows)"""
        self._clear_circle()
        if widget is None:
            return
        try:
            if not widget.winfo_ismapped():
                return
            x = widget.winfo_rootx()
            y = widget.winfo_rooty()
            w = widget.winfo_width()
            h = widget.winfo_height()
            if w <= 1 or h <= 1:
                return
        except Exception:
            return

        pad = 10
        gx, gy = x - pad, y - pad
        gw, gh = w + 2 * pad, h + 2 * pad

        try:
            cw = tk.Toplevel(self.app)
            cw.overrideredirect(True)
            cw.attributes("-topmost", True)
            cw.geometry(f"{gw}x{gh}+{gx}+{gy}")
            cw.configure(bg=self.TRANSPARENT)
            # ทำสีพื้นให้โปร่งใส + คลิกทะลุ (เฉพาะ Windows)
            cw.attributes("-transparentcolor", self.TRANSPARENT)
            canvas = tk.Canvas(cw, width=gw, height=gh, bg=self.TRANSPARENT,
                               highlightthickness=0, bd=0)
            canvas.pack(fill="both", expand=True)

            m = self.RING_WIDTH + 1
            if shape == "rect":
                canvas.create_rectangle(m, m, gw - m, gh - m,
                                        outline=self.RING_COLOR, width=self.RING_WIDTH)
            else:
                canvas.create_oval(m, m, gw - m, gh - m,
                                   outline=self.RING_COLOR, width=self.RING_WIDTH)
            self._circle_win = cw
            self._canvas = canvas
            self._ring_coords = (m, m, gw - m, gh - m, shape)
            self._pulse_on = True
            self._start_pulse()
        except Exception:
            # ถ้า transparentcolor ไม่รองรับ (non-Windows) — ข้ามวงกลมไป
            self._clear_circle()

        # ยกกล่องคำอธิบายให้อยู่เหนือวงกลม
        try:
            if self.win is not None and self.win.winfo_exists():
                self.win.lift()
                self.win.attributes("-topmost", True)
        except Exception:
            pass

    def _start_pulse(self):
        """กระพริบวงแดงเบา ๆ ให้สังเกตง่าย"""
        if self._pulse_after is not None:
            try:
                self.app.after_cancel(self._pulse_after)
            except Exception:
                pass
            self._pulse_after = None

        def tick():
            try:
                if self._circle_win is None or not self._circle_win.winfo_exists():
                    return
                self._pulse_on = not getattr(self, "_pulse_on", True)
                color = self.RING_COLOR if self._pulse_on else "#ff9a9a"
                x0, y0, x1, y1, shape = self._ring_coords
                self._canvas.delete("all")
                if shape == "rect":
                    self._canvas.create_rectangle(x0, y0, x1, y1, outline=color, width=self.RING_WIDTH)
                else:
                    self._canvas.create_oval(x0, y0, x1, y1, outline=color, width=self.RING_WIDTH)
                self._pulse_after = self.app.after(550, tick)
            except Exception:
                pass

        self._pulse_after = self.app.after(550, tick)

    # ---------- ตำแหน่งกล่องคำอธิบาย ---------- #
    def _position_popup(self, widget):
        if self.win is None or not self.win.winfo_exists():
            return
        try:
            self.app.update_idletasks()
            sw = self.win.winfo_screenwidth()
            sh = self.win.winfo_screenheight()
            pw = self.win.winfo_width() or 480
            ph = self.win.winfo_height() or 220

            if widget is None or not widget.winfo_ismapped():
                # ไม่มี widget → วางมุมขวาบนของแอป
                ax = self.app.winfo_rootx() + self.app.winfo_width() - pw - 30
                ay = self.app.winfo_rooty() + 60
                self.win.geometry(f"+{max(10, ax)}+{max(10, ay)}")
                return

            x = widget.winfo_rootx()
            y = widget.winfo_rooty()
            w = widget.winfo_width()
            h = widget.winfo_height()

            # วางกล่องด้านล่าง widget ก่อน ถ้าล้นจอค่อยขยับขึ้น/ข้าง
            px = x
            py = y + h + 18
            if py + ph > sh - 10:
                py = y - ph - 18           # วางด้านบนแทน
            if py < 10:
                py = 10
            if px + pw > sw - 10:
                px = sw - pw - 10
            if px < 10:
                px = 10
            self.win.geometry(f"+{int(px)}+{int(py)}")
        except Exception:
            pass
