from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class TutorialOverlay:
    def __init__(self, app: tk.Tk, steps: list[dict], widget_refs: dict):
        self.app = app
        self.steps = steps
        self.widget_refs = widget_refs
        self.idx = 0
        self.win: tk.Toplevel | None = None
        self._style = ttk.Style(app)
        self._highlighted_widget = None
        self._highlighted_style = None

        # Best-effort highlight styles for ttk widgets
        try:
            self._style.configure("Tutorial.TEntry", fieldbackground="#fff3b0")
            self._style.configure("Tutorial.TCombobox", fieldbackground="#fff3b0")
            self._style.configure("Tutorial.TButton", background="#ffe08a")
            self._style.configure("Tutorial.TLabelFrame", background="#fff8dc")
        except Exception:
            pass

    def start(self):
        if self.win is not None and self.win.winfo_exists():
            try:
                self.win.destroy()
            except Exception:
                pass
        self._build_window()
        self.idx = 0
        self._render()

    def _build_window(self):
        self.win = tk.Toplevel(self.app)
        self.win.title("Tutorial")
        self.win.attributes("-topmost", True)
        self.win.resizable(False, False)

        container = ttk.Frame(self.win, padding=12)
        container.pack(fill="both", expand=True)

        self.step_var = tk.StringVar(value="")
        self.title_var = tk.StringVar(value="")
        self.body_var = tk.StringVar(value="")

        ttk.Label(container, textvariable=self.step_var, font=("Segoe UI", 10, "bold")).pack(anchor="w")
        ttk.Label(container, textvariable=self.title_var, font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(6, 2))
        ttk.Label(container, textvariable=self.body_var, wraplength=460, justify="left").pack(anchor="w")

        btns = ttk.Frame(container)
        btns.pack(anchor="e", pady=(10, 0))

        self.btn_back = ttk.Button(btns, text="Back", command=self._prev)
        self.btn_next = ttk.Button(btns, text="Next", command=self._next)
        self.btn_skip = ttk.Button(btns, text="Close", command=self.close)

        self.btn_back.pack(side=tk.LEFT, padx=4)
        self.btn_next.pack(side=tk.LEFT, padx=4)
        self.btn_skip.pack(side=tk.LEFT, padx=4)

    def close(self):
        self._clear_highlight()
        if self.win is not None and self.win.winfo_exists():
            try:
                self.win.destroy()
            except Exception:
                pass

    def _prev(self):
        self.idx = max(0, self.idx - 1)
        self._render()

    def _next(self):
        self.idx = min(len(self.steps) - 1, self.idx + 1)
        self._render()

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

        self.step_var.set(f"Step {self.idx + 1} / {len(self.steps)}")
        self.title_var.set(step.get("title", ""))
        self.body_var.set(step.get("body", ""))

        self.btn_back.config(state=("disabled" if self.idx == 0 else "normal"))
        self.btn_next.config(state=("disabled" if self.idx == len(self.steps) - 1 else "normal"))

        self._highlight_widget(step.get("widget"))
        self._position_near_widget(step.get("widget"))

    def _get_widget(self, key):
        if not key:
            return None
        return self.widget_refs.get(key)

    def _highlight_widget(self, key):
        self._clear_highlight()
        w = self._get_widget(key)
        if w is None:
            return
        try:
            style = w.cget("style")
        except Exception:
            style = None
        self._highlighted_widget = w
        self._highlighted_style = style

        try:
            cls = w.winfo_class()
            if cls == "TEntry":
                w.configure(style="Tutorial.TEntry")
            elif cls == "TCombobox":
                w.configure(style="Tutorial.TCombobox")
            elif cls == "TButton":
                w.configure(style="Tutorial.TButton")
            elif cls == "TLabelFrame":
                w.configure(style="Tutorial.TLabelFrame")
        except Exception:
            pass

    def _clear_highlight(self):
        w = self._highlighted_widget
        if w is None:
            return
        try:
            if self._highlighted_style is not None:
                w.configure(style=self._highlighted_style)
        except Exception:
            pass
        self._highlighted_widget = None
        self._highlighted_style = None

    def _position_near_widget(self, key):
        if self.win is None or not self.win.winfo_exists():
            return
        w = self._get_widget(key)
        if w is None:
            return
        try:
            self.app.update_idletasks()
            x = w.winfo_rootx()
            y = w.winfo_rooty()
            h = w.winfo_height()
            self.win.geometry(f"+{x}+{y + h + 10}")
        except Exception:
            pass
