import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# ===================== Data =====================
def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "Date" in df.columns and "Time" in df.columns:
        df["datetime"] = pd.to_datetime(
            df["Date"].astype(str) + " " + df["Time"].astype(str),
            errors="coerce",
        )
    elif "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    else:
        df["datetime"] = pd.to_datetime(df.iloc[:, 0], errors="coerce")

    if "overload" in df.columns:
        df["overload_int"] = (
            df["overload"].astype(str).str.lower()
            .map({"true": 1, "false": 0, "1": 1, "0": 0})
            .fillna(0).astype(int)
        )
    else:
        df["overload_int"] = 0

    return df.dropna(subset=["datetime"]).sort_values("datetime")


def hhmm_to_dt(date_str: str, hhmm: str) -> pd.Timestamp:
    return pd.to_datetime(f"{date_str} {hhmm}:00")


def build_title(df, x_mode, xmin, xmax):
    date_str = df["datetime"].dt.date.iloc[0].strftime("%Y-%m-%d")
    data_min = df["datetime"].min()
    data_max = df["datetime"].max()

    xmin = (xmin or "").strip()
    xmax = (xmax or "").strip()

    if x_mode == "manual":
        tmin = hhmm_to_dt(date_str, xmin) if xmin else data_min
        tmax = hhmm_to_dt(date_str, xmax) if xmax else data_max
    else:
        tmin, tmax = data_min, data_max

    return f"Laser logs {date_str} {tmin:%H:%M} - {tmax:%H:%M}"


# ===================== Plot =====================
def plot_data(df, dpi, minor_tick,
              x_mode, xmin, xmax,
              y_mode, ymin, ymax):

    title = build_title(df, x_mode, xmin, xmax)

    fig, ax1 = plt.subplots(figsize=(14, 5), dpi=dpi)

    # Temperature
    if "LTEMF" in df.columns:
        ax1.plot(df["datetime"], df["LTEMF"], marker="o", ms=3, label="LTEMF (°C)")
    if "DTEMF" in df.columns:
        ax1.plot(df["datetime"], df["DTEMF"], marker="s", ms=3, label="DTEMF (°C)")

    ax1.set_ylabel("Temperature (°C)")
    ax1.set_xlabel("Time")

    # X axis
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_minor_locator(mdates.MinuteLocator(interval=minor_tick))

    if x_mode == "manual":
        date_str = df["datetime"].dt.date.iloc[0].strftime("%Y-%m-%d")
        if xmin:
            ax1.set_xlim(left=hhmm_to_dt(date_str, xmin))
        if xmax:
            ax1.set_xlim(right=hhmm_to_dt(date_str, xmax))

    # Y axis
    if y_mode == "manual":
        if ymin and ymax:
            ax1.set_ylim(float(ymin), float(ymax))
        elif ymin:
            ax1.set_ylim(bottom=float(ymin))
        elif ymax:
            ax1.set_ylim(top=float(ymax))

    # Overload
    ax2 = ax1.twinx()
    ax2.plot(df["datetime"], df["overload_int"],
             color="red", linestyle="--", marker="x",
             label="overload")
    ax2.set_ylim(-0.1, 1.1)
    ax2.set_ylabel("overheat (overload)")

    # Title
    fig.suptitle(title, fontsize=13)

    # Grid
    ax1.grid(True, which="major", axis="x")
    ax1.grid(True, which="minor", axis="x", alpha=0.4)
    ax1.grid(True, which="major", axis="y")

    # Legend
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper left")

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


# ===================== UI =====================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Laser Logs Plotter")

        self.geometry("860x190")
        self.minsize(860, 190)
        self.resizable(True, False)

        self.csv_var = tk.StringVar()
        self.minor_var = tk.StringVar(value="5")
        self.dpi_var = tk.StringVar(value="150")  # preview DPI

        self.x_mode = tk.StringVar(value="auto")
        self.xmin_var = tk.StringVar()
        self.xmax_var = tk.StringVar()

        self.y_mode = tk.StringVar(value="auto")
        self.ymin_var = tk.StringVar()
        self.ymax_var = tk.StringVar()

        self.last_fig = None  # keep last figure for saving

        self.build_ui()
        self.update_field_states()

    def build_ui(self):
        pad_x, pad_y = 4, 2

        root = tk.Frame(self)
        root.pack(fill="both", expand=True, anchor="nw")
        root.grid_columnconfigure(1, weight=1)

        # CSV
        tk.Label(root, text="CSV file").grid(row=0, column=0, sticky="w", padx=pad_x, pady=pad_y)
        tk.Entry(root, textvariable=self.csv_var).grid(row=0, column=1, sticky="ew", padx=pad_x, pady=pad_y)
        tk.Button(root, text="Browse", width=10, command=self.browse).grid(row=0, column=2, padx=pad_x, pady=pad_y)

        # Minor + DPI
        tk.Label(root, text="Minor tick (min)").grid(row=1, column=0, sticky="w", padx=pad_x, pady=pad_y)
        tk.Entry(root, textvariable=self.minor_var, width=8).grid(row=1, column=1, sticky="w", padx=pad_x, pady=pad_y)

        tk.Label(root, text="Preview DPI").grid(row=1, column=2, sticky="w", padx=pad_x, pady=pad_y)
        tk.Entry(root, textvariable=self.dpi_var, width=8).grid(row=1, column=3, sticky="w", padx=pad_x, pady=pad_y)

        # X-axis
        tk.Label(root, text="X-axis").grid(row=2, column=0, sticky="w", padx=pad_x, pady=pad_y)
        tk.Radiobutton(root, text="Auto", variable=self.x_mode, value="auto",
                       command=self.update_field_states).grid(row=2, column=1, sticky="w")
        tk.Radiobutton(root, text="Manual", variable=self.x_mode, value="manual",
                       command=self.update_field_states).grid(row=2, column=2, sticky="w")

        tk.Label(root, text="X-min").grid(row=2, column=3, sticky="w")
        self.xmin_entry = tk.Entry(root, textvariable=self.xmin_var, width=8)
        self.xmin_entry.grid(row=2, column=4, sticky="w")

        tk.Label(root, text="X-max").grid(row=2, column=5, sticky="w")
        self.xmax_entry = tk.Entry(root, textvariable=self.xmax_var, width=8)
        self.xmax_entry.grid(row=2, column=6, sticky="w")

        # Y-axis
        tk.Label(root, text="Y-axis").grid(row=3, column=0, sticky="w", padx=pad_x, pady=pad_y)
        tk.Radiobutton(root, text="Auto", variable=self.y_mode, value="auto",
                       command=self.update_field_states).grid(row=3, column=1, sticky="w")
        tk.Radiobutton(root, text="Manual", variable=self.y_mode, value="manual",
                       command=self.update_field_states).grid(row=3, column=2, sticky="w")

        tk.Label(root, text="Y-min").grid(row=3, column=3, sticky="w")
        self.ymin_entry = tk.Entry(root, textvariable=self.ymin_var, width=8)
        self.ymin_entry.grid(row=3, column=4, sticky="w")

        tk.Label(root, text="Y-max").grid(row=3, column=5, sticky="w")
        self.ymax_entry = tk.Entry(root, textvariable=self.ymax_var, width=8)
        self.ymax_entry.grid(row=3, column=6, sticky="w")

        # Buttons
        tk.Button(root, text="Plot", width=12, command=self.run_plot)\
            .grid(row=4, column=0, sticky="w", padx=pad_x, pady=6)

        tk.Button(root, text="Save PNG", width=12, command=self.save_png)\
            .grid(row=4, column=1, sticky="w", padx=pad_x, pady=6)

    def update_field_states(self):
        state_x = "disabled" if self.x_mode.get() == "auto" else "normal"
        state_y = "disabled" if self.y_mode.get() == "auto" else "normal"
        self.xmin_entry.configure(state=state_x)
        self.xmax_entry.configure(state=state_x)
        self.ymin_entry.configure(state=state_y)
        self.ymax_entry.configure(state=state_y)

    def browse(self):
        path = filedialog.askopenfilename(
            title="Select CSV file",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if path:
            self.csv_var.set(path)

    def run_plot(self):
        try:
            df = load_csv(Path(self.csv_var.get()))
            self.last_fig = plot_data(
                df=df,
                dpi=int(self.dpi_var.get()),
                minor_tick=int(self.minor_var.get()),
                x_mode=self.x_mode.get(),
                xmin=self.xmin_var.get(),
                xmax=self.xmax_var.get(),
                y_mode=self.y_mode.get(),
                ymin=self.ymin_var.get(),
                ymax=self.ymax_var.get(),
            )
            plt.show()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def save_png(self):
        if self.last_fig is None:
            messagebox.showwarning("Warning", "Please plot first.")
            return

        path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG Image", "*.png")],
            title="Save PNG (300 DPI)"
        )
        if not path:
            return

        try:
            # 🔒 DPI = 300 (independent from preview)
            self.last_fig.savefig(path, dpi=300, bbox_inches="tight")
            messagebox.showinfo("Saved", f"Saved PNG (300 DPI)\n{path}")
        except Exception as e:
            messagebox.showerror("Error", str(e))


if __name__ == "__main__":
    App().mainloop()
