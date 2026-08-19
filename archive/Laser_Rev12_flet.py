# Laser_Rev12_flet.py
"""
Rev12 — Flet UI prototype (หน้าตาตรงตาม mockup "Light Command Bar")
- render ด้วย Flutter → มุมมน, การ์ดพื้นสี, metric tile, status pill, switch สวยจริง
- ตอนนี้เป็น VISUAL prototype (ค่าตัวอย่างตาม mockup) ปุ่ม/ฟอร์มยังไม่ผูก logic จริง
- logic เดิม (scheduler.py / api_clients.py / laser_client.py) จะต่อเข้าทีหลัง

รันด้วย:  python Laser_Rev12_flet.py
"""
import flet as ft

# ---------------- Palette (mockup) ----------------
BG      = "#f3f4f6"   # page background (tertiary)
CARD    = "#ffffff"   # card surface
TILE    = "#f1f3f5"   # metric tile fill
BORDER  = "#e4e7eb"   # soft border
TXT     = "#1f2d3d"   # text primary
SUB     = "#6c7a89"   # text secondary
HINT    = "#9aa5b1"   # text tertiary
DANGER  = "#e24b4a"
WARNING = "#f0a020"
SUCCESS = "#1aa179"
INFO    = "#2780e3"

W5 = ft.FontWeight.W_500
W6 = ft.FontWeight.W_600
W7 = ft.FontWeight.W_700


# ---------------- reusable pieces ----------------
def card(title, *content, icon=None, trailing=None, expand=False):
    head_left = [ft.Text(title, size=13, weight=W6, color=TXT)]
    if icon:
        head_left.insert(0, ft.Icon(icon, size=16, color=SUB))
    header = ft.Row(
        [ft.Row(head_left, spacing=8),
         trailing if trailing is not None else ft.Container()],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
    )
    return ft.Container(
        bgcolor=CARD, border_radius=12, padding=14,
        border=ft.border.all(1, BORDER), expand=expand,
        content=ft.Column([header, *content], spacing=11, tight=True),
    )


def tile(label, value, unit="", value_color=TXT, big=22):
    return ft.Container(
        bgcolor=TILE, border_radius=8, padding=12, expand=True,
        content=ft.Column([
            ft.Text(label, size=12, color=SUB),
            ft.Row(
                [ft.Text(value, size=big, weight=W7, color=value_color),
                 ft.Text(unit, size=11, color=HINT)],
                spacing=4, vertical_alignment=ft.CrossAxisAlignment.END, tight=True),
        ], spacing=2, tight=True),
    )


def pill(text, color, icon=None):
    items = [ft.Container(width=7, height=7, border_radius=99, bgcolor=color)]
    if icon:
        items = [ft.Icon(icon, size=13, color=color)]
    items.append(ft.Text(text, size=11, weight=W5, color=color))
    return ft.Container(
        bgcolor=ft.Colors.with_opacity(0.12, color), border_radius=99,
        padding=ft.padding.symmetric(4, 10),
        content=ft.Row(items, spacing=5, tight=True),
    )


def status_item(caption, value, color=TXT, icon=None):
    val = ft.Text(value, size=13, weight=W6, color=color)
    if icon:
        val = ft.Row([ft.Icon(icon, size=14, color=color), val], spacing=5, tight=True)
    return ft.Column([ft.Text(caption, size=10, color=HINT), val], spacing=1, tight=True)


def vsep():
    return ft.Container(width=1, height=30, bgcolor=BORDER)


def big_button(label, icon, color, on_click):
    return ft.Container(
        expand=True, height=66, border_radius=12, bgcolor=CARD,
        border=ft.border.all(1, BORDER), on_click=on_click, ink=True,
        alignment=ft.alignment.center,
        content=ft.Column(
            [ft.Icon(icon, color=color, size=24),
             ft.Text(label, color=TXT, size=14, weight=W6)],
            alignment=ft.MainAxisAlignment.CENTER,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=4, tight=True),
    )


def field(label, value, width=None, hint=None):
    return ft.Column([
        ft.Row([ft.Text(label, size=11, color=HINT),
                ft.Text(hint or "", size=11, color=HINT)],
               alignment=ft.MainAxisAlignment.SPACE_BETWEEN) if hint else
        ft.Text(label, size=11, color=HINT),
        ft.TextField(value=value, height=38, width=width, text_size=13,
                     content_padding=ft.padding.symmetric(6, 10),
                     border_color=BORDER, focused_border_color=INFO, color=TXT,
                     bgcolor=CARD),
    ], spacing=4, tight=True, expand=(width is None))


def toggle(on=True, color=SUCCESS):
    return ft.Switch(value=on, active_color=color, scale=0.8)


def sensor_panel(title, icon, temp, rh, dew):
    return card(
        title, icon=icon, expand=True,
        trailing=ft.Text("updated 12:02:22", size=10, color=HINT),
        *[
            ft.Row([tile("Temp", temp, "°C", big=24), tile("Humidity", rh, "%", value_color=INFO, big=24)],
                   spacing=10),
            ft.Container(
                bgcolor=TILE, border_radius=8, padding=ft.padding.symmetric(8, 11),
                content=ft.Row(
                    [ft.Row([ft.Icon(ft.Icons.WATER_DROP_OUTLINED, size=14, color=SUB),
                             ft.Text("Dew point", size=12, color=SUB)], spacing=6),
                     ft.Text(f"{dew} °C", size=14, weight=W6, color=TXT)],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN)),
        ],
    )


def laser_temp_tile(name, value, mx, pct):
    return ft.Container(
        bgcolor=TILE, border_radius=8, padding=12, expand=True,
        content=ft.Column([
            ft.Row([ft.Text(name, size=12, weight=W5, color=SUB),
                    ft.Text(f"max {mx}°", size=10, color=HINT)],
                   alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ft.Row([ft.Text(value, size=30, weight=W7, color=TXT),
                    ft.Text("°C", size=12, color=HINT)],
                   spacing=3, vertical_alignment=ft.CrossAxisAlignment.END, tight=True),
            ft.ProgressBar(value=pct, color=WARNING, bgcolor=BORDER, bar_height=6,
                           border_radius=99),
            ft.Text(f"{int(pct*100)}% of limit", size=10, color=HINT),
        ], spacing=6, tight=True),
    )


def main(page: ft.Page):
    page.title = "Laser Software v5_Rev12"
    page.bgcolor = BG
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 0
    try:
        page.window.width = 1180
        page.window.height = 900
        page.window.min_width = 900
        page.window.min_height = 600
    except Exception:
        pass

    logs = ft.Text("Ready.", size=12, color=SUB, font_family="Consolas")

    def log(msg):
        logs.value = f"{msg}\n{logs.value}"
        page.update()

    # ---------- 1) status command bar ----------
    cmdbar = ft.Container(
        bgcolor=CARD, border_radius=12, padding=ft.padding.symmetric(12, 14),
        border=ft.border.all(1, BORDER),
        content=ft.Row([
            status_item("Connection", "Disconnected", DANGER, icon=ft.Icons.CIRCLE),
            vsep(),
            status_item("Laser", "—", SUB),
            vsep(),
            status_item("Rain", "Raining", INFO, icon=ft.Icons.WATER_DROP),
            vsep(),
            status_item("Roof", "Closed", WARNING, icon=ft.Icons.GARAGE_OUTLINED),
            vsep(),
            status_item("Program", "Firing", SUCCESS, icon=ft.Icons.LOCAL_FIRE_DEPARTMENT),
            ft.Container(expand=True),
            ft.FilledButton("Connect", icon=ft.Icons.LINK, bgcolor=INFO,
                            on_click=lambda e: log("Connect clicked")),
            ft.OutlinedButton("Disconnect", on_click=lambda e: log("Disconnect clicked")),
            ft.IconButton(ft.Icons.HELP_OUTLINE, tooltip="Tutorial"),
        ], spacing=12, vertical_alignment=ft.CrossAxisAlignment.CENTER),
    )

    # ---------- 2) big manual buttons ----------
    manual = ft.Row([
        big_button("Fire", ft.Icons.LOCAL_FIRE_DEPARTMENT, DANGER, lambda e: log("FIRE")),
        big_button("Standby", ft.Icons.PAUSE_CIRCLE_OUTLINE, WARNING, lambda e: log("STANDBY")),
        big_button("Stop", ft.Icons.STOP_CIRCLE_OUTLINED, SUB, lambda e: log("STOP")),
    ], spacing=12)

    # ---------- 3) settings row ----------
    laser_settings = card(
        "Laser settings", icon=ft.Icons.TUNE, expand=True,
        *[field("QSDELAY (µs)", "220", hint="0–400"),
          field("Frequency (Hz)", "20", hint="1–22"),
          ft.Row([ft.FilledButton("Save settings", icon=ft.Icons.SAVE, bgcolor=INFO)],
                 alignment=ft.MainAxisAlignment.END)],
    )
    temp_control = card(
        "Temp control", icon=ft.Icons.THERMOSTAT, expand=True, trailing=toggle(True, INFO),
        *[field("Max LTEMF (°C)", "35.0"),
          field("Max DTEMF (°C)", "55.0"),
          ft.Text("เกินค่า → STANDBY อัตโนมัติ", size=10, color=HINT)],
    )
    roof_card = card(
        "Sliding roof", icon=ft.Icons.GARAGE_OUTLINED, expand=True,
        trailing=pill("Closed", WARNING),
        *[ft.Row([
            ft.OutlinedButton("Open", icon=ft.Icons.ARROW_UPWARD, expand=True),
            ft.OutlinedButton("Close", icon=ft.Icons.ARROW_DOWNWARD, expand=True)], spacing=8),
          ft.Container(bgcolor=TILE, border_radius=8, padding=ft.padding.symmetric(8, 10),
                       content=ft.Row([ft.Text("Status", size=12, color=SUB),
                                       ft.Text("OFF · closed", size=12, weight=W6, color=WARNING)],
                                      alignment=ft.MainAxisAlignment.SPACE_BETWEEN)),
          ft.Row([toggle(True), ft.Text("Auto open (T−15s) / close (+5s)", size=11, color=SUB)],
                 spacing=6)],
    )
    settings_row = ft.Row([laser_settings, temp_control, roof_card], spacing=12,
                          vertical_alignment=ft.CrossAxisAlignment.START)

    # ---------- 4) laser temps / rain ----------
    laser_temps = card(
        "Laser temperatures", icon=ft.Icons.DEVICE_THERMOSTAT, expand=True,
        trailing=pill("within limits", SUCCESS, icon=ft.Icons.VERIFIED_USER_OUTLINED),
        *[ft.Row([laser_temp_tile("DTEMF", "42.1", "55", 0.77),
                  laser_temp_tile("LTEMF", "28.4", "35", 0.81)], spacing=10),
          ft.Row([toggle(True), ft.Text("Save CSV · logs/data/telemetry_20260612.csv",
                                        size=11, color=SUB)], spacing=6)],
    )
    rain_card = card(
        "Rain sensor", icon=ft.Icons.WATER_DROP_OUTLINED, expand=True,
        trailing=pill("Raining", INFO, icon=ft.Icons.WATER_DROP),
        *[ft.Row([tile("Intensity", "0.0", "mm/hr", value_color=INFO, big=30),
                  tile("Total", "221.2", "mm", big=30)], spacing=10),
          ft.Row([ft.Text("last update 12:02:22 (UTC+7)", size=10, color=HINT)],
                 alignment=ft.MainAxisAlignment.END)],
    )
    tele_row = ft.Row([laser_temps, rain_card], spacing=12,
                      vertical_alignment=ft.CrossAxisAlignment.START)

    # ---------- 5) indoor / outdoor ----------
    sensor_row = ft.Row([
        sensor_panel("Indoor sensor", ft.Icons.HOME_OUTLINED, "22.9", "49.3", "11.7"),
        sensor_panel("Outdoor sensor", ft.Icons.WB_SUNNY_OUTLINED, "35.0", "55.9", "24.9"),
    ], spacing=12, vertical_alignment=ft.CrossAxisAlignment.START)

    # ---------- 6) scheduled programs ----------
    prog_card = card(
        "Scheduled programs", icon=ft.Icons.EVENT,
        trailing=ft.Row([
            ft.OutlinedButton("Add", icon=ft.Icons.ADD),
            ft.FilledButton("Start all", icon=ft.Icons.PLAY_ARROW, bgcolor=SUCCESS),
            ft.OutlinedButton("Stop all", icon=ft.Icons.STOP)], spacing=6),
        *[ft.Container(
            border=ft.border.all(1, BORDER), border_radius=8, padding=12,
            content=ft.Column([
                ft.Row([toggle(True), field("Name", "Program 1", width=150),
                        ft.Dropdown(value="everyday", width=130,
                                    options=[ft.dropdown.Option("everyday")]),
                        ft.Container(expand=True),
                        pill("Firing · cycle 3", SUCCESS, icon=ft.Icons.LOCAL_FIRE_DEPARTMENT)],
                       spacing=10, vertical_alignment=ft.CrossAxisAlignment.END),
                ft.Row([field("Start (HH:MM)", "16:30"), field("End (HH:MM)", "16:50"),
                        field("Fire (min)", "1"), field("Rest (min)", "1")], spacing=10),
                ft.Row([ft.OutlinedButton("Preview fire times", icon=ft.Icons.VISIBILITY),
                        ft.FilledButton("Start", icon=ft.Icons.PLAY_ARROW, bgcolor=INFO),
                        ft.OutlinedButton("Pause", icon=ft.Icons.PAUSE),
                        ft.OutlinedButton("Duplicate", icon=ft.Icons.CONTENT_COPY)], spacing=6),
            ], spacing=11))],
    )

    # ---------- 7) charts + logs ----------
    charts_card = card(
        "Realtime charts", icon=ft.Icons.SHOW_CHART, expand=True,
        *[ft.Container(bgcolor=TILE, border_radius=8, height=120, alignment=ft.alignment.center,
                       content=ft.Text("Fire/Rest · DTEMF/LTEMF chart (จะต่อ matplotlib/flet chart)",
                                       size=11, color=HINT))],
    )
    logs_card = card(
        "Logs / Terminal", icon=ft.Icons.TERMINAL, expand=True,
        *[ft.Container(bgcolor="#1e1e1e", border_radius=8, padding=11, height=120,
                       content=ft.Column([logs], scroll=ft.ScrollMode.AUTO))],
    )
    bottom_row = ft.Row([charts_card, logs_card], spacing=12,
                        vertical_alignment=ft.CrossAxisAlignment.START)

    main_tab = ft.Container(
        padding=14, bgcolor=BG,
        content=ft.Column([
            cmdbar, manual, settings_row, tele_row, sensor_row, prog_card, bottom_row,
        ], spacing=12, scroll=ft.ScrollMode.AUTO, expand=True),
    )

    def placeholder(name):
        return ft.Container(padding=24, content=ft.Text(f"{name} — จะ port ต่อ", size=14, color=SUB))

    tabs = ft.Tabs(
        selected_index=0, animation_duration=200, expand=True,
        tabs=[
            ft.Tab(text="Main", content=main_tab),
            ft.Tab(text="Settings / Config", content=placeholder("Settings / Config")),
            ft.Tab(text="Network Scanner", content=placeholder("Network Scanner")),
            ft.Tab(text="Connection Settings", content=placeholder("Connection Settings")),
        ],
    )
    page.add(tabs)


if __name__ == "__main__":
    ft.app(main)
