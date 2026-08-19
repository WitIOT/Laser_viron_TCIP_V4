# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for Laser Control (Laser_Rev13.py)
# Build:  python -m PyInstaller --noconfirm LaserControl.spec
#
# Rev13 ใช้ tkinter (ttk มาตรฐาน) + matplotlib — ไม่ได้ใช้ ttkbootstrap
# โมดูลในโปรเจกต์ที่ต้อง bundle: api_clients.py, tutorial_overlay.py (auto-detect)

datas = []
binaries = []
hiddenimports = ['matplotlib.backends.backend_tkagg']

a = Analysis(
    ['Laser_Rev13.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='LaserControl',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,          # GUI app: no console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='LaserControl',
)
