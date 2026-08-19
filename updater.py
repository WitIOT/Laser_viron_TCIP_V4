# updater.py — ระบบอัปเดตผ่าน internet (GitHub Releases) แบบ swap-in-place
#
# หลักการ:
#   1. เช็คเวอร์ชันล่าสุดจาก GitHub Releases API (ไม่ต้องมี server เอง)
#   2. ถ้ามีใหม่กว่า → ดาวน์โหลด asset .zip ของ build ใหม่
#   3. แตกไฟล์ไปที่ temp แล้วเขียน .bat ที่ "รอแอปปิด → copy ทับ → เปิดใหม่"
#      → อัปเดตได้โดยไม่ต้อง uninstall/install ใหม่
#
# ต้องติดตั้งแบบ per-user (เช่น %LOCALAPPDATA%) เพื่อให้ copy ทับได้โดยไม่ต้องสิทธิ์ admin
#
# หมายเหตุ: การ "apply" ทำได้เฉพาะตอนรันจาก .exe ที่ freeze แล้ว (sys.frozen)
#           ตอนรันจาก source (.py) จะเช็คได้แต่ไม่ apply

from __future__ import annotations
import os
import sys
import re
import json
import zipfile
import tempfile
import threading
import subprocess
import urllib.request
import urllib.error

try:
    from version import __version__ as CURRENT_VERSION
except Exception:
    CURRENT_VERSION = "0.0.0"

# ---- ตั้งค่า repo ที่เก็บ release (แก้ให้ตรงกับ repo จริงของคุณ) ----
GITHUB_OWNER = "WitIOT"
GITHUB_REPO = "Laser_viron_TCIP_V4"

_API_LATEST = "https://api.github.com/repos/{owner}/{repo}/releases/latest"
_APP_EXE = "LaserControl.exe"
_HTTP_TIMEOUT = 15.0


# --------------------------------------------------------------------- #
#  Version helpers
# --------------------------------------------------------------------- #
def parse_version(text: str) -> tuple:
    """'v13.0.1' / '13.0.1' -> (13, 0, 1) ; ตัวที่แปลงไม่ได้ = (0,)"""
    if not text:
        return (0,)
    s = str(text).strip().lstrip("vV")
    parts = re.split(r"[.\-+]", s)
    nums = []
    for p in parts:
        if p.isdigit():
            nums.append(int(p))
        else:
            break
    return tuple(nums) if nums else (0,)


def is_newer(latest: str, current: str = CURRENT_VERSION) -> bool:
    return parse_version(latest) > parse_version(current)


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def app_install_dir() -> str:
    """โฟลเดอร์ที่ติดตั้งแอป (ที่มี LaserControl.exe) เมื่อ freeze แล้ว"""
    if is_frozen():
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


# --------------------------------------------------------------------- #
#  Check for update
# --------------------------------------------------------------------- #
class UpdateInfo:
    def __init__(self, version, download_url, notes, html_url):
        self.version = version
        self.download_url = download_url
        self.notes = notes or ""
        self.html_url = html_url or ""


class UpdateCheckError(Exception):
    """เช็คอัปเดตไม่สำเร็จ (เน็ต/timeout/HTTP/parse) — ต่างจาก 'ไม่มีอัปเดต'"""
    pass


def check_for_update(timeout: float = _HTTP_TIMEOUT) -> UpdateInfo | None:
    """
    คืน UpdateInfo ถ้ามีเวอร์ชันใหม่กว่า, คืน None ถ้า 'เป็นล่าสุดแล้ว/ไม่มี asset'
    ถ้าเช็คไม่สำเร็จ (เน็ต/timeout/HTTP/parse) จะ raise UpdateCheckError
    เพื่อให้ UI แยก 'ไม่มีอัปเดต' ออกจาก 'เช็คไม่ได้'
    """
    url = _API_LATEST.format(owner=GITHUB_OWNER, repo=GITHUB_REPO)
    req = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json",
        "User-Agent": "LaserControl-Updater",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # ไม่มี release เลย = ยังไม่เคย publish → ถือว่า 'เป็นล่าสุด'
            return None
        raise UpdateCheckError(f"HTTP {e.code}") from e
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise UpdateCheckError(f"เชื่อมต่อ GitHub ไม่ได้: {getattr(e, 'reason', e)}") from e

    try:
        data = json.loads(raw)
    except ValueError as e:
        raise UpdateCheckError("ข้อมูลจาก GitHub ไม่ถูกต้อง") from e

    tag = data.get("tag_name") or data.get("name") or ""
    if not is_newer(tag):
        return None

    assets = data.get("assets") or []
    zip_asset = None
    for a in assets:
        name = (a.get("name") or "").lower()
        if name.endswith(".zip"):
            if "lasercontrol" in name or zip_asset is None:
                zip_asset = a
    if not zip_asset:
        # มีเวอร์ชันใหม่แต่ลืมแนบ zip → บอกว่าเช็คได้แต่ใช้ไม่ได้
        raise UpdateCheckError(f"release {tag} ไม่มีไฟล์ .zip แนบ")

    return UpdateInfo(
        version=tag,
        download_url=zip_asset.get("browser_download_url"),
        notes=data.get("body", ""),
        html_url=data.get("html_url", ""),
    )


def check_for_update_async(on_result, timeout: float = _HTTP_TIMEOUT) -> None:
    """
    เช็คใน background thread แล้วเรียก on_result(info, error)
      - มีอัปเดต : on_result(UpdateInfo, None)
      - เป็นล่าสุด: on_result(None, None)
      - เช็คไม่ได้: on_result(None, "ข้อความ error")
    """
    def worker():
        info, err = None, None
        try:
            info = check_for_update(timeout=timeout)
        except UpdateCheckError as e:
            err = str(e)
        except Exception as e:
            err = f"{e}"
        try:
            on_result(info, err)
        except Exception:
            pass
    threading.Thread(target=worker, daemon=True, name="UpdateCheck").start()


# --------------------------------------------------------------------- #
#  Download + apply
# --------------------------------------------------------------------- #
def download_zip(url: str, dest_path: str, progress_cb=None, timeout: float = 60.0) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "LaserControl-Updater"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        total = int(resp.headers.get("Content-Length", 0) or 0)
        done = 0
        chunk = 1024 * 64
        with open(dest_path, "wb") as f:
            while True:
                buf = resp.read(chunk)
                if not buf:
                    break
                f.write(buf)
                done += len(buf)
                if progress_cb and total > 0:
                    try:
                        progress_cb(done, total)
                    except Exception:
                        pass


def _find_app_dir(root: str) -> str | None:
    """หาโฟลเดอร์ที่มี LaserControl.exe ภายในไฟล์ที่แตกออกมา"""
    for base, _dirs, files in os.walk(root):
        if _APP_EXE in files:
            return base
    return None


def apply_update(info: UpdateInfo, progress_cb=None) -> bool:
    """
    ดาวน์โหลด + แตกไฟล์ + เขียน swap.bat + สั่งรันแล้วปิดแอป
    คืน True ถ้าเริ่มกระบวนการ swap สำเร็จ (แอปจะถูกปิดโดย caller)
    ทำได้เฉพาะเมื่อ freeze แล้วเท่านั้น
    """
    if not is_frozen():
        raise RuntimeError("อัปเดตอัตโนมัติทำได้เฉพาะเวอร์ชันที่ติดตั้ง (.exe) เท่านั้น")
    if not info or not info.download_url:
        raise RuntimeError("ไม่มีไฟล์อัปเดตให้ดาวน์โหลด")

    work = tempfile.mkdtemp(prefix="LaserControl_update_")
    zip_path = os.path.join(work, "update.zip")
    stage = os.path.join(work, "stage")
    os.makedirs(stage, exist_ok=True)

    download_zip(info.download_url, zip_path, progress_cb=progress_cb)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(stage)

    src_dir = _find_app_dir(stage)
    if not src_dir:
        raise RuntimeError("ไฟล์อัปเดตไม่ถูกต้อง: หา LaserControl.exe ในไฟล์ zip ไม่เจอ")

    install_dir = app_install_dir()
    # ❗ bat ต้องอยู่นอก work dir — บรรทัดสุดท้ายลบ work ระหว่างที่ bat กำลังรัน
    #    (cmd อ่าน batch ทีละบรรทัดจากดิสก์ ลบไฟล์ตัวเองกลางคันจะตัดจบสคริปต์)
    bat_path = os.path.join(tempfile.gettempdir(), "LaserControl_apply_update.bat")
    log_path = os.path.join(tempfile.gettempdir(), "LaserControl_update.log")
    exe_path = os.path.join(install_dir, _APP_EXE)

    # รอแอปปิดจริง (ทั้ง process หายและไฟล์ exe ปลดล็อก) → copy ทับ → เปิดใหม่
    # หมายเหตุ:
    #  - ใช้ ping แทน timeout (timeout ต้องการ stdin console — ใช้ไม่ได้ใน hidden window)
    #  - รอ exe ปลดล็อกด้วยการลอง ren ชื่อเดิม (สำเร็จ = ไม่มีใครถือไฟล์แล้ว)
    #  - robocopy /R:30 /W:1 + เช็ค errorlevel>=8 (fail จริง) → ไม่ start แอปครึ่งๆ กลางๆ
    #  - log ทุกขั้นไว้ที่ %TEMP%\LaserControl_update.log เพื่อวินิจฉัยเมื่อพัง
    bat = f"""@echo off
setlocal
set LOG="{log_path}"
echo ==== update start %date% %time% ==== > %LOG%

:waitproc
tasklist /FI "IMAGENAME eq {_APP_EXE}" 2>nul | find /I "{_APP_EXE}" >nul
if not errorlevel 1 (
    ping -n 2 127.0.0.1 >nul
    goto waitproc
)
echo process gone %time% >> %LOG%

rem ---- รอไฟล์ exe ปลดล็อกจริง (สูงสุด ~30s) ----
set /a tries=0
:waitlock
set /a tries+=1
if %tries% gtr 30 (
    echo ERROR: exe still locked after 30s >> %LOG%
    goto fail
)
ren "{exe_path}" "{_APP_EXE}" 2>nul
if errorlevel 1 (
    ping -n 2 127.0.0.1 >nul
    goto waitlock
)
echo exe unlocked after %tries% tries >> %LOG%

robocopy "{src_dir}" "{install_dir}" /E /IS /IT /R:30 /W:1 /NFL /NDL /NJH >> %LOG% 2>&1
if errorlevel 8 (
    echo ERROR: robocopy failed errorlevel %errorlevel% >> %LOG%
    goto fail
)
echo copy done %time% >> %LOG%

start "" "{exe_path}"
echo relaunched %time% >> %LOG%
rmdir /S /Q "{work}" >nul 2>&1
del "%~f0" >nul 2>&1
exit /b 0

:fail
echo ==== update FAILED - see messages above ==== >> %LOG%
start "" "{exe_path}"
exit /b 1
"""
    with open(bat_path, "w", encoding="utf-8") as f:
        f.write(bat)

    # CREATE_NO_WINDOW (hidden console) — ห้ามใช้ DETACHED_PROCESS:
    # แบบ detached ไม่มี console เลย ทำให้คำสั่ง console (timeout ฯลฯ) ทำงานเพี้ยน
    NO_WINDOW = 0x08000000  # CREATE_NO_WINDOW
    subprocess.Popen(
        ["cmd", "/c", bat_path],
        creationflags=NO_WINDOW,
        close_fds=True,
        cwd=tempfile.gettempdir(),
    )
    return True
