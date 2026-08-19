# Laser Software v5 — Rev13

โปรแกรมควบคุม/ตั้งเวลายิงเลเซอร์ (Tkinter) พร้อมระบบความปลอดภัยหลังคา, เซ็นเซอร์ฝน,
เซ็นเซอร์อุณหภูมิ/ความชื้น และ telemetry

## โครงสร้างไฟล์

| ไฟล์ | หน้าที่ |
|------|---------|
| `Laser_Rev13.py` | **แอปหลัก (เวอร์ชันปัจจุบัน)** |
| `version.py` | เลขเวอร์ชันแอป (แหล่งเดียว — แก้ที่นี่ก่อน build) |
| `updater.py` | ระบบ auto-update ผ่าน GitHub Releases (swap-in-place) |
| `api_clients.py` | client สำหรับ Sliding Roof / Limit status API |
| `tutorial_overlay.py` | overlay สอนใช้งาน |
| `test_Laser_Rev13.py` | unit tests (roof/laser safety interlock) |
| `test_updater.py` | unit tests (logic เทียบเวอร์ชัน/เช็ค release) |
| `LaserControl.spec` | PyInstaller spec สำหรับ build `.exe` |
| `installer.iss` | Inno Setup script (per-user installer) |
| `requirements.txt` | dependency (matplotlib, requests) |
| `archive/` | เวอร์ชันเก่าและไฟล์ legacy (เก็บไว้อ้างอิง) |

## รัน

```bash
pip install -r requirements.txt
python Laser_Rev13.py
```

## เทสต์

```bash
python -m pytest -v
```

## Build installer (.exe)

```bash
# 1) freeze แอปเป็น one-dir ด้วย PyInstaller
pip install pyinstaller
python -m PyInstaller --noconfirm LaserControl.spec
#    → dist/LaserControl/LaserControl.exe

# 2) ห่อเป็น installer ด้วย Inno Setup
"C:\Program Files (x86)\Inno Setup 6\ISCC.exe" installer.iss
#    → installer_output/LaserControl-Setup-<version>.exe
```

installer ติดตั้งแบบ **per-user** (`%LOCALAPPDATA%\Programs\LaserControl`)
โดยเจตนา เพื่อให้ auto-update เขียนทับไฟล์ได้โดยไม่ต้องสิทธิ์ administrator

## Auto-update (ผ่าน internet)

แอปเช็คเวอร์ชันใหม่จาก **GitHub Releases** (`updater.py` — ตั้ง `GITHUB_OWNER`/`GITHUB_REPO`)
- เช็คอัตโนมัติตอนเปิด + ปุ่ม **"Check for updates"** ในแท็บ Main
- ถ้ามีใหม่ → ดาวน์โหลด `.zip` ของ build → แอปปิดชั่วครู่ → copy ทับ → เปิดใหม่เอง
  (ไม่ต้อง uninstall/install ใหม่)

### วิธี publish เวอร์ชันใหม่
1. แก้เลขใน `version.py` (เช่น `13.0.1`) และ `installer.iss` (`MyAppVersion`)
2. `PyInstaller` → zip โฟลเดอร์ `dist/LaserControl/` เป็น `LaserControl-v13.0.1.zip`
3. สร้าง GitHub Release **tag = `v13.0.1`** แล้วแนบ zip นั้นเป็น asset
4. เครื่องที่ติดตั้งอยู่จะเห็นและอัปเดตได้เอง

## หมายเหตุ

- ไฟล์ `setting/` และ `logs/` เป็น runtime state (ไม่เก็บใน git — ดู `.gitignore`)
- ประวัติการแก้บั๊กความปลอดภัยหลังคา/เลเซอร์ และการลบกลไก Pause/Resume
  อยู่ใน changelog หัวไฟล์ `Laser_Rev13.py` และ git log
