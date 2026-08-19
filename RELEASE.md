# วิธี publish เวอร์ชันใหม่ (auto-update)

แอปเช็คอัปเดตจาก **GitHub Releases** ของ repo `WitIOT/Laser_viron_TCIP_V4`
(ตั้งไว้ใน `updater.py`). เครื่องที่ติดตั้งอยู่จะเห็น release ที่ **tag ใหม่กว่า**
เวอร์ชันปัจจุบัน แล้วดาวน์โหลด `.zip` มาอัปเดตทับให้เอง

## ขั้นตอนออกเวอร์ชันใหม่ (เช่น 13.0.2)

1. **แก้เลขเวอร์ชัน** (2 ที่ให้ตรงกัน)
   - `version.py` → `__version__ = "13.0.2"`
   - `installer.iss` → `#define MyAppVersion "13.0.2"`

2. **build .exe**
   ```
   python -m PyInstaller --noconfirm --clean LaserControl.spec
   ```

3. **zip โฟลเดอร์ one-dir** (โครงสร้างต้องมี `LaserControl/LaserControl.exe` ข้างใน)
   ```powershell
   Compress-Archive -Path dist\LaserControl -DestinationPath release_assets\LaserControl-v13.0.2.zip -Force
   ```

4. **(ถ้าจะแจก installer ด้วย) build installer**
   ```
   "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" installer.iss
   ```

5. **สร้าง GitHub Release**
   - ไปที่ `https://github.com/WitIOT/Laser_viron_TCIP_V4/releases/new`
   - **Tag = `v13.0.2`** (ต้องมี `v` นำหน้า และเป็นเลขที่มากกว่าเวอร์ชันที่แจกไป)
   - แนบไฟล์ `LaserControl-v13.0.2.zip` เป็น asset
   - เขียน release notes (จะโชว์ในกล่องแจ้งเตือนอัปเดตของแอป)
   - กด Publish

6. เครื่องที่ติดตั้งเวอร์ชันเก่ากว่าจะเห็นตอนเปิดแอป (หรือกด "Check for updates")
   → ยืนยัน → ดาวน์โหลด → ปิดแอป → copy ทับ → เปิดใหม่อัตโนมัติ

## ข้อควรระวัง

- **tag ต้องมากกว่าเสมอ** — เทียบแบบ semantic (13.0.2 > 13.0.1). ถ้า tag เท่าเดิม
  หรือน้อยกว่า แอปจะถือว่า "เป็นล่าสุดแล้ว"
- **repo ต้อง public** — updater ใช้ GitHub API แบบไม่ล็อกอิน ถ้าเปลี่ยนเป็น private
  ต้องเพิ่มการยืนยันตัวตน (token) ใน `updater.py`
- **zip ต้องมีโฟลเดอร์ `LaserControl/`** อยู่ข้างใน (ไม่ใช่ไฟล์กระจาย) —
  updater มองหา `LaserControl.exe` ในไฟล์ zip
- แอปที่รันจาก source (`python Laser_Rev13.py`) จะเช็คได้แต่ **ไม่ apply**
  (อัปเดตอัตโนมัติใช้ได้เฉพาะเวอร์ชันที่ freeze เป็น .exe)
