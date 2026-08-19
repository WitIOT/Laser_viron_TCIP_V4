# Auto-build & release ด้วย GitHub Actions

Workflow: `.github/workflows/release-laser-control.yml`

เมื่อ **push tag `vX.Y.Z`** → Actions จะ build .exe + zip + installer อัตโนมัติ
แล้ว publish เป็น GitHub Release ใน repo นี้ (Laser_viron_TCIP_V4) เอง

## ไม่ต้องตั้งค่าอะไรเพิ่ม

โค้ดและ release อยู่ repo เดียวกัน จึงใช้ `GITHUB_TOKEN` ในตัวของ Actions
(ไม่ต้องสร้าง PAT / ไม่ต้องตั้ง secret)

## วิธีออกเวอร์ชันใหม่

```bash
git tag v13.0.6
git push origin v13.0.6
```

Actions จะ:
1. sync `version.py` + `installer.iss` ให้ตรงกับ tag อัตโนมัติ
2. รัน pytest (ถ้า fail จะไม่ release)
3. build .exe (PyInstaller) + zip + installer (Inno Setup)
4. สร้าง Release พร้อมแนบ zip (auto-update) + installer (ติดตั้งครั้งแรก)

เครื่องที่ติดตั้งอยู่จะเห็นและอัปเดตเองได้ทันที (updater ชี้มา repo นี้อยู่แล้ว)

## หมายเหตุ
- tag ต้องขึ้นต้น `v` และเป็น semantic version ที่มากกว่าเวอร์ชันที่แจกไป
- ถ้า build ล้มเหลว ดู log ที่แท็บ **Actions**
- ต้องเปิดใช้ GitHub Actions และบัญชีต้องไม่ติด billing lock
- ออกเวอร์ชันด้วยมือก็ได้ (ดู RELEASE.md) ไม่จำเป็นต้องใช้ CI
