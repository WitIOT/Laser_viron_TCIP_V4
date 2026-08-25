# Weather Protection & Safety Integration — Laser Control (Rev13)

เอกสารเฉพาะเรื่อง: การป้องกันสภาพอากาศ (ฝน/อุณหภูมิ/หลังคา) ผสานกับระบบความปลอดภัยเลเซอร์
และรายละเอียดของ Rain Sensor

สารบัญ
1. [ภาพรวมการผสานระบบป้องกันสภาพอากาศ](#1-ภาพรวมการผสานระบบป้องกันสภาพอากาศ)
2. [Rain Sensor — ลำดับการ poll และตัดสินใจ](#2-rain-sensor--ลำดับการ-poll-และตัดสินใจ)
3. [Rain Sensor — State Machine](#3-rain-sensor--state-machine)
4. [Rain Sensor — ลำดับเหตุการณ์เมื่อฝนตก (integration)](#4-rain-sensor--ลำดับเหตุการณ์เมื่อฝนตก-integration)
5. [ตารางสรุปการตอบสนอง](#5-ตารางสรุปการตอบสนอง)

---

## 1. ภาพรวมการผสานระบบป้องกันสภาพอากาศ

```mermaid
flowchart TB
    subgraph SENSE[อินพุตสภาพแวดล้อม]
      RAIN[Rain Sensor<br/>ฝน / ความเข้ม / สะสม]
      TEMP[Laser Temp<br/>DTEMF / LTEMF]
      ROOF[Roof / Limit<br/>ON / OFF / N-A]
    end

    subgraph DECIDE[ตรรกะความปลอดภัย]
      D1[_on_rain_started<br/>ฝนตก]
      D2[_temp_monitor_tick<br/>อุณหภูมิเกิน]
      D3[_guard_fire_by_roof<br/>Safety Fire ก่อนยิง]
      D4[_monitor_roof_during_fire<br/>เฝ้าหลังคาระหว่างยิง]
    end

    subgraph ACT[การป้องกันเลเซอร์]
      A1[STOP โปรแกรมทั้งหมด]
      A2[STANDBY ดับการยิง]
      A3[ปิดหลังคา]
      A4[บล็อก FIRE ไม่ให้ยิง]
    end

    RAIN --> D1
    TEMP --> D2
    ROOF --> D3
    ROOF --> D4

    D1 --> A1
    D1 --> A2
    D1 --> A3
    D2 --> A2
    D2 --> A3
    D3 --> A4
    D4 --> A2

    A3 -.ปิดหลังคาต้อง.-> A2
```

**หลักการผสานระบบ**
- อินพุตสภาพแวดล้อม 3 ทาง (ฝน / อุณหภูมิ / หลังคา) ป้อนเข้าตรรกะความปลอดภัย
  ที่ทำงานอิสระคู่ขนานกัน
- ทุกเส้นทางยึดหลัก **"ดับเลเซอร์ก่อนเสมอ"** — แม้แต่การปิดหลังคาก็ต้อง `$STANDBY`
  ก่อน (เส้นประ)
- **Safety Fire** (D3) เป็นด่านป้องกัน *ก่อน* ยิง; **roof monitor** (D4) เป็นด่าน
  *ระหว่าง* ยิง — สองชั้นเสริมกัน

---

## 2. Rain Sensor — ลำดับการ poll และตัดสินใจ

```mermaid
flowchart TD
    S([_poll_rain_sensor<br/>ทุก poll interval]) --> G1{_rain_poll_stop?}
    G1 -->|yes| STOP([หยุด poll])
    G1 -->|no| G2{มี request ค้าง?<br/>_rain_poll_inflight}
    G2 -->|yes| RESCH[reschedule]
    G2 -->|no| WK[worker thread:<br/>_fetch_rain_data]
    WK --> OK{ได้ข้อมูล ok?}

    OK -->|yes| P[parse is_raining,<br/>intensity, total, online]
    P --> CACHE[อัปเดต cache +<br/>_rain_last_ok_ts, fail_count=0]
    CACHE --> UI[_update_rain_ui สด]
    UI --> TR{is_raining และ<br/>รอบก่อน = ไม่ฝน?}
    TR -->|ใช่ ขอบ False→True| RAIN[after 0: _on_rain_started]
    TR -->|ไม่| RESCH

    OK -->|no| F[fail_count++]
    F --> AGE{ข้อมูลเก่าเกิน<br/>stale threshold?}
    AGE -->|ใช่| NA[_show_rain_na<br/>แสดง N/A]
    AGE -->|ไม่| RETRY[แสดงค่าล่าสุด + ↻<br/>stale=True]
    NA --> RESCH
    RETRY --> RESCH
    RAIN --> RESCH
    RESCH --> S
```

**อธิบาย**
- poll ทำใน **background thread** (`_fetch_rain_data`) ไม่บล็อก UI; มี guard
  `_rain_poll_inflight` กันยิงซ้อน
- **สำเร็จ:** อัปเดต cache + จอ, แล้วตรวจ **ขอบเปลี่ยน** ไม่ฝน→ฝน เพื่อสั่ง
  `_on_rain_started` เพียงครั้งเดียวต่อการเริ่มฝน (ไม่รัวทุก poll)
- **ล้มเหลว (เน็ต/timeout):** ไม่ลบค่าทันที — มี **grace period**; ถ้าเก่าเกิน
  `stale threshold` จึงแสดง **N/A**, ไม่งั้นแสดงค่าล่าสุดพร้อมเครื่องหมาย retry (↻)

---

## 3. Rain Sensor — State Machine

```mermaid
stateDiagram-v2
    [*] --> NoRain
    NoRain --> Raining: อ่านได้ is_raining=True
    Raining --> NoRain: อ่านได้ is_raining=False
    NoRain --> Stale: อ่านไม่ได้ (ยังไม่เกิน threshold)
    Raining --> Stale: อ่านไม่ได้ (ยังไม่เกิน threshold)
    Stale --> NoRain: อ่านได้อีกครั้ง (ไม่ฝน)
    Stale --> Raining: อ่านได้อีกครั้ง (ฝน)
    Stale --> NA: เกิน stale threshold
    NA --> NoRain: อ่านได้อีกครั้ง (ไม่ฝน)
    NA --> Raining: อ่านได้อีกครั้ง (ฝน)

    note right of Raining
        เข้าครั้งแรก (ขอบ) เท่านั้น
        จึง trigger _on_rain_started
        cooldown popup 300s
    end note
```

**อธิบาย**
- **NoRain / Raining** — สถานะปกติจากเซ็นเซอร์
- **Stale** — อ่านไม่ได้ชั่วคราว ยังแสดงค่าล่าสุด (มี ↻) กันจอกระพริบตอนเน็ตสะดุด
- **N/A** — อ่านไม่ได้นานเกิน `rain_stale_sec` → ถือว่าข้อมูลใช้ไม่ได้
- การเข้าสู่ **Raining จากขอบ** (ไม่ฝน→ฝน) เท่านั้นที่สั่ง safety actions;
  popup มี cooldown 300s กันเด้งซ้ำเมื่อเซ็นเซอร์รายงานกระตุก

---

## 4. Rain Sensor — ลำดับเหตุการณ์เมื่อฝนตก (integration)

```mermaid
sequenceDiagram
    participant P as RainPoll (thread)
    participant API as Rain API
    participant A as App (main)
    participant PR as โปรแกรมที่รัน
    participant L as LaserClient
    participant RC as SlidingRoofClient
    participant U as ผู้ใช้

    P->>API: GET rain status
    API-->>P: is_raining=True (ขอบ False→True)
    P->>A: after(0, _on_rain_started)

    A->>A: เช็ค cooldown popup (300s)
    loop ทุกโปรแกรมที่ is_alive
        A->>PR: stop_program(idx)
        A->>A: cancel prefire/postrest timers
    end
    alt กำลังยิงอยู่
        A->>L: $STANDBY
    end
    A->>RC: roof_close (standby ก่อนปิด)
    A->>U: popup Rain Detected

    Note over A,U: หลังฝนหยุด สถานะกลับ No Rain<br/>แต่โปรแกรมไม่เริ่มเอง — ต้องกด Start ใหม่
```

**อธิบาย**
- ลำดับเป๊ะ: **STOP โปรแกรม → cancel timer หลังคา → STANDBY → ปิดหลังคา → popup**
- เปลี่ยนจาก pause เป็น **stop** เพื่อให้ `FireRestScheduler` หยุดยิงจริง
  (pause ไม่หยุด scheduler รอบปัจจุบัน)
- ยกเลิก prefire/postrest timer กัน "orphan timer" ไปสั่งเปิด/ปิดหลังคาเองภายหลัง
- ปลอดภัยแบบ fail-safe: การปิดหลังคาเรียก `roof_close()` ที่ `$STANDBY` ให้เสร็จก่อน

---

## 5. ตารางสรุปการตอบสนอง

| เหตุการณ์ | ตรวจโดย | STOP โปรแกรม | STANDBY | ปิดหลังคา | บล็อก FIRE | Popup |
|-----------|---------|:---:|:---:|:---:|:---:|:---:|
| ฝนตก | `_on_rain_started` | ✅ | ✅ | ✅ | — | ✅ Rain |
| DTEMF/LTEMF เกิน Max | `_temp_monitor_tick` | — | ✅ | ✅ (หลัง 5s) | — | ✅ Overheat |
| หลังคาไม่เปิด (ก่อนยิง) | `_guard_fire_by_roof` | — | — | — | ✅ | ✅ Roof Closed* |
| หลังคาปิดเอง (ระหว่างยิง) | `_monitor_roof_during_fire` | — | ✅ | — | — | ✅ Roof Closed |
| สั่งปิดหลังคาขณะยิง | `roof_close` | — | ✅ (ก่อนปิด) | ✅ | — | — |

\* ระงับ popup เมื่อฝนตก (เพราะหลังคาปิดเป็นเรื่องปกติของฝน)

**ค่าตั้งที่เกี่ยวข้อง** (แท็บ Settings/Config)
- Rain: `rain_api_url`, poll interval (แนะนำ 1–5s), stale threshold (`rain_stale_sec`)
- Roof: Pre-open lead (15s), Post-close delay (3–5s)
- Temp: Max LTEMF / Max DTEMF, hysteresis 0.3°C
