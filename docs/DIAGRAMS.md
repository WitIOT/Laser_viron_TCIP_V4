# Diagrams (เชิงลึก) — Laser Control (Rev13)

ไดอะแกรมเสริมแบบละเอียด: sequence, state machine, class, และ data flow
พร้อมคำอธิบายทีละส่วน (GitHub เรนเดอร์ Mermaid อัตโนมัติ)

สารบัญ
1. [Sequence — การเชื่อมต่อเลเซอร์](#1-sequence--การเชื่อมต่อเลเซอร์)
2. [Sequence — ยิงมือ (Manual FIRE) + Roof Interlock](#2-sequence--ยิงมือ-manual-fire--roof-interlock)
3. [State Machine — สถานะของโปรแกรมตั้งเวลา](#3-state-machine--สถานะของโปรแกรมตั้งเวลา)
4. [Sequence — ตรวจฝนตกและการตอบสนอง](#4-sequence--ตรวจฝนตกและการตอบสนอง)
5. [Sequence — อัปเดตอัตโนมัติ (swap-in-place)](#5-sequence--อัปเดตอัตโนมัติ-swap-in-place)
6. [Class Diagram — โครงสร้างคลาส](#6-class-diagram--โครงสร้างคลาส)
7. [Data Flow — Telemetry & Sensors](#7-data-flow--telemetry--sensors)

---

## 1. Sequence — การเชื่อมต่อเลเซอร์

```mermaid
sequenceDiagram
    actor U as ผู้ใช้
    participant A as App (UI)
    participant L as LaserClient
    participant D as Laser device (TCP)

    U->>A: กด Connect
    A->>A: อ่าน IP / Port / User จาก config
    A->>L: connect(ip, port)
    L->>D: TCP connect
    alt เชื่อมต่อไม่ได้
        D-->>L: error / timeout
        L-->>A: exception
        A-->>U: สถานะ Disconnected (แดง)
    else เชื่อมต่อสำเร็จ
        D-->>L: connected
        A->>L: send_cmd("$LOGIN [user]")
        L->>D: $LOGIN [user]
        D-->>L: OK
        A-->>U: สถานะ Connected (เขียว)
        A->>A: เริ่ม telemetry / poll ต่าง ๆ
    end
```

**อธิบาย**
- การเชื่อมต่อเริ่มจากปุ่ม Connect → `App.connect()` อ่านค่าจาก config
  แล้วสร้าง TCP socket ผ่าน `LaserClient`
- ถ้าล้มเหลว (IP ผิด/เครื่องปิด/timeout) จะจับ exception และคง
  สถานะ **Disconnected** ไม่มีผลข้างเคียง
- เมื่อสำเร็จ ระบบส่งคำสั่ง login อัตโนมัติ (`$LOGIN [user]`) ตามรูปแบบที่ตั้งไว้
  แล้วเปลี่ยนสถานะเป็น **Connected**
- `send_cmd()` มี `threading.Lock` ภายใน จึงเรียกจากหลาย thread ได้ปลอดภัย
  (แต่ละคำสั่งไม่ทับกัน)

---

## 2. Sequence — ยิงมือ (Manual FIRE) + Roof Interlock

```mermaid
sequenceDiagram
    actor U as ผู้ใช้
    participant A as App
    participant G as _guard_fire_by_roof
    participant R as Roof status (cache)
    participant L as LaserClient

    U->>A: กด FIRE
    A->>A: ต่อเลเซอร์แล้วหรือยัง?
    alt ยังไม่ connect
        A-->>U: เตือน 'Laser not connected'
    else connected
        A->>G: ตรวจ interlock
        G->>R: อ่านสถานะหลังคา (cache)
        alt Roof != ON
            G-->>A: False
            A-->>U: popup 'Roof Closed' (ระงับถ้าฝนตก)
            A->>A: is_firing = False
        else Roof = ON
            G-->>A: True
            A->>A: is_firing = True
            A->>L: send_cmd("$FIRE")
            A->>A: เริ่มบันทึก CSV (ถ้าเปิด)
            A-->>U: สถานะ Firing + จุดกราฟ = 1
        end
    end
```

**อธิบาย**
- ก่อนยิงทุกครั้งต้องผ่าน **2 ด่าน**: (1) เชื่อมต่อแล้ว (2) หลังคาเปิด (Safety Fire)
- `_guard_fire_by_roof()` อ่านสถานะหลังคาจาก **cache** (ไม่ยิง API ตรงเพื่อไม่ให้ช้า)
  — cache มาจาก `_poll_roof_status()` ที่ดึงเป็นระยะ
- ถ้าหลังคาไม่เปิด จะบล็อกและเตือน (ยกเว้นตอนฝนตกจะไม่เด้ง popup เพราะเป็นเรื่องปกติ)
- เมื่อผ่านด่าน จึงตั้ง `is_firing = True` (ใต้ `manual_lock`) แล้วส่ง `$FIRE`
- `_safe_fire()` เป็น wrapper ที่ตรวจ interlock ซ้ำก่อนส่งคำสั่งจริง

---

## 3. State Machine — สถานะของโปรแกรมตั้งเวลา

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Waiting: Start Program
    Waiting --> Firing: ถึงเวลา + Roof ON
    Waiting --> Blocked: ถึงเวลา แต่ Roof OFF
    Blocked --> Firing: Roof เปิด
    Firing --> Resting: ครบ fire_td
    Resting --> Firing: ครบ rest_td (มีรอบต่อ)
    Resting --> Waiting: จบหน้าต่าง มีรอบวันถัดไป
    Resting --> Done: จบหน้าต่าง ไม่มีรอบต่อ
    Done --> Waiting: everyday/weekdays วันถัดไป
    Done --> [*]: once ทำครบ

    Firing --> Stopped: Stop / ฝนตก / อุณหภูมิเกิน
    Resting --> Stopped: Stop / ฝนตก
    Waiting --> Stopped: Stop / ฝนตก
    Blocked --> Stopped: Stop / ฝนตก
    Stopped --> [*]
```

**อธิบาย**
- **Idle** — ยังไม่เริ่ม (ค่าเริ่มต้น)
- **Waiting** — กด Start แล้ว รอถึงเวลาเริ่มของหน้าต่างเวลา (manager loop)
- **Firing / Resting** — วงจรหลักที่ `FireRestScheduler` สลับไปมาตาม `fire_td`/`rest_td`
- **Blocked** — ถึงเวลายิงแต่หลังคายังไม่เปิด → ไม่ยิง รอจนหลังคาเปิด
- **Done** — จบหน้าต่างเวลา; ถ้าโหมด everyday/weekdays จะกลับ **Waiting** รอวันถัดไป,
  ถ้า once จะจบ
- **Stopped** — ออกจากทุกสถานะได้ทันทีเมื่อ Stop Program / ฝนตก / อุณหภูมิเกิน
  (ตั้ง `manager_stop` + `oneshot_stop` หยุด scheduler จริง)
- **ไม่มี Paused** — ในเวอร์ชันนี้ตัดกลไก pause ออก ใช้ Start/Stop แทน

---

## 4. Sequence — ตรวจฝนตกและการตอบสนอง

```mermaid
sequenceDiagram
    participant P as RainPoll (thread)
    participant API as Rain API
    participant A as App (main)
    participant S as stop_program
    participant L as LaserClient
    participant RC as SlidingRoofClient

    loop ทุก poll interval
        P->>API: GET rain status
        API-->>P: is_raining, intensity, total
        alt เปลี่ยน False → True
            P->>A: after(0, _on_rain_started)
            A->>S: STOP ทุกโปรแกรมที่รัน
            S->>S: cancel prefire/postrest timers
            A->>L: $STANDBY (ถ้ากำลังยิง)
            A->>RC: roof_close (standby ก่อนปิด)
            A->>A: _show_rain_popup
        end
    end
```

**อธิบาย**
- การดึงข้อมูลฝนทำใน **background thread** (ไม่บล็อก UI) แล้วส่งงานกลับ main thread
  ผ่าน `after(0, ...)`
- ตรวจเฉพาะ **ขอบเปลี่ยน** False→True (ไม่ trigger ซ้ำทุก poll) — กัน popup เด้งรัว
  ด้วย cooldown 300s
- ลำดับความปลอดภัย: **STOP โปรแกรม → cancel timer หลังคา → STANDBY → ปิดหลังคา → popup**
  (เปลี่ยนจาก pause เป็น stop เพื่อให้ `FireRestScheduler` หยุดยิงจริง)
- หลังฝนหยุด สถานะกลับ No Rain แต่ **โปรแกรมไม่เริ่มเอง** — ผู้ใช้ต้องกด Start ใหม่

---

## 5. Sequence — อัปเดตอัตโนมัติ (swap-in-place)

```mermaid
sequenceDiagram
    actor U as ผู้ใช้
    participant A as App
    participant UP as updater
    participant GH as GitHub API
    participant BAT as swap.bat
    participant FS as Install dir

    U->>A: Check for updates
    A->>UP: check_for_update_async
    UP->>GH: GET releases/latest
    GH-->>UP: tag_name + asset zip
    UP-->>A: UpdateInfo (ถ้าใหม่กว่า)
    A-->>U: ยืนยันอัปเดต?
    U->>A: Yes
    A->>UP: apply_update
    UP->>GH: ดาวน์โหลด zip
    UP->>UP: แตกไฟล์ไป temp
    UP->>BAT: เขียน + รัน swap.bat (hidden)
    A->>A: os._exit(0) ปิด process
    Note over A,FS: ปล่อย lock ไฟล์ LaserControl.exe
    BAT->>BAT: รอ exe ปลดล็อก
    BAT->>FS: robocopy ทับ install dir
    BAT->>FS: start LaserControl.exe
    FS-->>U: เปิดเวอร์ชันใหม่
```

**อธิบาย**
- `check_for_update()` เทียบ `tag_name` กับ `version.py` แบบ semantic;
  แยก 3 กรณี: มีอัปเดต / เป็นล่าสุด / เช็คไม่สำเร็จ (เน็ต)
- แอปที่กำลังรัน **ล็อกไฟล์ .exe ตัวเอง** จึง copy ทับไม่ได้ทันที — ต้องใช้
  `swap.bat` ที่ **รอ exe ปลดล็อก** แล้วค่อย `robocopy`
- `os._exit(0)` บังคับ process ตายทันที (ไม่ค้างจาก thread/matplotlib)
  เพื่อปล่อย lock ให้เร็ว — จุดนี้คือหัวใจที่ทำให้ copy สำเร็จ
- ทำงานเฉพาะ **.exe (frozen)**; ติดตั้งแบบ per-user จึงไม่ต้องสิทธิ์ admin

---

## 6. Class Diagram — โครงสร้างคลาส

```mermaid
classDiagram
    class App {
        +bool is_firing
        +LaserClient laser
        +list programs
        +dict _ui_refs
        +connect()
        +cmd_fire()
        +start_program(idx)
        +stop_program(idx)
        +roof_close(reason)
        +_guard_fire_by_roof()
        +_on_rain_started()
        +_temp_monitor_tick()
        +check_for_updates()
        +_start_tutorial()
    }
    class LaserClient {
        +socket sock
        +Lock lock
        +send_cmd(cmd) str
    }
    class FireRestScheduler {
        +datetime start_dt
        +datetime end_dt
        +timedelta fire_td
        +timedelta rest_td
        +Event stop_event
        +run()
        +count_fire_cycles() int$
    }
    class SlidingRoofClient {
        +post_open()
        +post_close()
        +get_status()
    }
    class LimitStatusClient {
        +fetch_state() str
        +fetch_state_async()
    }
    class TutorialOverlay {
        +list steps
        +start()
        +close()
        +_show_circle()
    }
    class UpdateInfo {
        +str version
        +str download_url
        +str notes
    }

    App --> LaserClient : ใช้ส่งคำสั่ง
    App --> FireRestScheduler : สร้างต่อโปรแกรม
    App --> SlidingRoofClient : คุมหลังคา
    App --> LimitStatusClient : อ่านสถานะหลังคา
    App --> TutorialOverlay : คู่มือ
    App ..> UpdateInfo : ผลการเช็คอัปเดต
```

**อธิบาย**
- **`App`** เป็นศูนย์กลาง ถือ state (`is_firing`, `programs`, `_ui_refs`) และ
  ประสานทุกโมดูล
- **`LaserClient`** ห่อ TCP socket — เมธอดเดียวหลักคือ `send_cmd()` (มี lock)
- **`FireRestScheduler`** ถูกสร้าง **หนึ่งตัวต่อหน้าต่างเวลาของโปรแกรม** —
  เป็น `threading.Thread`, `count_fire_cycles()` เป็น staticmethod (pure logic ทดสอบง่าย)
- **`SlidingRoofClient` / `LimitStatusClient`** (ใน `api_clients.py`) คุย HTTP กับ
  ระบบหลังคา แยกจากตรรกะ UI
- **`TutorialOverlay` / `UpdateInfo`** เป็นส่วนเสริม (คู่มือ / ผลการเช็คอัปเดต)

---

## 7. Data Flow — Telemetry & Sensors

```mermaid
flowchart LR
    LASER[(Laser)] -->|$DTEMF/$LTEMF| POLL[telemetry poll]
    POLL --> CACHE[last_dtemf / last_ltemf]
    CACHE --> LBL[ป้าย DTEMF/LTEMF]
    CACHE --> CHART[กราฟ matplotlib]
    CACHE --> CSV[ไฟล์ CSV logs/data]
    CACHE --> TMON[_temp_monitor_tick<br/>เทียบ Max → STANDBY]

    RAINAPI[(Rain API)] -->|poll| RAINUI[Rain status + popup]
    SENSORAPI[(Temp/RH API)] -->|poll| SENSORUI[Indoor/Outdoor labels]
    ROOFAPI[(Limit API)] -->|poll backoff| ROOFCACHE[roof status cache]
    ROOFCACHE --> GUARD[_guard_fire_by_roof]
    ROOFCACHE --> RMON[_monitor_roof_during_fire]
```

**อธิบาย**
- ค่าอุณหภูมิเลเซอร์ (DTEMF/LTEMF) อ่านผ่าน telemetry แล้วเก็บใน cache
  (`last_dtemf`/`last_ltemf`) — จากนั้นแตกไปหลายปลายทาง: ป้าย, กราฟ, CSV,
  และตัวเฝ้าอุณหภูมิ
- **สำคัญ:** `_temp_monitor_tick` อ่านจาก cache นี้ — การจำลองอุณหภูมิใน tutorial
  จึง **ไม่แตะ cache** (แก้แค่ป้าย) เพื่อไม่ให้ trigger STANDBY จริง
- สถานะหลังคาอ่านแบบ **poll + backoff** (2→20s) เก็บใน cache แล้วใช้ทั้ง
  interlock ก่อนยิง และตัวเฝ้าระหว่างยิง
- เซ็นเซอร์ฝน/อุณหภูมิ-ความชื้น poll แยกกัน อัปเดต UI ผ่าน main thread
