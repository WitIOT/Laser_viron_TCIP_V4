# Flow Chart — Laser Control (Rev13)

แผนภาพการทำงานของโปรแกรม (GitHub เรนเดอร์ Mermaid อัตโนมัติ)

---

## 1. ภาพรวมการใช้งาน (User Flow)

```mermaid
flowchart TD
    A([เปิดโปรแกรม]) --> B[ตั้งค่าเชื่อมต่อ<br/>IP / Port / User]
    B --> C{กด Connect}
    C -->|สำเร็จ| D[ตั้งค่าเลเซอร์<br/>QSDELAY / Frequency]
    C -->|ล้มเหลว| B
    D --> E[เปิด Temp Control<br/>ตั้ง Max LTEMF/DTEMF]
    E --> F{เลือกวิธียิง}
    F -->|ยิงมือ| G[กด FIRE]
    F -->|ตั้งเวลา| H[ตั้งโปรแกรม<br/>Mode / เวลา / Fire-Rest]
    H --> I[Preview Fire Times]
    I --> J[Start Program]
    G --> K[ดูกราฟ / Logs / Telemetry]
    J --> K
    K --> L([เลิกงาน: Disconnect])
```

---

## 2. ลำดับเริ่มต้นโปรแกรม (Startup / Init)

```mermaid
flowchart TD
    S([App.__init__]) --> CFG[โหลด config<br/>load_config]
    CFG --> UI[สร้าง UI<br/>_build_ui]
    UI --> PLOT[_init_plots]
    PLOT --> T1[after: _update_clock_and_plot]
    PLOT --> T2[after: _temp_monitor_tick วนทุก 1s]
    PLOT --> T3[after: _ui_telemetry_tick]
    PLOT --> T4[after: _monitor_roof_during_fire วนทุก 1s]
    PLOT --> T5[after: _poll_roof_status<br/>backoff 2→20s]
    PLOT --> T6{rain_enabled?}
    T6 -->|yes| R[after: _poll_rain_sensor]
    PLOT --> T7{sensor_enabled?}
    T7 -->|yes| SN[after: _poll_sensor]
    PLOT --> U[after 3s: check_for_updates<br/>เงียบ]
```

---

## 3. โปรแกรมตั้งเวลา — วงจร Fire / Rest

```mermaid
flowchart TD
    ST([Start Program]) --> V{ตรวจ input<br/>fire>0, rest>=0}
    V -->|ไม่ผ่าน| ERR[แจ้ง error + ยกเลิก]
    V -->|ผ่าน| RN[runner thread:<br/>MANAGER START]
    RN --> NX[compute_next_occurrence<br/>หาช่วงเวลาถัดไป]
    NX --> W{ถึงเวลาเริ่ม?}
    W -->|ยัง| WAIT[รอ + เช็ค manager_stop]
    WAIT --> W
    W -->|ถึง| PRE[_schedule_prefire_api<br/>เปิดหลังคาก่อนยิง Pre-open lead]
    PRE --> FR[FireRestScheduler thread]

    subgraph CYCLE[วงจรในหนึ่งหน้าต่างเวลา]
      FR --> ONF[on_fire]
      ONF --> GUARD{_guard_fire_by_roof<br/>Roof = ON?}
      GUARD -->|ไม่| BLK[Blocked Roof Closed<br/>ไม่ยิง]
      GUARD -->|ใช่| FIRE[_safe_fire → $FIRE<br/>is_firing = True]
      FIRE --> RESTW[รอครบ fire_td]
      RESTW --> ONR[on_rest]
      ONR --> STB[$STANDBY<br/>is_firing = False]
      STB --> POST[_schedule_postrest_api<br/>ปิดหลังคาหลังพัก]
      POST --> LAST{rest สุดท้าย?}
      LAST -->|ไม่| PRE2[ตั้ง prefire รอบถัดไป]
      PRE2 --> ONF
      LAST -->|ใช่| CLOSE[_schedule_roof_close_if_open]
    end

    CLOSE --> LOOP{มีรอบถัดไป?<br/>everyday/weekdays}
    LOOP -->|มี| NX
    LOOP -->|ไม่/Stop| DONE([MANAGER STOP])
```

> **หมายเหตุ interlock:** prefire/postrest timer ทุกตัวเช็ค `_is_program_active(idx)`
> ณ เวลาที่ทำงานจริง — ถ้าโปรแกรมถูกหยุด/ฝนตกไปแล้ว จะไม่สั่งเปิด/ปิดหลังคา

---

## 4. ระบบความปลอดภัย (Safety Interlocks)

```mermaid
flowchart TD
    subgraph RAIN[ฝนตก _on_rain_started]
      R1[ตรวจพบฝน False→True] --> R2[STOP ทุกโปรแกรมที่รัน<br/>stop_program + cancel timers]
      R2 --> R3[$STANDBY ถ้ากำลังยิง]
      R3 --> R4[ปิดหลังคา roof_close]
      R4 --> R5[popup Rain Detected<br/>ผู้ใช้ต้อง Start เองภายหลัง]
    end

    subgraph TEMP[อุณหภูมิเกิน _temp_monitor_tick วนทุก 1s]
      T1[DTEMF/LTEMF > Max?] -->|เกิน| T2[$STANDBY + is_firing=False]
      T2 --> T3[after 5s: ปิดหลังคา]
      T3 --> T4[popup Overheat พื้นแดง<br/>ค้างจนอุณหภูมิต่ำกว่า Max]
    end

    subgraph ROOFCLOSE[ปิดหลังคาขณะยิง roof_close]
      C1[สั่งปิดหลังคา] --> C2{is_firing?}
      C2 -->|ใช่| C3[_send_standby_sync<br/>ดับเลเซอร์ให้เสร็จก่อน]
      C3 --> C4[post_close ปิดหลังคา]
      C2 -->|ไม่| C4
    end

    subgraph MONITOR[หลังคาปิดเองขณะยิง _monitor_roof_during_fire]
      M1[Roof = OFF ขณะ is_firing?] -->|ใช่| M2[$STANDBY ทันที + เตือน]
    end
```

**หลักการ:** ทุกเส้นทางยึด "ดับเลเซอร์ก่อนเสมอ" — ปิดหลังคา/ฝน/อุณหภูมิ/หลังคาหลุด
จะสั่ง `$STANDBY` ก่อนทำอย่างอื่น

---

## 5. ระบบอัปเดตอัตโนมัติ (Auto-update)

```mermaid
flowchart TD
    U1([Check for updates]) --> U2[GitHub Releases API<br/>WitIOT/Laser_viron_TCIP_V4]
    U2 --> U3{เทียบ tag_name<br/>ใหม่กว่า version.py?}
    U3 -->|error/timeout| UE[แจ้ง 'เช็คไม่สำเร็จ']
    U3 -->|ไม่มีใหม่| UL[แจ้ง 'เป็นล่าสุดแล้ว']
    U3 -->|มีใหม่| U4[ผู้ใช้ยืนยัน]
    U4 --> U5[ดาวน์โหลด zip<br/>+ progress bar]
    U5 --> U6[แตกไฟล์ไป temp]
    U6 --> U7[เขียน swap.bat<br/>รอ exe ปลดล็อก → robocopy → เปิดใหม่]
    U7 --> U8[os._exit ปิด process<br/>ปล่อย lock ไฟล์]
    U8 --> U9[bat: copy ทับ install dir]
    U9 --> U10([เปิดเวอร์ชันใหม่อัตโนมัติ])
```

> ทำงานเฉพาะเวอร์ชัน freeze (.exe); รันจาก source จะเช็คได้แต่ไม่ apply
> ติดตั้งแบบ per-user (%LOCALAPPDATA%) จึง copy ทับได้โดยไม่ต้องสิทธิ์ admin
