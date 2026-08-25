# Diagrams (PNG) — Laser Control (Rev13)

ภาพ PNG ของไดอะแกรมทั้งหมด (เอาไปใส่เอกสาร/สไลด์ได้เลย)
สร้างจากไฟล์ Mermaid ต้นฉบับใน [FLOWCHART.md](FLOWCHART.md) และ [DIAGRAMS.md](DIAGRAMS.md)

> วิธีสร้างใหม่: ติดตั้ง `npm i -g @mermaid-js/mermaid-cli` แล้วรัน
> `mmdc -i <file>.mmd -o <file>.png -b white -s 2 -w 1400`

## Flow Charts

### 1. ภาพรวมการใช้งาน
![user flow](images/flow-1-user-flow.png)

### 2. ลำดับเริ่มต้นโปรแกรม (Startup / Init)
![startup](images/flow-2-startup-init.png)

### 3. วงจร Fire / Rest
![fire-rest](images/flow-3-fire-rest-cycle.png)

### 4. ระบบความปลอดภัย (Safety Interlocks)
![safety](images/flow-4-safety-interlocks.png)

### 5. อัปเดตอัตโนมัติ
![auto-update](images/flow-5-auto-update.png)

## Diagrams เชิงลึก

### 6. Sequence — การเชื่อมต่อเลเซอร์
![seq-connect](images/diagram-1-seq-connect.png)

### 7. Sequence — ยิงมือ + Roof Interlock
![seq-fire](images/diagram-2-seq-manual-fire.png)

### 8. State Machine — สถานะโปรแกรม
![state](images/diagram-3-state-machine.png)

### 9. Sequence — ตรวจฝนตก
![seq-rain](images/diagram-4-seq-rain.png)

### 10. Sequence — อัปเดต (swap-in-place)
![seq-update](images/diagram-5-seq-update.png)

### 11. Class Diagram
![class](images/diagram-6-class.png)

### 12. Data Flow — Telemetry & Sensors
![data-flow](images/diagram-7-data-flow.png)

## Weather Protection & Safety Integration

### 13. ภาพรวมการผสานระบบป้องกันสภาพอากาศ
![weather-integration](images/weather-1-integration.png)

### 14. Rain Sensor — ลำดับ poll และตัดสินใจ
![rain-poll](images/weather-2-rain-poll.png)

### 15. Rain Sensor — State Machine
![rain-state](images/weather-3-rain-state.png)

### 16. Rain Sensor — ลำดับเหตุการณ์เมื่อฝนตก
![rain-sequence](images/weather-4-rain-sequence.png)
