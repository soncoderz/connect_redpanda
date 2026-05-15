# 🚀 Redpanda Connect × MongoDB — Complete Pipeline Guide

Dự án này là một hệ thống **Event-Driven Architecture (Kiến trúc Hướng Sự kiện)** hoàn chỉnh sử dụng **Node.js, Kafka (Redpanda), Redpanda Connect, và MongoDB**.

Nó giải quyết một bài toán phổ biến trong Microservices: **Làm sao để lưu dữ liệu vào cơ sở dữ liệu một cách bất đồng bộ và xử lý cập nhật nhiều bảng cùng lúc thông qua Kafka?**

---

## 📋 Mục lục

- [🏗 Kiến trúc tổng quan](#-kiến-trúc-tổng-quan)
- [⚙️ Cơ chế xử lý của Redpanda Connect (Tuần tự hay Đa luồng?)](#️-cơ-chế-xử-lý-của-redpanda-connect-tuần-tự-hay-đa-luồng)
- [💻 Giải thích luồng dữ liệu (Code Walkthrough)](#-giải-thích-luồng-dữ-liệu-code-walkthrough)
  - [1. Node.js Controller (Gửi Event)](#1-nodejs-controller-gửi-event)
  - [2. Pipeline Master (Nhận Event & Xử lý)](#2-pipeline-master-nhận-event--xử-lý)
  - [3. Kỹ thuật cập nhật nhiều bảng (Broker Fan-Out)](#3-kỹ-thuật-cập-nhật-nhiều-bảng-broker-fan-out)
- [⚠️ Bẫy thường gặp (Gotchas)](#️-bẫy-thường-gặp-gotchas)
- [📁 Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [🚀 Cách chạy & Khắc phục lỗi](#-cách-chạy--khắc-phục-lỗi)

---

## 🏗 Kiến trúc tổng quan

Mọi thao tác ghi/xóa/sửa (CRUD) từ người dùng không được ghi trực tiếp vào MongoDB. Thay vào đó, nó đi qua Kafka theo luồng sau:

```
┌──────────────┐     ┌──────────────────┐     ┌─────────────────────┐     ┌──────────┐
│   Client     │────▶│  Node.js Express │────▶│  Redpanda (Kafka)   │────▶│ Redpanda │
│  (Postman)   │ HTTP│  API Server      │ Gửi │  Broker             │ Đọc │ Connect  │
│              │ POST│  (Port 3000)     │Event│  Topics:            │     │ Pipeline │
│              │     │                  │     │  doctor-events      │     │          │
└──────────────┘     └──────────────────┘     │  department-events  │     └────┬─────┘
                                              │  users, orders...   │          │
                                              └─────────────────────┘     ┌────▼─────┐
                                                                          │ MongoDB  │
                                                                          │ (app_db) │
                                                                          └──────────┘
```

### Tại sao lại làm phức tạp như vậy?
1. **Chống quá tải (Buffer):** Khi có hàng chục ngàn request cùng lúc, Node.js chỉ việc đẩy vào Kafka (rất nhanh). Pipeline sẽ từ từ nhặt ra để ghi vào MongoDB, giúp DB không bị sập.
2. **Decoupling (Giảm phụ thuộc):** App Node.js không cần biết MongoDB lưu trữ ra sao. Nó chỉ quan tâm là "đã có sự kiện tạo bác sĩ xảy ra".
3. **Cập nhật đa bảng dễ dàng:** Một sự kiện (như update) có thể kích hoạt nhiều thao tác song song trên nhiều bảng khác nhau mà không cần Node.js phải xử lý giao dịch (transaction) phức tạp.

---

## ⚙️ Cơ chế xử lý của Redpanda Connect (Tuần tự hay Đa luồng?)

Đây là phần rất quan trọng cần hiểu rõ, vì nó ảnh hưởng trực tiếp đến hiệu năng và cách debug lỗi.

### Trả lời ngắn gọn

Redpanda Connect xử lý theo cơ chế **bán song song (Semi-Parallel)**: Phần PIPELINE (xử lý/biến đổi dữ liệu) chạy đa luồng, nhưng phần INPUT (đọc) và OUTPUT (ghi) chạy theo cơ chế hàng đợi — **1 message phải ghi thành công rồi mới nhận message tiếp theo**.

### Giải thích chi tiết từng tầng

```
┌─────────────────────────────────────────────────────────────────────┐
│                    REDPANDA CONNECT PIPELINE                        │
│                                                                     │
│  ┌─────────┐     ┌──────────────────────┐     ┌──────────────────┐  │
│  │  INPUT  │     │      PIPELINE        │     │     OUTPUT       │  │
│  │         │     │   (processors)       │     │   (switch/mongo) │  │
│  │ Kafka   │────▶│                      │────▶│                  │  │
│  │ Consumer│     │  threads: 2          │     │  Ghi vào MongoDB │  │
│  │         │     │  ┌────────┐┌────────┐│     │                  │  │
│  │ TUẦN TỰ │     │  │Luồng 1 ││Luồng 2 ││     │  TUẦN TỰ        │  │
│  │ (1 msg  │     │  │validate││validate││     │  (phải ghi xong  │  │
│  │  1 lần) │     │  │filter  ││filter  ││     │   mới nhận tiếp) │  │
│  │         │     │  │enrich  ││enrich  ││     │                  │  │
│  └─────────┘     │  └────────┘└────────┘│     └──────────────────┘  │
│                  │   ĐA LUỒNG           │                           │
│                  └──────────────────────┘                           │
└─────────────────────────────────────────────────────────────────────┘
```

#### Tầng 1: INPUT (Kafka Consumer) — Tuần tự

```yaml
input:
  kafka:
    consumer_group: connect-master-group  # Kafka đảm bảo mỗi message chỉ được đọc 1 lần
    start_from_oldest: true               # Đọc từ đầu nếu chưa có offset đã commit
```

- Kafka Consumer đọc message **từng cái một** (hoặc theo batch) từ các topic.
- `consumer_group` đảm bảo không có 2 instance nào đọc trùng message.
- `start_from_oldest: true` nghĩa là: **lần đầu chạy** sẽ đọc từ message cũ nhất. Lần chạy sau sẽ đọc tiếp từ chỗ dừng (offset đã commit).

#### Tầng 2: PIPELINE (Processors) — Đa luồng (Cấu hình được)

```yaml
pipeline:
  threads: 2  # ← Tham số quyết định số luồng xử lý song song
```

- `threads: 2` nghĩa là Redpanda Connect tạo **2 goroutine (luồng nhẹ)** để chạy song song các processor (validate, filter, enrich metadata).
- Nếu bạn tăng lên `threads: 4`, sẽ có 4 luồng xử lý cùng lúc → nhanh hơn, nhưng tốn RAM hơn.
- Nếu đặt `threads: 1`, mọi message sẽ được xử lý **tuần tự hoàn toàn** (chậm nhưng đảm bảo thứ tự).

| `threads` | Hành vi | Khi nào dùng |
|-----------|---------|--------------|
| `1` | Tuần tự, đảm bảo thứ tự message | Khi thứ tự xử lý quan trọng (VD: cần update trước delete) |
| `2` (hiện tại) | 2 luồng song song | Cân bằng tốc độ và tài nguyên |
| `4+` | Nhanh hơn, nhiều luồng hơn | Khi có lượng message lớn, server mạnh |

#### Tầng 3: OUTPUT (MongoDB) — Tuần tự, có retry

Đây là tầng **gây tắc nghẽn nhiều nhất**. Cơ chế hoạt động:

```
Message A ghi MongoDB → Thành công → ✅ Commit offset → Nhận Message B
Message A ghi MongoDB → Thất bại  → ❌ Retry → Retry → Retry... (CHẶN tất cả)
```

- Mỗi message **PHẢI** được ghi thành công vào MongoDB, rồi Redpanda Connect mới commit offset và nhận message tiếp theo.
- Nếu output bị lỗi (VD: duplicate key, MongoDB sập), Redpanda Connect sẽ **retry vô hạn** → TẮC NGHẼN toàn bộ pipeline.
- Đây chính là lý do tại sao một lỗi `duplicate key` ở `res-001` có thể **chặn hết** mọi message khác (doctors, departments, payments...).

### Ngoại lệ: Broker Fan-Out — Song song trong output

Khi dùng `broker` với `pattern: fan_out`, các output bên trong sẽ chạy **song song**:

```yaml
output:
  broker:
    pattern: fan_out
    outputs:
      - mongodb: { collection: doctors }      # ← Chạy song song
      - mongodb: { collection: departments }  # ← Chạy song song
```

Tuy nhiên, **CẢ HAI phải thành công** thì message mới được coi là hoàn tất. Nếu 1 trong 2 lỗi → toàn bộ message bị retry.

### Tóm tắt bằng hình ảnh

```
                    threads: 2
                   ┌──────────┐
      Kafka        │ Processor│        MongoDB
     ┌───┐    ┌───▶│ Luồng 1  │───┐    ┌───┐
     │ A │────┤    └──────────┘   ├───▶│ A │──▶ OK → Nhận C
     │ B │    │    ┌──────────┐   │    │ B │──▶ Lỗi → Retry mãi → TẮC!
     │ C │    └───▶│ Luồng 2  │───┘    │   │
     │ D │         └──────────┘        │   │   C, D phải chờ B ghi xong
     └───┘                             └───┘
   Hàng đợi       Xử lý song song    Ghi tuần tự
```

---

## 💻 Giải thích luồng dữ liệu (Code Walkthrough)

Hãy cùng đi theo luồng của một Request tạo và cập nhật Bác sĩ (`doctors`) để xem code hoạt động thế nào.

### 1. Node.js Controller (Gửi Event)

File: `src/controllers/crud.controller.js`

Thay vì lưu thẳng vào Database, Node.js gom dữ liệu lại thành một cục gọi là **Event** (Sự kiện) và đẩy lên Kafka.

```javascript
// Khai báo tường minh topic tương ứng với từng đối tượng
const TOPICS = {
  users:    "users",       // Event liên quan user → chui vào topic "users"
  orders:   "orders",      // Event liên quan đơn hàng → topic "orders"
  payments: "payments",    // Event liên quan thanh toán → topic "payments"
};

// Hàm tạo mới user
const createUser = async (req, res) => {
  try {
    // 1. Tạo cục Event bao gồm: ID, Loại sự kiện (add), Thời gian, và Dữ liệu
    const event = {
      id: crypto.randomUUID(),           // Cấp 1 mã ID độc nhất (UUID v4)
      eventType: "add",                  // Loại hành động: Thêm mới
      eventTime: new Date().toISOString(),
      ...req.body                        // Lấy dữ liệu người dùng gửi lên (name, email...)
    };

    // 2. Gửi cục Event này vào Kafka Topic "users"
    //    Chính biến TOPICS.users ở trên quyết định event đi vào topic nào
    await sendMessage(TOPICS.users, event);

    // 3. Trả về cho người dùng báo thành công
    //    (lúc này DB chưa chắc đã lưu xong, nhưng Kafka đã nhận)
    res.status(201).json({ success: true, eventId: event.id });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};
```

File: `src/controllers/doctors.controller.js` — Tương tự nhưng topic là `"doctor-events"`.

---

### 2. Pipeline Master (Nhận Event & Xử lý)

File: `redpanda-connect/pipelines/00-master-pipeline.yaml`

Đây là "Trái tim" của hệ thống. Nó là một file cấu hình YAML gồm 3 khối: INPUT → PIPELINE → OUTPUT.

```yaml
# ═══════════════════════════════════════════════════════════
# KHỐI 1: INPUT — Nơi dữ liệu đi vào
# ═══════════════════════════════════════════════════════════
input:
  kafka:
    addresses: [redpanda:9092]     # Địa chỉ Kafka broker (trong Docker)
    topics:                         # Lắng nghe đồng thời nhiều topic
      - users
      - orders
      - payments
      - crud-events
      - appointment-events
      - doctor-events
      - department-events
    consumer_group: connect-master-group  # Kafka theo dõi đã đọc đến đâu
    start_from_oldest: true               # Lần đầu: đọc từ message cũ nhất

# ═══════════════════════════════════════════════════════════
# KHỐI 2: PIPELINE — Xử lý trung gian (Nhào nặn dữ liệu)
# ═══════════════════════════════════════════════════════════
pipeline:
  threads: 2   # 2 luồng xử lý song song (xem mục "Cơ chế xử lý" ở trên)
  processors:
    # Bước 1: Kiểm tra tính hợp lệ
    - bloblang: |
        let hasId    = this.id != null
        let hasEvent = this.eventType != null && this.eventType != ""
        root = this
        root._meta.valid = $hasId && $hasEvent   # Đánh dấu hợp lệ (true) hay không (false)
        root._meta.topic = @kafka_topic           # Lưu lại tên topic xuất phát

    # Bước 2: Lọc bỏ rác
    - bloblang: |
        # Nếu hợp lệ → cho đi tiếp. Nếu không → gọi deleted() để vứt bỏ.
        root = if this._meta.valid { this } else { deleted() }

# ═══════════════════════════════════════════════════════════
# KHỐI 3: OUTPUT — Nơi dữ liệu đi ra (rẽ nhánh theo topic + eventType)
# ═══════════════════════════════════════════════════════════
output:
  switch:  # Giống lệnh switch/case trong lập trình
    cases:
      # Nếu event đến từ topic "doctor-events" VÀ eventType là "add"
      - check: 'this._meta.topic == "doctor-events" && this.eventType == "add"'
        output:
          mongodb:
            url: mongodb://host.docker.internal:27017
            database: app_db
            collection: doctors         # → Ghi vào bảng doctors
            operation: insert-one       # → Chèn 1 bản ghi mới
            document_map: |
              root = this               # Lấy toàn bộ event làm dữ liệu
              root._id = this.id        # Dùng id của event làm khóa chính MongoDB
```

---

### 3. Kỹ thuật cập nhật nhiều bảng (Broker Fan-Out)

Bài toán: Khi chuyển bác sĩ sang khoa khác (Update), ta vừa phải **sửa thông tin trong bảng Bác sĩ**, vừa phải **thêm ID bác sĩ đó vào mảng danh sách của bảng Khoa**.

Giải pháp: Dùng `broker` với pattern `fan_out` — 1 Event đi vào sẽ được nhân bản ra và kích hoạt 2 hành động ghi DB chạy song song.

```yaml
      # Cập nhật bác sĩ → SỬA ĐỒNG THỜI 2 BẢNG
      - check: 'this._meta.topic == "doctor-events" && this.eventType == "update"'
        output:
          broker:
            pattern: fan_out   # Nhân bản event ra cho TẤT CẢ outputs bên dưới
            outputs:

              # LUỒNG 1: Sửa bảng doctors
              - mongodb:
                  collection: doctors
                  operation: update-one        # Cập nhật 1 bản ghi
                  filter_map: |
                    root._id = this.id         # Tìm bác sĩ theo ID
                  document_map: |
                    root."$set" = this         # Ghi đè dữ liệu mới bằng toán tử $set

              # LUỒNG 2: Sửa bảng departments (thêm bác sĩ vào khoa)
              - mongodb:
                  collection: departments
                  operation: update-one
                  filter_map: |
                    root._id = this.departmentId   # Tìm khoa theo departmentId
                  document_map: |
                    # $addToSet: thêm ID vào mảng, tự tránh trùng lặp
                    root."$addToSet"."doctorIds" = this.id
```

> **Lưu ý quan trọng:** Cả 2 luồng output PHẢI thành công thì message mới được xử lý xong. Nếu 1 luồng lỗi → toàn bộ message sẽ bị retry → có thể gây tắc nghẽn pipeline (xem mục Bẫy thường gặp bên dưới).

---

## ⚠️ Bẫy thường gặp (Gotchas)

### Bẫy 1: Dùng `insert-one` với message bị đọc lại → Tắc nghẽn toàn bộ pipeline

**Tình huống:** Pipeline dùng `operation: insert-one` để thêm bản ghi. Message đã được insert thành công rồi. Nhưng vì lý do nào đó (restart container, consumer group reset), pipeline đọc lại message cũ → cố insert lần nữa → MongoDB báo lỗi `E11000 duplicate key` → Redpanda Connect **retry vô hạn** → **TẮC NGHẼN tất cả** (không message nào khác được xử lý nữa kể cả message ở topic hoàn toàn khác).

**Giải pháp:** Dùng `operation: update-one` + `upsert: true` thay cho `insert-one`:
```yaml
# ❌ NGUY HIỂM: Nếu đọc lại message cũ → lỗi duplicate key → tắc pipeline
operation: insert-one

# ✅ AN TOÀN: Chưa có → tạo mới, có rồi → cập nhật, không bao giờ lỗi
operation: update-one
upsert: true
filter_map: |
  root._id = this.id
document_map: |
  root."$set" = this
```

### Bẫy 2: Sửa file YAML nhưng pipeline không thay đổi

File `00-master-pipeline.yaml` chạy bên trong Docker container. Khi bạn sửa file trên máy, container không tự biết.

**Giải pháp:** Mỗi lần sửa file `.yaml` → BẮT BUỘC chạy:
```bash
docker-compose restart connect
```

### Bẫy 3: PUT update doctor nhưng bảng departments không xuất hiện

`update-one` với `upsert: false` chỉ **sửa bản ghi đã tồn tại**. Nếu bảng `departments` trống (chưa POST tạo khoa nào), thì lệnh update sẽ **không làm gì cả** (matched: 0).

**Giải pháp:** Luôn tạo dữ liệu phụ thuộc trước:
```
1. POST /api/departments  →  Tạo khoa (lấy ID khoa)
2. POST /api/doctors      →  Tạo bác sĩ (truyền departmentId = ID khoa ở trên)
3. PUT  /api/doctors/:id  →  Cập nhật bác sĩ → broker fan_out sửa CẢ 2 bảng
```

---

## 📁 Cấu trúc thư mục

```
connect_redpanda/
├── docker-compose.yml                # Dựng toàn bộ: Redpanda + MongoDB + Connect Pipeline
├── package.json                      # Cấu hình Node.js
│
├── redpanda-connect/
│   └── pipelines/
│       └── 00-master-pipeline.yaml   # ★ File cấu hình luồng xử lý chính (INPUT → PIPELINE → OUTPUT)
│
├── src/
│   ├── server.js                     # Entry point: khởi tạo Express, đăng ký route
│   ├── config/kafka.js               # Cấu hình kết nối tới Redpanda broker
│   ├── kafka/producer.js             # Hàm sendMessage(topic, event) gửi vào Kafka
│   ├── controllers/
│   │   ├── crud.controller.js        # Controller cho users/orders/payments (3 topic trong 1 file)
│   │   ├── doctors.controller.js     # Controller cho doctors (topic: doctor-events)
│   │   ├── departments.controller.js # Controller cho departments (topic: department-events)
│   │   └── appointment.controller.js # Controller cho appointments (topic: appointment-events)
│   └── routes/
│       ├── users.routes.js           # POST/PUT/DELETE /api/users
│       ├── orders.routes.js          # POST/PUT/DELETE /api/orders
│       ├── payments.routes.js        # POST/PUT/DELETE /api/payments
│       ├── doctors.routes.js         # POST/PUT/DELETE /api/doctors
│       ├── departments.routes.js     # POST/PUT/DELETE /api/departments
│       ├── appointment.routes.js     # POST /api/appointments
│       └── publish.routes.js         # POST /api/publish/:topic (dynamic)
│
└── postman/
    └── Redpanda_Connect_MongoDB.postman_collection.json  # File import Postman có sẵn để test
```

---

## 🚀 Cách chạy & Khắc phục lỗi

### 1. Cài đặt và khởi chạy

```bash
# 1. Cài thư viện Node.js
npm install

# 2. Khởi động hệ thống nền tảng (Kafka, DB, Pipeline) bằng Docker
docker-compose up -d

# 3. Khởi chạy API server Node.js
npm run dev
```

### 2. Xem log pipeline (debug)

```bash
# Xem 50 dòng log gần nhất của container connect
docker logs connect --tail 50

# Theo dõi log realtime (Ctrl+C để dừng)
docker logs connect -f
```

### 3. Cách test bằng Postman

1. Mở Postman → Import → chọn file `postman/Redpanda_Connect_MongoDB.postman_collection.json`.
2. Tạo khoa trước: Folder **9. CRUD - Departments** → POST → Copy `id` từ response.
3. Tạo bác sĩ: Folder **8. CRUD - Doctors** → POST → Dán `departmentId` = id khoa vừa tạo.
4. Cập nhật bác sĩ: PUT → Mở MongoDB UI → Kiểm tra cả bảng `doctors` lẫn `departments` đều thay đổi!

---

*Được xây dựng với: Node.js + KafkaJS + Redpanda Connect + MongoDB*
