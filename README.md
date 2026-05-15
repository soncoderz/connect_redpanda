# 🚀 Redpanda Connect × MongoDB — Complete Pipeline Guide

Dự án này là một hệ thống **Event-Driven Architecture (Kiến trúc Hướng Sự kiện)** hoàn chỉnh sử dụng **Node.js, Kafka (Redpanda), Redpanda Connect, và MongoDB**.

Nó giải quyết một bài toán phổ biến trong Microservices: **Làm sao để lưu dữ liệu vào cơ sở dữ liệu một cách bất đồng bộ và xử lý cập nhật nhiều bảng cùng lúc thông qua Kafka?**

---

## 📋 Mục lục

- [🏗 Kiến trúc tổng quan](#-kiến-trúc-tổng-quan)
- [💻 Giải thích luồng dữ liệu (Code Walkthrough)](#-giải-thích-luồng-dữ-liệu-code-walkthrough)
  - [1. Node.js Controller (Gửi Event)](#1-nodejs-controller-gửi-event)
  - [2. Pipeline Master (Nhận Event & Xử lý)](#2-pipeline-master-nhận-event--xử-lý)
  - [3. Kỹ thuật cập nhật nhiều bảng (Broker Fan-Out)](#3-kỹ-thuật-cập-nhật-nhiều-bảng-broker-fan-out)
- [📁 Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [🚀 Cách chạy & Khắc phục lỗi](#-cách-chạy--khắc-phục-lỗi)

---

## 🏗 Kiến trúc tổng quan

Mọi thao tác ghi/xóa/sửa (CRUD) từ người dùng không được ghi trực tiếp vào MongoDB. Thay vào đó, nó đi qua Kafka theo luồng sau:

```mermaid
graph TD
    Client[Client (Postman)] -->|HTTP POST /api/doctors| Express[Node.js Express API]
    Express -->|Gửi Kafka Event| Kafka[(Redpanda / Kafka)]
    Kafka -->|Topic: doctor-events| Connect[Redpanda Connect Pipeline]
    Connect -->|Lọc & Biến đổi| Switch{Rẽ nhánh theo eventType}
    Switch -->|add| MongoInsert[MongoDB: insert-one]
    Switch -->|update| MongoBroker[Broker Fan-Out: sửa nhiều bảng]
    MongoBroker --> MongoUpdate1[MongoDB: Cập nhật bảng doctors]
    MongoBroker --> MongoUpdate2[MongoDB: Thêm ID vào bảng departments]
```

### Tại sao lại làm phức tạp như vậy?
1. **Chống quá tải (Buffer):** Khi có hàng chục ngàn request cùng lúc, Node.js chỉ việc đẩy vào Kafka (rất nhanh). Pipeline sẽ từ từ nhặt ra để ghi vào MongoDB, giúp DB không bị sập.
2. **Decoupling (Giảm phụ thuộc):** App Node.js không cần biết MongoDB lưu trữ ra sao. Nó chỉ quan tâm là "đã có sự kiện tạo bác sĩ xảy ra".
3. **Cập nhật đa bảng dễ dàng:** Một sự kiện (như update) có thể kích hoạt nhiều thao tác song song trên nhiều bảng khác nhau mà không cần Node.js phải xử lý giao dịch (transaction) phức tạp.

---

## 💻 Giải thích luồng dữ liệu (Code Walkthrough)

Hãy cùng đi theo luồng của một Request tạo và cập nhật Bác sĩ (`doctors`) để xem code hoạt động thế nào.

### 1. Node.js Controller (Gửi Event)

File: `src/controllers/crud.controller.js`

Thay vì lưu thẳng vào Database, Node.js gom dữ liệu lại thành một cục gọi là **Event** (Sự kiện) và đẩy lên Kafka.

```javascript
// Khai báo topic tương ứng với từng đối tượng
const TOPICS = {
  users:    "users",
  orders:   "orders",
  payments: "payments",
  doctors:  "doctor-events" // Mọi thứ liên quan đến bác sĩ sẽ chui vào đây
};

// Hàm tạo mới bác sĩ
const createDoctor = async (req, res) => {
  try {
    // 1. Tạo cục Event bao gồm: ID, Loại sự kiện (add), Thời gian, và Dữ liệu (req.body)
    const event = {
      id: crypto.randomUUID(),           // Cấp cho bác sĩ một mã ID độc nhất
      eventType: "add",                  // Loại hành động: Thêm mới
      eventTime: new Date().toISOString(),
      ...req.body                        // Lấy dữ liệu người dùng gửi lên (name, specialty...)
    };

    // 2. Gửi cục Event này vào Kafka Topic "doctor-events"
    await sendMessage(TOPICS.doctors, event);

    // 3. Trả về cho người dùng báo thành công (lúc này DB chưa chắc đã lưu xong, nhưng Kafka đã nhận)
    res.status(201).json({ success: true, eventId: event.id });
  } catch (err) {
    // ...
  }
};
```

---

### 2. Pipeline Master (Nhận Event & Xử lý)

File: `redpanda-connect/pipelines/00-master-pipeline.yaml`

Đây là "Trái tim" của hệ thống. Nó là một file cấu hình định nghĩa luồng chảy của dữ liệu từ Kafka vào MongoDB.

```yaml
# 1. INPUT: Nơi dữ liệu đi vào
input:
  kafka:
    addresses: [redpanda:9092]
    topics:
      - doctor-events      # Lắng nghe topic bác sĩ
      - department-events  # Lắng nghe topic phòng ban
      # ... các topic khác
    consumer_group: connect-master-group

# 2. PIPELINE: Xử lý trung gian (Nhào nặn dữ liệu)
pipeline:
  processors:
    - bloblang: |
        # Ngôn ngữ Bloblang: Kiểm tra xem event có id và eventType không?
        let hasId    = this.id != null
        let hasEvent = this.eventType != null && this.eventType != ""
        root = this
        root._meta.valid = $hasId && $hasEvent # Đánh dấu hợp lệ hay không

    - bloblang: |
        # Lọc bỏ rác: Nếu hợp lệ thì cho đi tiếp, nếu không thì gọi deleted() để vứt bỏ
        root = if this._meta.valid { this } else { deleted() }

# 3. OUTPUT: Nơi dữ liệu đi ra (Đích đến MongoDB)
output:
  switch: # Lệnh switch/case để rẽ nhánh
    cases:
      # TRƯỜNG HỢP: Nếu đến từ topic bác sĩ VÀ là lệnh "add"
      - check: 'this._meta.topic == "doctor-events" && this.eventType == "add"'
        output:
          mongodb:
            url: mongodb://host.docker.internal:27017
            database: app_db
            collection: doctors    # Chọn bảng (collection) doctors
            operation: insert-one  # Thao tác: CHÈN 1 BẢN GHI MỚI
            document_map: |
              root = this          # Lấy toàn bộ nội dung event làm dữ liệu lưu vào DB
              root._id = this.id   # Lấy id của event làm khóa chính (_id) cho MongoDB
```

---

### 3. Kỹ thuật cập nhật nhiều bảng (Broker Fan-Out)

Bài toán: Khi chuyển bác sĩ sang khoa khác (Update), ta vừa phải **sửa thông tin trong bảng Bác sĩ**, vừa phải **thêm ID bác sĩ đó vào mảng danh sách của bảng Khoa**.

Giải pháp trong Pipeline: Dùng `broker` với pattern `fan_out` (Nhân bản). 1 Event đi vào sẽ kích hoạt 2 hành động ghi DB cùng lúc.

```yaml
      # TRƯỜNG HỢP: Cập nhật bác sĩ (SỬA ĐỒNG THỜI 2 BẢNG)
      - check: 'this._meta.topic == "doctor-events" && this.eventType == "update"'
        output:
          broker:
            pattern: fan_out # Tách event này ra làm 2 luồng chạy song song
            outputs:
              
              # LUỒNG 1: Sửa bảng doctors
              - mongodb:
                  database: app_db
                  collection: doctors
                  operation: update-one # Lệnh CẬP NHẬT
                  filter_map: |
                    root._id = this.id  # Tìm bác sĩ theo ID
                  document_map: |
                    root."$set" = this  # Dùng toán tử $set của MongoDB để ghi đè dữ liệu mới
              
              # LUỒNG 2: Sửa bảng departments
              - mongodb:
                  database: app_db
                  collection: departments
                  operation: update-one # Lệnh CẬP NHẬT
                  filter_map: |
                    # Tìm đúng cái khoa mà bác sĩ vừa chuyển tới (dựa vào departmentId gửi lên)
                    root._id = this.departmentId
                  document_map: |
                    # Dùng $addToSet của MongoDB: Nhét ID của bác sĩ vào mảng "doctorIds" của khoa đó
                    # ($addToSet thông minh ở chỗ nếu ID đã có trong mảng thì nó sẽ bỏ qua, không bị trùng)
                    root."$addToSet"."doctorIds" = this.id
```
**Chữ `-one` có ý nghĩa gì?**
Tại sao luôn dùng `insert-one`, `update-one`?
Bởi vì mỗi Event từ Kafka chỉ đại diện cho sự thay đổi của **một** đối tượng duy nhất. Dùng `-one` giúp đảm bảo an toàn, tránh việc lỡ cấu hình sai (`filter_map` bị rỗng) thì nó sẽ xóa/sửa hàng loạt dữ liệu trong DB.

---

## 📁 Cấu trúc thư mục

```
connect_redpanda/
├── docker-compose.yml              # Dựng Kafka, MongoDB, và Redpanda Connect
├── package.json                    # Cấu hình Node.js
├── redpanda-connect/
│   └── pipelines/
│       └── 00-master-pipeline.yaml # File cấu hình luồng xử lý chính
├── src/
│   ├── server.js                   # Entry point của Node.js (khởi tạo API)
│   ├── config/kafka.js             # Kết nối tới Redpanda
│   ├── kafka/producer.js           # Chứa hàm sendMessage() gửi vào Kafka
│   ├── controllers/
│   │   └── crud.controller.js      # Gói gọn logic đóng gói Event và đẩy lên Kafka
│   └── routes/                     # Các file định tuyến API (POST/PUT/DELETE)
└── postman/
    └── Redpanda_Connect_MongoDB.postman_collection.json # File test API có sẵn
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

### 2. Sửa lỗi kinh điển: Sửa YAML nhưng không ăn code?

Khi bạn chỉnh sửa code Node.js (`src/`), server sẽ tự khởi động lại nhờ công cụ như nodemon/npm.
Tuy nhiên, khi bạn chỉnh sửa file `00-master-pipeline.yaml`, bạn đang sửa file cấu hình của một **Docker Container**. Container đó không tự biết file đã thay đổi.

**Giải pháp:** Mọi lần sửa file `.yaml` trong thư mục `redpanda-connect`, BẮT BUỘC phải chạy lệnh sau để khởi động lại bộ xử lý:

```bash
docker-compose restart connect
```

### 3. Cách test bằng Postman
1. Mở Postman.
2. Bấm Import.
3. Chọn file `postman/Redpanda_Connect_MongoDB.postman_collection.json` nằm trong dự án.
4. Chọn các Folder cuối cùng (như **8. CRUD - Doctors**) và bấm Send thử các request POST, PUT. Mở MongoDB UI để xem kết quả biến đổi của các bảng dữ liệu!
