# 🚀 Redpanda Connect × MongoDB × SMTP — Complete Pipeline Guide

Dự án này là một demo **Event-Driven Architecture (Kiến trúc hướng sự kiện)** sử dụng **Node.js, Redpanda/Kafka, Redpanda Connect, MongoDB và SMTP Gmail**.

Mục tiêu chính của project:

- Nhận dữ liệu từ Postman/API.
- Đẩy dữ liệu vào Redpanda topic.
- Cho Redpanda Connect đọc topic, xử lý dữ liệu rồi ghi vào MongoDB.
- Tạo tài khoản user và gửi email xác nhận.
- Ghi log kết quả gửi mail thành công/thất bại vào MongoDB thông qua Redpanda.

---

## 📋 Mục Lục

- [🏗 Kiến trúc tổng quan](#-kiến-trúc-tổng-quan)
- [🔁 Luồng xử lý chính](#-luồng-xử-lý-chính)
- [📦 Topic và Collection](#-topic-và-collection)
- [⚙️ Cơ chế hoạt động của Redpanda Connect](#️-cơ-chế-hoạt-động-của-redpanda-connect)
- [💻 Code Walkthrough](#-code-walkthrough)
  - [1. Express Server](#1-express-server)
  - [2. Publish Controller](#2-publish-controller)
  - [3. User Controller](#3-user-controller)
  - [4. Mail Service](#4-mail-service)
  - [5. Mail Log Service](#5-mail-log-service)
  - [6. Redpanda Connect Pipeline](#6-redpanda-connect-pipeline)
- [🔐 Cấu hình môi trường](#-cấu-hình-môi-trường)
- [🚀 Cách chạy project](#-cách-chạy-project)
- [🧪 Test bằng Postman](#-test-bằng-postman)
- [🧾 API Reference](#-api-reference)
- [📁 Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [⚠️ Lỗi thường gặp](#️-lỗi-thường-gặp)
- [📝 Ghi chú kỹ thuật](#-ghi-chú-kỹ-thuật)

---

## 🏗 Kiến Trúc Tổng Quan

Project không ghi dữ liệu trực tiếp vào MongoDB từ API. Thay vào đó, API chỉ có nhiệm vụ tạo event và gửi event vào Redpanda. Redpanda Connect sẽ là thành phần đọc event từ Redpanda và ghi vào MongoDB.

```txt
┌──────────────┐
│   Postman    │
│   Client     │
└──────┬───────┘
       │ HTTP Request
       ▼
┌────────────────────┐
│  Node.js Express   │
│  API Server        │
└──────┬─────────────┘
       │ KafkaJS Producer
       ▼
┌────────────────────┐
│ Redpanda / Kafka   │
│ topics:            │
│ - users            │
│ - mail-logs        │
└──────┬─────────────┘
       │ Kafka Consumer
       ▼
┌────────────────────┐
│ Redpanda Connect   │
│ Pipeline YAML      │
└──────┬─────────────┘
       │ MongoDB output
       ▼
┌────────────────────┐
│ MongoDB            │
│ app_db.users       │
│ app_db.mail_logs   │
└────────────────────┘
```

### Vì Sao Không Ghi Thẳng Vào MongoDB?

1. **Tách trách nhiệm rõ ràng**

   Node.js chỉ nhận request và publish event. Việc lưu dữ liệu do Redpanda Connect xử lý.

2. **Dễ mở rộng**

   Nếu sau này cần thêm xử lý như gửi webhook, ghi log, ghi sang database khác, ta có thể mở rộng pipeline thay vì sửa toàn bộ API.

3. **Có log và retry tốt hơn**

   Message đã vào Redpanda thì có thể được đọc lại, debug lại hoặc replay khi cần.

4. **Phù hợp kiến trúc event-driven**

   API phát ra sự kiện, các service/pipeline khác tự xử lý theo nhu cầu.

---

## 🔁 Luồng Xử Lý Chính

### Luồng 1: Publish User JSON Từ Postman

```txt
POST /api/publish/users
  -> Node.js nhận JSON body
  -> Node.js thêm id, eventType, eventTime nếu thiếu
  -> KafkaJS gửi message vào topic users
  -> Redpanda Connect đọc topic users
  -> Pipeline thêm _meta
  -> MongoDB lưu vào app_db.users
```

Ví dụ JSON gửi từ Postman:

```json
{
  "id": "user-001",
  "name": "Nguyen Van A",
  "email": "receiver@example.com",
  "status": "from_postman"
}
```
# 1. Xóa topic cũ (Sẽ mất tin nhắn đang chờ nếu có)
docker exec redpanda rpk topic delete send-mail

# 2. Tạo lại topic mới với 1 partition
docker exec redpanda rpk topic create send-mail -p 1


# tăng partition
docker exec redpanda rpk topic add-partitions send-mail -n 3



Sau khi qua API, event được đẩy vào Redpanda có dạng:

```json
{
  "id": "user-001",
  "name": "Nguyen Van A",
  "email": "receiver@example.com",
  "status": "from_postman",
  "eventType": "add",
  "eventTime": "2026-05-18T..."
}
```

Sau khi qua Redpanda Connect, dữ liệu lưu vào MongoDB có thêm `_meta`:

```json
{
  "_id": "user-001",
  "id": "user-001",
  "name": "Nguyen Van A",
  "email": "receiver@example.com",
  "status": "from_postman",
  "eventType": "add",
  "eventTime": "2026-05-18T...",
  "_meta": {
    "topic": "users",
    "processedAt": "2026-05-18T...",
    "pipeline": "simple-redpanda-to-mongodb"
  }
}
```

### Luồng 2: Tạo User Và Gửi Mail Xác Nhận

```txt
POST /api/users/register
  -> Node.js validate email
  -> Tạo user event trạng thái pending_email_confirmation
  -> Publish user event vào topic users
  -> Gửi email xác nhận bằng SMTP
  -> Tạo mail log success hoặc failed
  -> Publish mail log vào topic mail-logs
  -> Redpanda Connect ghi:
       users     -> app_db.users
       mail-logs -> app_db.mail_logs
```

Request:

```json
{
  "name": "Nguyen Van A",
  "email": "receiver@example.com"
}
```

User event:

```json
{
  "id": "uuid",
  "eventType": "add",
  "name": "Nguyen Van A",
  "email": "receiver@example.com",
  "status": "pending_email_confirmation",
  "emailVerified": false,
  "emailVerificationToken": "random-token",
  "eventTime": "2026-05-18T..."
}
```

Mail log nếu gửi thành công:

```json
{
  "id": "uuid",
  "eventType": "add",
  "mailRequestId": "uuid",
  "mailId": "<provider-message-id>",
  "providerMessageId": "<provider-message-id>",
  "userId": "uuid",
  "to": "receiver@example.com",
  "subject": "Xac nhan tai khoan",
  "template": "account_confirmation",
  "status": "success",
  "eventTime": "2026-05-18T...",
  "sentAt": "2026-05-18T..."
}
```

Mail log nếu gửi thất bại:

```json
{
  "id": "uuid",
  "eventType": "add",
  "mailRequestId": "uuid",
  "mailId": "uuid",
  "providerMessageId": null,
  "userId": "uuid",
  "to": "receiver@example.com",
  "subject": "Xac nhan tai khoan",
  "template": "account_confirmation",
  "status": "failed",
  "error": {
    "message": "Missing SMTP_USER or SMTP_PASS. Use an SMTP app password in .env.",
    "code": null
  },
  "eventTime": "2026-05-18T...",
  "failedAt": "2026-05-18T..."
}
```

### Luồng 3: Gửi 5 Mail Liên Tục

```txt
POST /api/users/send-five-mails
  -> Lặp tối đa 5 lần
  -> Mỗi lần gửi một email xác nhận
  -> Mỗi lần tạo một mail log riêng
  -> Publish từng log vào topic mail-logs
  -> Redpanda Connect ghi từng log vào app_db.mail_logs
```

Request:

```json
{
  "userId": "user-001",
  "name": "Nguyen Van A",
  "email": "receiver@example.com",
  "count": 5
}
```

`count` được giới hạn từ `1` đến `5` để tránh spam quá nhiều trong demo.

---

## 📦 Topic Và Collection

Pipeline hiện tại chỉ đọc 2 topic đơn giản:

| Redpanda Topic | Ý nghĩa | MongoDB Database | MongoDB Collection |
|---|---|---|---|
| `users` | Dữ liệu user, tạo tài khoản, xác nhận tài khoản | `app_db` | `users` |
| `mail-logs` | Log kết quả gửi email thành công/thất bại | `app_db` | `mail_logs` |

File pipeline:

```txt
redpanda-connect/pipelines/00-master-pipeline.yaml
```

---

## ⚙️ Cơ Chế Hoạt Động Của Redpanda Connect

Redpanda Connect hoạt động theo 3 khối chính:

```txt
INPUT  ->  PIPELINE PROCESSORS  ->  OUTPUT
```

### 1. Input

Input đọc message từ Redpanda/Kafka:

```yaml
input:
  kafka:
    addresses:
      - redpanda:9092
    topics:
      - users
      - mail-logs
    consumer_group: simple-connect-group
    start_from_oldest: true
```

Ý nghĩa:

| Field | Ý nghĩa |
|---|---|
| `addresses` | Địa chỉ broker Kafka/Redpanda mà Connect sẽ đọc |
| `topics` | Danh sách topic cần lắng nghe |
| `consumer_group` | Nhóm consumer để Kafka quản lý offset |
| `start_from_oldest` | Nếu group mới, đọc từ message cũ nhất |

### 2. Pipeline Processors

Pipeline hiện tại rất đơn giản, chỉ giữ nguyên message và thêm `_meta`:

```yaml
pipeline:
  processors:
    - bloblang: |
        root = this
        root._meta.topic = @kafka_topic
        root._meta.processedAt = now()
        root._meta.pipeline = "simple-redpanda-to-mongodb"
```

Ý nghĩa:

| Field | Ý nghĩa |
|---|---|
| `root = this` | Giữ nguyên toàn bộ JSON event |
| `@kafka_topic` | Lấy tên topic gốc mà message đi vào |
| `processedAt` | Thời gian Redpanda Connect xử lý message |
| `pipeline` | Tên pipeline để debug |

### 3. Output

Output dùng `switch` để route message theo topic:

```yaml
output:
  switch:
    cases:
      - check: 'this._meta.topic == "users"'
        output:
          mongodb:
            collection: users

      - check: 'this._meta.topic == "mail-logs"'
        output:
          mongodb:
            collection: mail_logs
```

Điểm quan trọng:

- Message từ `users` đi vào collection `users`.
- Message từ `mail-logs` đi vào collection `mail_logs`.
- Cả hai đều dùng `update-one` + `upsert: true` để tránh lỗi duplicate key khi message bị đọc lại.

---

## 💻 Code Walkthrough

### 1. Express Server

File:

```txt
src/server.js
```

Server cấu hình Express, JSON body parser, health check và route:

```javascript
app.use(express.json());

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "redpanda-connect-backend",
    time: new Date().toISOString(),
  });
});

app.use("/api/publish", require("./routes/publish.routes"));
app.use("/api/users", require("./routes/users.routes"));
```

Khi start server, producer Kafka sẽ connect trước:

```javascript
const start = async () => {
  await connectProducer();
  app.listen(PORT, () => {
    console.log(`Server  : http://localhost:${PORT}`);
  });
};
```

Nếu Redpanda chưa chạy hoặc `KAFKA_BROKERS` sai, API có thể không start được vì producer không kết nối được.

### 2. Publish Controller

File:

```txt
src/controllers/publish.controller.js
```

Controller này nhận topic từ URL:

```http
POST /api/publish/:topic
```

Ví dụ:

```http
POST /api/publish/users
```

Code tạo event:

```javascript
const event = {
  ...body,
  id: body.id || crypto.randomUUID(),
  eventType: body.eventType || "add",
  eventTime: new Date().toISOString(),
};
```

Điểm quan trọng:

- JSON body được giữ lại gần như nguyên dạng.
- Nếu body chưa có `id`, API tự tạo UUID.
- Nếu body chưa có `eventType`, API tự set `"add"`.
- Nếu body chưa có `eventTime`, API tự set thời gian hiện tại.
- Event sau đó được gửi vào Redpanda bằng KafkaJS.

Gửi message:

```javascript
await sendMessage(topic, event);
```

### 3. User Controller

File:

```txt
src/controllers/users.controller.js
```

Controller này có 3 chức năng chính:

| Hàm | API | Chức năng |
|---|---|---|
| `registerUser` | `POST /api/users/register` | Tạo user, gửi mail xác nhận, ghi log |
| `confirmUser` | `GET /api/users/confirm` | Publish event xác nhận tài khoản |
| `sendFiveConfirmationEmails` | `POST /api/users/send-five-mails` | Gửi tối đa 5 mail và ghi log từng lần |

#### `registerUser`

Luồng xử lý:

1. Lấy `email` từ JSON body.
2. Validate email.
3. Tạo `userId`.
4. Tạo token xác nhận email.
5. Publish user event vào topic `users`.
6. Gửi email xác nhận.
7. Publish mail log vào topic `mail-logs`.

Code tạo user event:

```javascript
const userEvent = {
  id: userId,
  eventType: "add",
  eventTime: new Date().toISOString(),
  name,
  email,
  status: "pending_email_confirmation",
  emailVerified: false,
  emailVerificationToken: token,
};
```

Nếu gửi mail thành công, response:

```json
{
  "success": true,
  "message": "Tao user thanh cong. Vui long check mail de xac nhan tai khoan.",
  "userId": "uuid",
  "mail": {
    "status": "success",
    "mailId": "<provider-message-id>",
    "logId": "uuid"
  }
}
```

Nếu gửi mail thất bại, user vẫn đã được publish vào Redpanda, response sẽ là `202`:

```json
{
  "success": true,
  "message": "User da duoc tao, nhung gui mail xac nhan that bai. Da ghi log that bai vao Redpanda.",
  "userId": "uuid",
  "mail": {
    "status": "failed",
    "mailId": "uuid",
    "logId": "uuid",
    "error": "..."
  }
}
```

#### `confirmUser`

API:

```http
GET /api/users/confirm?userId=user-001&token=demo-token
```

Endpoint này publish event update vào topic `users`:

```json
{
  "id": "user-001",
  "eventType": "update",
  "status": "email_confirmed",
  "emailVerified": true,
  "emailVerifiedAt": "2026-05-18T...",
  "emailVerificationToken": "demo-token"
}
```

Lưu ý: demo hiện tại chưa truy vấn MongoDB để kiểm tra token đúng/sai. Endpoint chỉ publish event xác nhận vào Redpanda.

#### `sendFiveConfirmationEmails`

API:

```http
POST /api/users/send-five-mails
```

Request:

```json
{
  "userId": "user-001",
  "name": "Nguyen Van A",
  "email": "receiver@example.com",
  "count": 5
}
```

Code giới hạn số lần gửi:

```javascript
return Math.min(Math.max(Math.trunc(parsed), 1), 5);
```

Vì vậy:

- `count < 1` thì thành `1`.
- `count > 5` thì thành `5`.
- Nếu không truyền `count`, mặc định là `5`.

### 4. Mail Service

File:

```txt
src/services/mail.service.js
```

Service này dùng `nodemailer` để gửi mail SMTP:

```javascript
const transporter = nodemailer.createTransport({
  host: config.host,
  port: config.port,
  secure: config.secure,
  auth: config.auth,
});
```

Thông tin SMTP lấy từ `.env`:

```env
SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SMTP_SECURE=true
SMTP_USER=your-email@gmail.com
SMTP_PASS=your-app-password
SMTP_FROM=your-email@gmail.com
```

Link xác nhận được build từ:

```env
APP_BASE_URL=http://localhost:3001
MAIL_CONFIRM_PATH=/api/users/confirm
```

Ví dụ link gửi trong email:

```txt
http://localhost:3001/api/users/confirm?userId=user-001&token=random-token
```

### 5. Mail Log Service

File:

```txt
src/services/mail-log.service.js
```

Service này tạo event log rồi publish vào topic `mail-logs`:

```javascript
const MAIL_LOG_TOPIC = process.env.MAIL_LOG_TOPIC || "mail-logs";
```

Mỗi log đều có:

| Field | Ý nghĩa |
|---|---|
| `id` | ID của log |
| `mailRequestId` | ID request gửi mail |
| `mailId` | ID mail, ưu tiên provider message id |
| `providerMessageId` | ID do SMTP provider trả về |
| `userId` | User liên quan |
| `to` | Email người nhận |
| `subject` | Tiêu đề mail |
| `status` | `success` hoặc `failed` |
| `error` | Chi tiết lỗi nếu gửi thất bại |
| `sentAt` | Thời điểm gửi thành công |
| `failedAt` | Thời điểm gửi thất bại |

### 6. Redpanda Connect Pipeline

File:

```txt
redpanda-connect/pipelines/00-master-pipeline.yaml
```

Pipeline đầy đủ hiện tại:

```yaml
input:
  kafka:
    addresses:
      - redpanda:9092
    topics:
      - users
      - mail-logs
    consumer_group: simple-connect-group
    start_from_oldest: true

pipeline:
  processors:
    - bloblang: |
        root = this
        root._meta.topic = @kafka_topic
        root._meta.processedAt = now()
        root._meta.pipeline = "simple-redpanda-to-mongodb"

output:
  switch:
    cases:
      - check: 'this._meta.topic == "users"'
        output:
          mongodb:
            url: mongodb://mongodb:27017
            database: app_db
            collection: users
            operation: update-one
            upsert: true
            filter_map: |
              root._id = this.id
            document_map: |
              root."$set" = this

      - check: 'this._meta.topic == "mail-logs"'
        output:
          mongodb:
            url: mongodb://mongodb:27017
            database: app_db
            collection: mail_logs
            operation: update-one
            upsert: true
            filter_map: |
              root._id = this.id
            document_map: |
              root."$set" = this
```

---

## 🔐 Cấu Hình Môi Trường

Tạo file `.env` từ `.env.example`:

```env
PORT=3001
KAFKA_BROKERS=localhost:19092
KAFKA_CLIENT_ID=connect-redpanda-backend

USERS_TOPIC=users
MAIL_LOG_TOPIC=mail-logs

APP_BASE_URL=http://localhost:3001
MAIL_CONFIRM_PATH=/api/users/confirm
MAIL_CONFIRM_SUBJECT=Xac nhan tai khoan

SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SMTP_SECURE=true
SMTP_USER=your-email@gmail.com
SMTP_PASS=your-app-password
SMTP_FROM=your-email@gmail.com
```

### Giải Thích Từng Biến

| Biến | Ý nghĩa |
|---|---|
| `PORT` | Port chạy Express API |
| `KAFKA_BROKERS` | Địa chỉ Redpanda/Kafka broker để Node.js publish message |
| `KAFKA_CLIENT_ID` | Client ID của KafkaJS |
| `USERS_TOPIC` | Topic chứa event user |
| `MAIL_LOG_TOPIC` | Topic chứa log gửi mail |
| `APP_BASE_URL` | Base URL để tạo link xác nhận tài khoản |
| `MAIL_CONFIRM_PATH` | Path xác nhận tài khoản |
| `MAIL_CONFIRM_SUBJECT` | Tiêu đề email xác nhận |
| `SMTP_HOST` | SMTP server, Gmail là `smtp.gmail.com` |
| `SMTP_PORT` | Port SMTP, Gmail SSL thường dùng `465` |
| `SMTP_SECURE` | `true` nếu dùng port `465` |
| `SMTP_USER` | Email dùng để đăng nhập SMTP |
| `SMTP_PASS` | App password SMTP |
| `SMTP_FROM` | Email hiển thị ở người gửi |

### Lưu Ý Với Gmail SMTP

Để dùng Gmail gửi mail:

1. Bật 2-Step Verification cho tài khoản Google.
2. Tạo App Password.
3. Gán App Password vào `SMTP_PASS`.
4. Đặt `SMTP_FROM` giống `SMTP_USER` nếu chưa cấu hình alias.

Không commit file `.env` lên Git vì có mật khẩu SMTP.

---

## 🚀 Cách Chạy Project

### 1. Cài thư viện

```bash
npm install
```

### 2. Chạy toàn bộ bằng Docker Compose

Project đã có `docker-compose.yml` để chạy đủ 4 service:

- `redpanda`: Kafka-compatible broker.
- `mongodb`: database lưu dữ liệu.
- `redpanda-connect`: đọc topic và ghi MongoDB bằng pipeline YAML.
- `api`: Node.js Express API.

Chạy tất cả:

```bash
docker compose up -d --build
```

Xem log:

```bash
docker compose logs -f
```

Xem log riêng Redpanda Connect:

```bash
docker compose logs -f redpanda-connect
```

Tắt nhưng giữ dữ liệu:

```bash
docker compose down
```

Tắt và xóa volume dữ liệu:

```bash
docker compose down -v
```

Trong Docker Compose, pipeline dùng MongoDB URL nội bộ:

```yaml
url: mongodb://mongodb:27017
```

Nếu bạn không chạy bằng Docker Compose mà chạy Redpanda Connect trực tiếp trên máy, hãy đổi URL MongoDB trong YAML thành:

```yaml
url: mongodb://localhost:27017
```

### 3. Tạo topic nếu broker không auto-create

Pipeline cần 2 topic:

```txt
users
mail-logs
```

Ví dụ với `rpk`:

```bash
rpk topic create users
rpk topic create mail-logs
```

Nếu chạy Redpanda trong Docker:

```bash
docker exec redpanda rpk topic create users
docker exec redpanda rpk topic create mail-logs
```

### 4. Chạy API

Development:

```bash
npm run dev
```

Production:

```bash
npm start
```

Khi chạy thành công, console sẽ in:

```txt
Redpanda Connect Backend
Server  : http://localhost:3001
Health  : GET  /health
Publish : POST /api/publish/:topic
Users   : POST /api/users | POST /api/users/register
Confirm : GET  /api/users/confirm?userId=...&token=...
Mail x5 : POST /api/users/send-five-mails
```

---

## 🧪 Test Bằng Postman

Postman collection:

```txt
postman/Simple_Redpanda_Mail.postman_collection.json
```

Import file này vào Postman.

Collection có 5 request:

| STT | Request | Mục đích |
|---|---|---|
| 1 | `Health` | Kiểm tra API còn sống |
| 2 | `Publish user event to Redpanda` | Gửi JSON vào topic `users` |
| 3 | `Register user and send confirmation mail` | Tạo user và gửi email |
| 4 | `Send 5 confirmation mails` | Gửi nhiều mail liên tục |
| 5 | `Confirm user` | Publish event xác nhận user |

Biến Postman:

```txt
baseUrl = http://localhost:3001
```

Nếu API chạy port khác, sửa `baseUrl`.

---

## 🧾 API Reference

### 1. Health Check

```http
GET /health
```

Response:

```json
{
  "status": "ok",
  "service": "redpanda-connect-backend",
  "time": "2026-05-18T..."
}
```

### 2. Publish JSON Vào Topic

```http
POST /api/publish/:topic
Content-Type: application/json
```

Ví dụ:

```http
POST /api/publish/users
```

Body:

```json
{
  "id": "user-001",
  "name": "Postman User",
  "email": "receiver@example.com",
  "status": "from_postman"
}
```

Response:

```json
{
  "success": true,
  "message": "Event da duoc gui vao topic \"users\"",
  "topic": "users",
  "eventId": "user-001",
  "event": {
    "id": "user-001",
    "name": "Postman User",
    "email": "receiver@example.com",
    "status": "from_postman",
    "eventType": "add",
    "eventTime": "2026-05-18T..."
  },
  "note": "Redpanda Connect se doc topic, xu ly pipeline va luu vao MongoDB."
}
```

### 3. Tạo User Và Gửi Mail

```http
POST /api/users/register
Content-Type: application/json
```

Body:

```json
{
  "name": "Nguyen Van A",
  "email": "receiver@example.com"
}
```

Response khi gửi mail thành công:

```json
{
  "success": true,
  "message": "Tao user thanh cong. Vui long check mail de xac nhan tai khoan.",
  "userId": "uuid",
  "userEventId": "uuid",
  "mail": {
    "status": "success",
    "mailId": "<provider-message-id>",
    "logId": "uuid"
  }
}
```

Response khi user đã publish nhưng mail lỗi:

```json
{
  "success": true,
  "message": "User da duoc tao, nhung gui mail xac nhan that bai. Da ghi log that bai vao Redpanda.",
  "userId": "uuid",
  "userEventId": "uuid",
  "mail": {
    "status": "failed",
    "mailId": "uuid",
    "logId": "uuid",
    "error": "..."
  }
}
```

### 4. Gửi 5 Mail Liên Tục

```http
POST /api/users/send-five-mails
Content-Type: application/json
```

Body:

```json
{
  "userId": "user-001",
  "name": "Postman User",
  "email": "receiver@example.com",
  "count": 5
}
```

Response:

```json
{
  "success": true,
  "message": "Da gui lien tuc 5 mail va ghi log vao Redpanda.",
  "userId": "user-001",
  "successCount": 5,
  "failureCount": 0,
  "results": [
    {
      "sequence": 1,
      "status": "success",
      "mailId": "<provider-message-id>",
      "logId": "uuid"
    }
  ]
}
```

### 5. Xác Nhận Tài Khoản

```http
GET /api/users/confirm?userId=user-001&token=demo-token
```

Response:

```json
{
  "success": true,
  "message": "Da nhan yeu cau xac nhan tai khoan.",
  "userId": "user-001"
}
```

---

## 📁 Cấu Trúc Thư Mục

```txt
connect_redpanda/
├── Dockerfile
├── README.md
├── package.json
├── package-lock.json
├── postman/
│   └── Simple_Redpanda_Mail.postman_collection.json
├── redpanda-connect/
│   └── pipelines/
│       ├── 00-master-pipeline.yaml
│       └── operation.md
└── src/
    ├── server.js
    ├── config/
    │   └── kafka.js
    ├── controllers/
    │   ├── publish.controller.js
    │   └── users.controller.js
    ├── kafka/
    │   ├── consumer.js
    │   └── producer.js
    ├── routes/
    │   ├── publish.routes.js
    │   └── users.routes.js
    └── services/
        ├── mail-log.service.js
        └── mail.service.js
```

---

## ⚠️ Lỗi Thường Gặp

### 1. API không start được

Nguyên nhân thường gặp:

- Redpanda chưa chạy.
- `KAFKA_BROKERS` sai.
- Port broker không đúng.

Kiểm tra `.env`:

```env
KAFKA_BROKERS=localhost:19092
```

Nếu Redpanda chạy trong Docker, hãy kiểm tra port expose ra host.

### 2. Gửi mail lỗi `Missing SMTP_USER or SMTP_PASS`

Nguyên nhân:

- `.env` chưa có `SMTP_USER`.
- `.env` chưa có `SMTP_PASS`.
- Server chưa restart sau khi sửa `.env`.

Cách sửa:

```env
SMTP_USER=your-email@gmail.com
SMTP_PASS=your-app-password
SMTP_FROM=your-email@gmail.com
```

### 3. Gmail báo sai mật khẩu

Không dùng mật khẩu Gmail thường. Phải dùng **Google App Password**.

Các bước:

1. Vào Google Account.
2. Bật 2-Step Verification.
3. Tạo App Password.
4. Copy app password vào `SMTP_PASS`.

### 4. MongoDB không có dữ liệu

Kiểm tra:

- Redpanda Connect đã chạy chưa.
- Pipeline có đọc đúng file `00-master-pipeline.yaml` không.
- Topic `users` và `mail-logs` có message chưa.
- MongoDB URL trong YAML đúng chưa.

Nếu sửa YAML, phải restart Redpanda Connect.

### 5. Dữ liệu bị ghi đè trong MongoDB

Pipeline đang dùng:

```yaml
operation: update-one
upsert: true
filter_map: |
  root._id = this.id
```

Nghĩa là nếu 2 message có cùng `id`, document trong MongoDB sẽ được update, không tạo document mới.

Đây là chủ ý để tránh lỗi duplicate key khi Redpanda Connect đọc lại message cũ.

### 6. Postman gửi thành công nhưng không thấy email

Kiểm tra:

- Email người nhận trong JSON body đúng chưa.
- Mail có vào spam không.
- SMTP account có bị Google chặn không.
- `SMTP_FROM` có giống `SMTP_USER` không.

---

## 📝 Ghi Chú Kỹ Thuật

- File `.env` chứa mật khẩu SMTP, không commit lên Git.
- Email người nhận được lấy từ JSON body, không lấy từ `.env`.
- `.env` chỉ chứa email gửi đi và SMTP app password.
- Project hiện tại chỉ còn 2 nhóm API: `publish` và `users`.
- Pipeline hiện tại chỉ xử lý `users` và `mail-logs`.
- Các script `produce` và `test:all` trong `package.json` đang trỏ tới thư mục `scripts`, nhưng thư mục `scripts` hiện không còn trong project.
- Nếu muốn mở rộng thêm `orders`, `payments`, `doctors`, chỉ cần thêm topic vào pipeline và thêm route/controller tương ứng.

---

## ✅ Tóm Tắt

Project này minh họa một luồng event-driven đơn giản nhưng thực tế:

```txt
Client gửi JSON
  -> Node.js publish event
  -> Redpanda lưu event
  -> Redpanda Connect xử lý event
  -> MongoDB lưu dữ liệu
```

Với chức năng email:

```txt
Tạo user
  -> Gửi mail xác nhận
  -> Ghi log gửi mail
  -> Lưu user và mail log vào MongoDB qua Redpanda Connect
```

*Được xây dựng với Node.js, KafkaJS, Redpanda, Redpanda Connect, MongoDB và Nodemailer.*


## 📦 Cơ Chế Phân Chia Và Xử Lý Partition Trong Redpanda

Sự phân chia này hoạt động theo 2 quy luật rất rõ ràng về **thứ tự nhận việc** (phía Producer) và **thứ tự xử lý** (phía Consumer):

### 1. Phân chia tin nhắn vào Partition (Phía Producer)
Khi Server gửi tin nhắn lên Redpanda, việc quyết định tin nhắn chui vào Partition nào sẽ dựa vào `key` (khóa) của tin nhắn đó:

*   **Không truyền `key` (Mặc định):**
    Redpanda tự động phân phối đều theo cơ chế **Round-Robin** (xoay vòng):
    *   *Event 1* ➡️ Partition 0
    *   *Event 2* ➡️ Partition 1
    *   *Event 3* ➡️ Partition 2
    *   *... (lặp lại liên tục)*
    Redpanda sẽ băm (hash) giá trị của `key` ra một con số để chọn partition tương ứng.
    *Giải thích chi tiết:* 
    Redpanda sử dụng thuật toán băm (thường là MurmurHash2) và áp dụng công thức chia lấy dư:
    ```text
    Chỉ số Partition = MurmurHash2(key) % Tổng số Partitions
    ```
    Vì hàm băm của cùng một giá trị `key` luôn trả về một số cố định, nên kết quả phép chia lấy dư của nó với tổng số partition sẽ luôn luôn trả về cùng một chỉ số partition (ví dụ: luôn ra Partition 2).
    > [!IMPORTANT]
    > **Quy tắc:** Tất cả các event có cùng giá trị `key` (ví dụ: chung một `userId`) sẽ **luôn luôn** được đẩy vào cùng một Partition.
    > 
    > *Mục đích:* Đảm bảo chuỗi hành động của một User (Đăng ký ➡️ Gửi mail ➡️ Xác thực) luôn nằm chung một luồng, tránh bị đảo lộn trình tự thời gian xử lý.

---

### 2. Thứ tự xử lý tin nhắn của các Worker (Phía Consumer)

#### A. Trong phạm vi 1 Partition (Thứ tự tuyệt đối - FIFO)
Redpanda cam kết thứ tự xử lý nghiêm ngặt theo nguyên tắc **First In, First Out** (Vào trước - Ra trước):
*   Tin nhắn nào ghi vào phân vùng trước (có offset nhỏ hơn) bắt buộc phải được xử lý trước. Không có chuyện nhảy cóc hay tranh lượt.
*   *Ví dụ:* Nếu Partition 0 chứa các tin nhắn theo thứ tự `[1, 3, 5]`, Worker sẽ xử lý lần lượt `1` ➡️ `3` ➡️ `5`.

#### B. Giữa các Partition khác nhau (Không đảm bảo thứ tự)
Không có sự ràng buộc hay cam kết thứ tự xử lý giữa các partition khác nhau:
*   *Ví dụ:* Worker A đọc Partition 0 (tin `1`, `3`), Worker B đọc Partition 1 (tin `2`, `4`). Có khả năng Worker B xử lý tin `2` và `4` xong xuôi trước cả khi Worker A bắt đầu hoặc hoàn thành tin `1`.

---

### 💡 Tóm lại (Best Practices)
*   **Để tối ưu tốc độ (Max Throughput):** Chia thành nhiều Partitions, chạy nhiều Workers song song và không cần truyền `key` (để Redpanda tự động phân phối đều tải cho các Worker).
*   **Để bảo toàn trình tự (Strict Ordering):** Bắt buộc phải cấu hình truyền `key` (chính là `userId`) khi gửi tin nhắn. Tất cả các event của user đó sẽ đi vào cùng 1 partition và được xử lý theo đúng trình tự trước sau.