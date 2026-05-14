# 🚀 Redpanda Connect × MongoDB — Complete Pipeline Guide

> **33 use-cases** được implement đầy đủ: từ đọc topic đơn giản đến CRUD, validate, DLQ, batch, retry, transform, và routing nâng cao.

---

## 📋 Mục lục

- [Kiến trúc tổng quan](#-kiến-trúc-tổng-quan)
- [Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [Cách chạy](#-cách-chạy)
- [API Reference](#-api-reference)
- [Danh sách 33 Use-Cases](#-danh-sách-33-use-cases)
- [Chi tiết từng Pipeline](#-chi-tiết-từng-pipeline)
- [Thay đổi Database / Topic / Collection](#-thay-đổi-database--topic--collection)
- [Phân loại Event (eventType)](#-phân-loại-event-eventtype)
- [Redpanda Connect làm được gì?](#-redpanda-connect-làm-được-gì)

---

## 🏗 Kiến trúc tổng quan

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT / API TEST                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │ HTTP POST
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              Node.js Express API (Port 3000)                    │
│                                                                 │
│  POST /api/publish/:topic    ← Dynamic topic                    │
│  POST /api/users             ← eventType: "add"                 │
│  PUT  /api/users/:id         ← eventType: "update"              │
│  DELETE /api/users/:id       ← eventType: "delete"              │
│  PATCH /api/users/:id        ← eventType: "upsert"              │
└──────────────────────────────┬──────────────────────────────────┘
                               │ KafkaJS Producer
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Redpanda Broker (:9092)                       │
│                                                                 │
│  Topics: users, orders, payments, crud-events, all-events ...   │
└──────────────────────────────┬──────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│ connect-master   │ │  connect-crud    │ │  connect-validate    │
│ (Pipeline 00)    │ │  (Pipeline 08)   │ │  (Pipeline 07)       │
│ Full routing     │ │  CRUD operations │ │  Validate + DLQ      │
└────────┬─────────┘ └────────┬─────────┘ └──────────┬───────────┘
         │                    │                       │
         └────────────────────┼───────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              MongoDB (:27017) — mongodb://host.docker.internal:27017           │
│                                                                 │
│  Database: app_db                                               │
│  Collections: users, orders, payments, resources,              │
│               all_events, valid_orders, audit_log, ...         │
└─────────────────────────────────────────────────────────────────┘
```

### Luồng dữ liệu chi tiết

```
API POST /api/orders (body: { eventType: "add", ... })
   ↓
Express Route → sendMessage("orders", { id, eventType: "add", ... })
   ↓
Redpanda Topic: "orders"
   ↓
Redpanda Connect đọc topic "orders"
   ↓
Pipeline Processor: validate → filter → transform → add metadata
   ↓
Switch output theo eventType:
  "add"    → MongoDB insert-one  → collection orders
  "update" → MongoDB update-one  → collection orders
  "delete" → MongoDB delete-one  → collection orders
```

---

## 📁 Cấu trúc thư mục

```
connect_redpanda/
├── docker-compose.yml              # Toàn bộ stack: Redpanda + MongoDB + Connect × 5
├── Dockerfile                      # Build Node.js API
├── .env                            # Biến môi trường local
├── .env.example                    # Template biến môi trường
│
├── redpanda-connect/
│   ├── connect.yaml                # Pipeline cũ (backward compat)
│   └── pipelines/
│       ├── 00-master-pipeline.yaml         # Pipeline tổng hợp (#32, #33)
│       ├── 01-single-topic-to-collection.yaml  # 1 topic → 1 collection (#1, #2)
│       ├── 02-multi-topic-multi-collection.yaml # N topic → N collection (#3, #24)
│       ├── 03-multi-topic-one-collection.yaml   # N topic → 1 collection (#4)
│       ├── 04-filter-by-eventtype.yaml     # Filter theo eventType (#5, #6)
│       ├── 05-filter-by-groupid.yaml       # Filter theo groupId (#7, #26)
│       ├── 06-transform-data.yaml          # Transform, rename, delete field (#8-12)
│       ├── 07-validate-and-dlq.yaml        # Validate + DLQ (#13-16)
│       ├── 08-crud-operations.yaml         # Insert/Update/Delete/Upsert (#17-20)
│       ├── 09-route-by-eventtype.yaml      # Route theo eventType (#25)
│       └── 10-batch-retry-errorlog.yaml    # Batch, Retry, Log (#28-31)
│
└── src/
    ├── server.js                   # Express app
    ├── config/kafka.js             # KafkaJS config
    ├── kafka/
    │   ├── producer.js             # Gửi message vào topic
    │   └── consumer.js             # Node.js consumer (optional)
    └── routes/
        ├── publish.routes.js       # POST /api/publish/:topic (dynamic)
        ├── crud.routes.js          # CRUD factory route
        └── appointment.routes.js   # Legacy route
```

---

## 🚀 Cách chạy

### Yêu cầu

- Docker & Docker Compose
- Node.js 20+ (chỉ cần cho dev local)

### 1. Clone và cấu hình

```bash
# Copy env
cp .env.example .env
```

### 2. Chạy toàn bộ stack

```bash
# Khởi động tất cả: Redpanda + MongoDB + 5 pipeline Connect + API
docker compose up -d

# Xem logs
docker compose logs -f connect-master
docker compose logs -f connect-crud
docker compose logs -f api
```

### 3. Chạy chỉ một pipeline cụ thể

```bash
# Chỉ chạy pipeline CRUD
docker compose up -d redpanda mongo connect-crud

# Chỉ chạy pipeline validate + DLQ
docker compose up -d redpanda mongo connect-validate
```

### 4. Chạy local (dev)

```bash
npm install
npm run dev
```

### 5. Tạo topics cần thiết (lần đầu)

```bash
# Tạo các topic
docker exec redpanda rpk topic create users orders payments crud-events all-events high-volume-events events raw-events orders-dlq

# Xem danh sách topic
docker exec redpanda rpk topic list
```

---

## 📡 API Reference

### Base URL

```
http://localhost:3000
```

### GET /health

```json
{ "status": "ok", "service": "redpanda-connect-backend" }
```

---

### POST /api/publish/:topic — Dynamic Publish

Gửi message vào **bất kỳ topic nào** bạn muốn. Redpanda Connect sẽ xử lý phía sau.

```bash
# Ví dụ: gửi vào topic "users"
curl -X POST http://localhost:3000/api/publish/users \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "add",
    "name": "Nguyen Van A",
    "email": "a@example.com"
  }'

# Ví dụ: gửi vào topic "orders" với eventType "delete"
curl -X POST http://localhost:3000/api/publish/orders \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "delete",
    "id": "abc-123"
  }'
```

**Body Parameters:**

| Field | Type | Mặc định | Mô tả |
|-------|------|----------|-------|
| `eventType` | string | `"add"` | `add` / `update` / `delete` / `upsert` / custom |
| `id` | string | auto UUID | ID của document |
| `groupId` | string | null | Dùng cho filter theo group |
| `database` | string | null | Metadata (pipeline đọc nếu cần) |
| `collection` | string | null | Metadata (pipeline đọc nếu cần) |
| `...payload` | any | - | Bất kỳ data nào khác |

---

### CRUD Routes (shorthand)

#### Users

```bash
# Tạo user mới → eventType: "add"
curl -X POST http://localhost:3000/api/users \
  -H "Content-Type: application/json" \
  -d '{ "name": "Nguyen Van A", "email": "a@example.com" }'

# Cập nhật user → eventType: "update"
curl -X PUT http://localhost:3000/api/users/USER_ID \
  -H "Content-Type: application/json" \
  -d '{ "name": "Nguyen Van B" }'

# Xóa user → eventType: "delete"
curl -X DELETE http://localhost:3000/api/users/USER_ID

# Upsert user → eventType: "upsert"
curl -X PATCH http://localhost:3000/api/users/USER_ID \
  -H "Content-Type: application/json" \
  -d '{ "name": "Nguyen Van A", "email": "new@example.com" }'
```

#### Orders

```bash
# Tạo order
curl -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -d '{ "item": "Laptop", "amount": 25000000 }'

# Cập nhật order
curl -X PUT http://localhost:3000/api/orders/ORDER_ID \
  -H "Content-Type: application/json" \
  -d '{ "status": "PAID" }'

# Xóa order
curl -X DELETE http://localhost:3000/api/orders/ORDER_ID
```

#### Payments

```bash
# Tạo payment
curl -X POST http://localhost:3000/api/payments \
  -H "Content-Type: application/json" \
  -d '{ "orderId": "xyz", "amount": 500000, "method": "MOMO" }'
```

---

### Thay đổi topic khi POST

```bash
# Gửi order vào topic tùy chỉnh
curl -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "my-custom-topic",
    "item": "Phone",
    "amount": 10000000
  }'
```

---

## 📋 Danh sách 33 Use-Cases

| # | Use-case | Pipeline File | Trạng thái |
|---|----------|---------------|-----------|
| 1 | Đọc topic → lưu MongoDB | `01-single-topic-to-collection.yaml` | ✅ |
| 2 | Đọc 1 topic → 1 collection | `01-single-topic-to-collection.yaml` | ✅ |
| 3 | Đọc nhiều topic → nhiều collection | `02-multi-topic-multi-collection.yaml` | ✅ |
| 4 | Đọc nhiều topic → 1 collection | `03-multi-topic-one-collection.yaml` | ✅ |
| 5 | Đọc topic có nhiều eventType, chỉ lưu 1 loại | `04-filter-by-eventtype.yaml` | ✅ |
| 6 | Lọc message theo field eventType | `04-filter-by-eventtype.yaml` | ✅ |
| 7 | Lọc message theo field groupId | `05-filter-by-groupid.yaml` | ✅ |
| 8 | Transform dữ liệu trước khi lưu | `06-transform-data.yaml` | ✅ |
| 9 | Thêm field `savedAt` | `01` → `10` (tất cả pipeline) | ✅ |
| 10 | Thêm field `source` | `01` → `10` (tất cả pipeline) | ✅ |
| 11 | Xóa field không cần thiết | `06-transform-data.yaml` | ✅ |
| 12 | Đổi tên field | `06-transform-data.yaml` | ✅ |
| 13 | Validate dữ liệu trước khi lưu | `07-validate-and-dlq.yaml` | ✅ |
| 14 | Message hợp lệ → lưu MongoDB | `07-validate-and-dlq.yaml` | ✅ |
| 15 | Message lỗi → bỏ qua (`deleted()`) | `00-master-pipeline.yaml` | ✅ |
| 16 | Message lỗi → gửi sang DLQ topic | `07-validate-and-dlq.yaml` | ✅ |
| 17 | Insert document mới | `08-crud-operations.yaml` | ✅ |
| 18 | Update document | `08-crud-operations.yaml` | ✅ |
| 19 | Delete document | `08-crud-operations.yaml` | ✅ |
| 20 | Upsert document | `08-crud-operations.yaml` | ✅ |
| 21 | Ghi topic `users` → collection `users` | `02-multi-topic-multi-collection.yaml` | ✅ |
| 22 | Ghi topic `orders` → collection `orders` | `02-multi-topic-multi-collection.yaml` | ✅ |
| 23 | Ghi topic `payments` → collection `payments` | `02-multi-topic-multi-collection.yaml` | ✅ |
| 24 | Route theo topic → collection khác nhau | `02-multi-topic-multi-collection.yaml` | ✅ |
| 25 | Route theo eventType → collection khác nhau | `09-route-by-eventtype.yaml` | ✅ |
| 26 | Route theo groupId → collection khác nhau | `05-filter-by-groupid.yaml` | ✅ |
| 27 | consumer_group quản lý offset | Tất cả pipeline | ✅ |
| 28 | Scale bằng nhiều instance + consumer_group | `10-batch-retry-errorlog.yaml` + Docker | ✅ |
| 29 | Batch message để ghi MongoDB hiệu quả | `10-batch-retry-errorlog.yaml` | ✅ |
| 30 | Retry khi MongoDB lỗi tạm thời | `07-validate-and-dlq.yaml` | ✅ |
| 31 | Ghi log lỗi khi lưu MongoDB thất bại | `07-validate-and-dlq.yaml` | ✅ |
| 32 | Pipeline input → processors → output | `00-master-pipeline.yaml` | ✅ |
| 33 | Thay Kafka Connect MongoDB Sink | Toàn bộ hệ thống | ✅ |

---

## 🔧 Chi tiết từng Pipeline

### Pipeline 00: Master Pipeline (Production)

**File:** `redpanda-connect/pipelines/00-master-pipeline.yaml`

Pipeline tổng hợp dùng cho production. Tích hợp: validate → filter → transform → CRUD routing.

```
Topics đọc: users, orders, payments, crud-events
Validate → Bỏ message lỗi → Thêm metadata
Switch output theo topic + eventType:
  crud-events + add    → insert-one  → resources
  crud-events + update → update-one  → resources
  crud-events + delete → delete-one  → resources
  users                → insert-one  → users
  orders               → insert-one  → orders
  payments             → insert-one  → payments
  (default)            → insert-one  → unrouted_events
```

---

### Pipeline 01: Single Topic → Single Collection

**File:** `redpanda-connect/pipelines/01-single-topic-to-collection.yaml`

```yaml
# Thay đổi topic, database, collection ở đây:
input.kafka.topics: [users]
output.mongodb.database: app_db
output.mongodb.collection: users
```

---

### Pipeline 02: Multi-Topic → Multi-Collection

**File:** `redpanda-connect/pipelines/02-multi-topic-multi-collection.yaml`

```
users    topic → users    collection
orders   topic → orders   collection
payments topic → payments collection
```

Routing dựa trên Kafka metadata `@kafka_topic`.

---

### Pipeline 07: Validate + DLQ

**File:** `redpanda-connect/pipelines/07-validate-and-dlq.yaml`

```
Message đến
  │
  ├── validate(id, eventType, amount > 0)
  │
  ├── Valid    → retry(5x) → MongoDB → valid_orders
  │                       ↘ thất bại → stdout log
  │
  └── Invalid  → kafka topic: orders-dlq
```

---

### Pipeline 08: CRUD Operations

**File:** `redpanda-connect/pipelines/08-crud-operations.yaml`

```
eventType == "add"    → insert-one   (tạo mới)
eventType == "update" → update-one   (cập nhật)
eventType == "delete" → delete-one   (xóa)
eventType == "upsert" → update-one + upsert: true
```

---

### Pipeline 10: Batch + Retry + Scale

**File:** `redpanda-connect/pipelines/10-batch-retry-errorlog.yaml`

```
- threads: 4                   # 4 goroutine xử lý song song
- fetch_min_bytes: 1024        # Đợi ít nhất 1KB để batch
- fetch_max_wait: 500ms        # Timeout batch
- retry.max_retries: 5         # Retry 5 lần
- retry.backoff: 1s → 30s      # Exponential backoff
```

---

## ⚙️ Thay đổi Database / Topic / Collection

### Cách 1: Chỉnh sửa file YAML (cố định)

```yaml
# Trong bất kỳ file pipeline nào:
input:
  kafka:
    topics:
      - my-new-topic        # ← Đổi topic ở đây

output:
  mongodb:
    database: my_database   # ← Đổi database ở đây
    collection: my_collection  # ← Đổi collection ở đây
```

### Cách 2: Dùng biến môi trường

```yaml
output:
  mongodb:
    url: ${MONGO_URL:mongodb://host.docker.internal:27017}
    database: ${MONGO_DATABASE:app_db}
    collection: ${MONGO_COLLECTION:events}
```

```bash
# Truyền biến khi chạy
MONGO_DATABASE=production_db MONGO_COLLECTION=v2_events \
  docker compose up connect-master
```

### Cách 3: API Dynamic (không cần restart)

```bash
# Gửi vào topic bất kỳ qua API
curl -X POST http://localhost:3000/api/publish/my-custom-topic \
  -d '{ "eventType": "add", "data": "..." }'
```

Sau đó cấu hình pipeline để đọc `my-custom-topic` trong `.yaml`.

---

## 🏷️ Phân loại Event (eventType)

Hệ thống sử dụng field `eventType` trong message để phân loại và route:

| `eventType` | MongoDB Operation | Mô tả |
|-------------|-------------------|-------|
| `add` | `insert-one` | Tạo document mới |
| `update` | `update-one` | Cập nhật document theo `id` |
| `delete` | `delete-one` | Xóa document theo `id` |
| `upsert` | `update-one + upsert:true` | Tạo mới nếu chưa có |
| Custom string | Xem pipeline 09 | Route sang collection khác |

### Ví dụ payload đầy đủ

```json
{
  "id": "uuid-v4",
  "eventType": "update",
  "eventTime": "2026-05-14T10:00:00.000Z",
  "groupId": "GROUP_A",
  "name": "Nguyen Van A",
  "email": "a@example.com"
}
```

---

## 🤔 Redpanda Connect với MongoDB làm được gì?

Redpanda Connect là **data pipeline engine** chạy giữa Redpanda/Kafka và MongoDB. Nó **không phải** một consumer viết tay — nó là một framework pipeline với hàng trăm connector sẵn có.

### Khả năng chính

| Tính năng | Cách dùng |
|-----------|-----------|
| **Read topics** | `input.kafka` với 1 hoặc nhiều topic |
| **Write MongoDB** | `output.mongodb` với insert/update/delete/upsert |
| **Filter message** | `bloblang: deleted()` để bỏ qua |
| **Transform data** | `mapping` processor để rename/add/remove field |
| **Route output** | `switch` output theo bất kỳ điều kiện nào |
| **Validate** | Kiểm tra field bắt buộc trước khi lưu |
| **DLQ** | Gửi message lỗi sang topic khác |
| **Retry** | Tự động retry với exponential backoff |
| **Batch** | Gom nhiều message để ghi hiệu quả |
| **Scale** | Nhiều instance dùng cùng `consumer_group` |
| **CRUD** | Toàn bộ CRUD với `operation` config |

### So sánh với Kafka Connect

| | Kafka Connect | Redpanda Connect |
|--|---------------|------------------|
| Filter/Transform | Cần SMT (Single Message Transform) | Bloblang (mạnh hơn nhiều) |
| CRUD routing | Không có sẵn | Switch output đơn giản |
| DLQ | Cần config phức tạp | `fallback` output |
| Validate | Không có | Bloblang built-in |
| Config | JSON phức tạp | YAML đơn giản |
| Language | Java | Go (nhẹ hơn) |

---

## 🔍 Debugging

```bash
# Xem topic có message chưa
docker exec redpanda rpk topic consume users --num 5

# Xem MongoDB
docker exec -it mongo mongosh
> use app_db
> db.users.find().pretty()
> db.orders.find().pretty()
> db.orders_dlq.find().pretty()   # DLQ messages

# Xem log pipeline
docker compose logs -f connect-master
docker compose logs -f connect-crud

# Restart pipeline sau khi thay đổi YAML
docker compose restart connect-master
```

---

## 📊 Monitoring

```bash
# Xem tất cả consumer groups
docker exec redpanda rpk group list

# Xem lag của consumer group
docker exec redpanda rpk group describe connect-master-group

# Xem stats connect
docker compose ps
```

---

## 🔄 Scale Pipeline

```bash
# Chạy 3 instance connect-master cùng consumer_group
docker compose up -d --scale connect-master=3

# 3 instance sẽ tự động chia partition để xử lý
# Không có message nào bị xử lý 2 lần (consumer_group đảm bảo)
```

---

*Được xây dựng với: Node.js + KafkaJS + Redpanda Connect + MongoDB*
