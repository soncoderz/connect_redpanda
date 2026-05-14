#!/usr/bin/env node
// ============================================================
// scripts/test-all.js - Test tất cả 33 use-cases qua API
// Chạy: node scripts/test-all.js
// ============================================================

const BASE = "http://localhost:3000";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function post(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  console.log(`  ✅ POST ${path} →`, data.message || data.error);
  return data;
}

async function put(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  console.log(`  ✅ PUT  ${path} →`, data.message || data.error);
  return data;
}

async function del(path) {
  const res = await fetch(`${BASE}${path}`, { method: "DELETE" });
  const data = await res.json();
  console.log(`  ✅ DEL  ${path} →`, data.message || data.error);
  return data;
}

async function patch(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  console.log(`  ✅ PATCH ${path} →`, data.message || data.error);
  return data;
}

// ─────────────────────────────────────────────────────────────
async function main() {
  console.log("\n🚀 Test Redpanda Connect × MongoDB — 33 Use-Cases\n");

  // Health check
  const health = await fetch(`${BASE}/health`).then((r) => r.json());
  console.log("❤️  Health:", health.status, "\n");

  // ─── Use-case #1, #2: Đọc 1 topic → 1 collection ─────────
  console.log("📌 [#1, #2] Single topic → single collection");
  await post("/api/publish/users", {
    eventType: "add",
    name: "Nguyen Van A",
    email: "a@example.com",
  });
  await sleep(500);

  // ─── Use-case #3: Nhiều topic → nhiều collection ──────────
  console.log("\n📌 [#3, #21, #22, #23] Multi-topic → multi-collection");
  await post("/api/users", { name: "User 1", email: "u1@test.com" });
  await post("/api/orders", { item: "Laptop", amount: 25000000 });
  await post("/api/payments", { method: "MOMO", amount: 500000 });
  await sleep(500);

  // ─── Use-case #4: Nhiều topic → 1 collection ─────────────
  console.log("\n📌 [#4] Multi-topic → 1 collection (all_events)");
  await post("/api/publish/users", { name: "Fan-in user" });
  await post("/api/publish/orders", { item: "Phone" });
  await sleep(500);

  // ─── Use-case #5, #6: Filter theo eventType ───────────────
  console.log("\n📌 [#5, #6] Filter by eventType");
  await post("/api/publish/orders", {
    eventType: "ORDER_CREATED",
    item: "Book",
    amount: 100000,
  });
  await post("/api/publish/orders", {
    eventType: "ORDER_CANCELLED", // Sẽ bị filter bỏ
    item: "TV",
    amount: 5000000,
  });
  await sleep(500);

  // ─── Use-case #7, #26: Filter/Route theo groupId ─────────
  console.log("\n📌 [#7, #26] Filter/Route by groupId");
  await post("/api/publish/events", { groupId: "GROUP_A", data: "Event A" });
  await post("/api/publish/events", { groupId: "GROUP_B", data: "Event B" });
  await sleep(500);

  // ─── Use-case #8, #11, #12: Transform ────────────────────
  console.log("\n📌 [#8, #11, #12] Transform, rename, delete fields");
  await post("/api/publish/raw-events", {
    userName: "Raw User",       // Sẽ được đổi tên → name
    userEmail: "raw@test.com",  // Sẽ được đổi tên → email
    id: "raw-001",
    status: "ACTIVE",
    password: "secret123",      // Sẽ bị xóa
    internalCode: "INT-001",    // Sẽ bị xóa
  });
  await sleep(500);

  // ─── Use-case #9, #10: Thêm savedAt, source ──────────────
  console.log("\n📌 [#9, #10] Thêm savedAt + source (tất cả pipeline)");
  await post("/api/publish/users", { name: "Auto metadata user" });
  await sleep(500);

  // ─── Use-case #13, #14: Validate + lưu hợp lệ ───────────
  console.log("\n📌 [#13, #14] Validate valid message → MongoDB");
  await post("/api/publish/orders", {
    id: "valid-order-001",
    eventType: "ORDER_CREATED",
    amount: 500000,
    item: "Valid item",
  });
  await sleep(500);

  // ─── Use-case #15: Message lỗi bị bỏ qua ─────────────────
  console.log("\n📌 [#15] Invalid message → bỏ qua (không có id/eventType)");
  await post("/api/publish/orders", {
    justSomeData: "no id no eventType",
  });
  await sleep(500);

  // ─── Use-case #16: Message lỗi → DLQ ─────────────────────
  console.log("\n📌 [#16] Invalid message → DLQ topic: orders-dlq");
  await post("/api/publish/orders", {
    // Thiếu id, eventType, amount → sẽ vào DLQ
    randomField: "this will fail validation",
  });
  await sleep(500);

  // ─── Use-case #17: Insert ─────────────────────────────────
  console.log("\n📌 [#17] Insert document mới (eventType: add)");
  const newId = `res-${Date.now()}`;
  await post("/api/publish/crud-events", {
    id: newId,
    eventType: "add",
    name: "New Resource",
    value: 100,
  });
  await sleep(500);

  // ─── Use-case #18: Update ─────────────────────────────────
  console.log("\n📌 [#18] Update document (eventType: update)");
  await put(`/api/users/${newId}`, { name: "Updated User" });
  await sleep(500);

  // ─── Use-case #19: Delete ─────────────────────────────────
  console.log("\n📌 [#19] Delete document (eventType: delete)");
  await del(`/api/users/${newId}`);
  await sleep(500);

  // ─── Use-case #20: Upsert ─────────────────────────────────
  console.log("\n📌 [#20] Upsert document (eventType: upsert)");
  await patch(`/api/users/upsert-id-001`, {
    name: "Upserted User",
    email: "upsert@test.com",
  });
  await sleep(500);

  // ─── Use-case #24: Route theo topic ──────────────────────
  console.log("\n📌 [#24] Route theo topic → collection khác nhau");
  await post("/api/publish/users", { name: "Routed to users" });
  await post("/api/publish/orders", { item: "Routed to orders" });
  await post("/api/publish/payments", { method: "Routed to payments" });
  await sleep(500);

  // ─── Use-case #25: Route theo eventType ──────────────────
  console.log("\n📌 [#25] Route theo eventType → collection khác nhau");
  await post("/api/publish/all-events", {
    eventType: "USER_REGISTERED",
    name: "New User",
  });
  await post("/api/publish/all-events", {
    eventType: "ORDER_CREATED",
    item: "New Order",
  });
  await post("/api/publish/all-events", {
    eventType: "PAYMENT_COMPLETED",
    amount: 999000,
  });
  await sleep(500);

  // ─── Use-case #29: Batch ─────────────────────────────────
  console.log("\n📌 [#29] Batch messages vào high-volume-events");
  const batchPromises = Array.from({ length: 10 }, (_, i) =>
    post("/api/publish/high-volume-events", {
      eventType: "add",
      batchIndex: i,
      data: `Batch item ${i}`,
    })
  );
  await Promise.all(batchPromises);
  await sleep(500);

  console.log("\n✅ Tất cả test cases đã được gửi vào Redpanda!");
  console.log("📊 Kiểm tra MongoDB:\n");
  console.log("  docker exec -it mongo mongosh");
  console.log("  > use app_db");
  console.log("  > db.getCollectionNames()");
  console.log("  > db.users.find().pretty()");
  console.log("  > db.orders.find().pretty()");
  console.log("  > db.orders_dlq.find().pretty()  // DLQ messages\n");
}

main().catch(console.error);
