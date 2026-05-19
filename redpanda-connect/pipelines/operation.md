

| Trường hợp                           | Operation   | Upsert | Ý nghĩa                                      |
| ------------------------------------ | ----------- | ------ | -------------------------------------------- |
| Tạo mới hoặc cập nhật (nếu có)      | `update-one`| `true` | Nếu chưa có → tạo mới; đã có → cập nhật       |
| Chỉ cập nhật, không tạo mới         | `update-one`| `false`| Chỉ update nếu tìm thấy, không làm gì nếu không có |
| Chỉ tạo mới, không sửa nếu đã có     | `insert-one`| `false`| Đẩy vào DB, nếu trùng ID → lỗi              |
| Thay thế toàn bộ (INSERT nếu chưa có) | `replace-one`| `true` | Ghi đè hoàn toàn, nếu không có → tạo mới     |
| Xóa một bản ghi                      | `delete-one`| `-`    | Xóa bản ghi theo filter_map                    |


  - check: 'this._meta.topic == "crud-events" && this.eventType == "add"'



  | Giá trị     | Đọc từ đâu?                         | Dùng khi nào                         |
| ----------- | ----------------------------------- | ------------------------------------ |
| `earliest`  | Message cũ nhất còn trong topic     | Import/backfill dữ liệu cũ           |
| `latest`    | Message mới sau lúc consumer chạy   | Chỉ xử lý dữ liệu mới                |
| `committed` | Offset đã commit của consumer group | Muốn đọc tiếp chính xác từ offset cũ |
 start_from_oldest:
  true: Đọc lại từ đầu nếu chưa có offset
  false: Chỉ xử lý message mới sau khi consumer start





| Thuộc tính                 | Vai trò                                  |
| ------------------------- | ---------------------------------------- |
| `database`                | Chọn MongoDB database                   |
| `collection`              | Chọn collection (có thể dùng ${})        |
| `operation`               | `insert-one`, `update-one`, `delete-one`, `replace-one`|
| `key_field`               | Dùng làm key trong filter_map           |
| `filter_map`              | Xây logic filter cho upsert/update/delete |
| `document_map`            | Xây logic insert/update/replace         |
| `auto_create_index_on`    | Tự động tạo index khi chưa có           | 
| `auto_create_index_match` | Điều kiện match khi tạo index            |




| Thuộc tính                 | Vai trò                                            |
| ------------------------- | -------------------------------------------------- |
| `key_field`               | Key dùng trong filter_map và auto_create_index_on |
| `filter_map`              | Xây logic filter cho upsert/update/delete         |
| `document_map`            | Xây logic insert/update/replace                   |
| `auto_create_index_on`    | Tự động tạo index theo key_field khi chưa có index |
| `auto_create_index_match` | Điều kiện match khi tạo index                      |