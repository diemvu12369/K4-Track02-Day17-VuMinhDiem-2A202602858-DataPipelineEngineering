# B2 — Brainstorm: pipeline feature chống gian lận cho ví điện tử / BNPL

**Học viên:** Vũ Minh Điềm — 2A202602858

## 1. Bài toán và ràng buộc thực

Một ví điện tử Việt Nam có tính năng "mua trước trả sau" (BNPL) muốn chấm điểm rủi ro
gian lận cho **mỗi giao dịch** trước khi duyệt. Mô hình (gradient boosting, sau này thêm
một LLM đọc ghi chú CSKH) cần các feature kiểu: số giao dịch của user trong 1 giờ / 24 giờ,
số thiết bị khác nhau đã đăng nhập trong 7 ngày, tỷ lệ hoàn tiền, user có từng khiếu nại
"bị lừa chuyển khoản" hay không.

Người dùng của pipeline:

- **Dịch vụ chấm điểm online**: cần feature trong < 50 ms tại thời điểm giao dịch.
- **Nhóm data science**: cần training set lịch sử *không rò rỉ tương lai*.
- **Nhóm vận hành rủi ro / pháp chế**: cần giải thích vì sao một giao dịch bị chặn,
  và phải xoá dữ liệu cá nhân khi khách yêu cầu (Nghị định 13/2023/NĐ-CP).

Vì sao khó: nhãn "gian lận" đến **muộn** (chargeback sau 30–90 ngày), dữ liệu sự kiện đến
muộn khi app mất mạng (giống u05 trong lab), schema giao dịch thay đổi theo đối tác
ngân hàng, và một lỗi point-in-time làm mô hình offline đẹp nhưng online vô dụng.

## 2. Sơ đồ kiến trúc

```
Postgres core-banking ─ Debezium CDC ─┐
App events (Kafka) ───────────────────┼─▶ Bronze (Parquet/Iceberg, bất biến, theo ngày ingest)
Chargeback file đối tác (SFTP, T+n) ──┘          │
                                                 ▼
                      Silver: MERGE theo khoá + LSN guard, PII tách sang vault
                                 │                         │
             streaming (Flink)   │                         │ batch hằng đêm (dbt + DuckDB/Spark)
                                 ▼                         ▼
               Online store (Redis)              Offline store (feature theo event_time,
               feature cửa sổ 1h/24h               valid_from/valid_to — SCD2)
                                 │                         │
                                 ▼                         ▼
                     Scoring service ◀── cùng định nghĩa feature ──▶ Training set
                       (log feature đã dùng)                (ASOF join tại thời điểm giao dịch)
                                 │
                                 └──▶ Bronze "scoring_log" ──▶ flywheel: nhãn chargeback → eval
```

## 3. Các câu hỏi then chốt và quyết định

### Q2 — Batch hay streaming?

**Quyết định:** kiến trúc lai, *không* Lambda đầy đủ. Chỉ 5–6 feature cửa sổ ngắn
(velocity 1h/24h) chạy streaming; mọi feature còn lại (7–90 ngày, tỷ lệ hoàn tiền,
lịch sử khiếu nại) chạy batch hằng đêm.

**Đánh đổi:** streaming toàn bộ (Kappa) vs lai. Kappa cho một code path duy nhất nhưng
chi phí vận hành Flink cho các feature 90 ngày rất cao mà độ tươi tăng thêm gần như
không giúp gì — tỷ lệ hoàn tiền 90 ngày không đổi đáng kể trong một giờ. Chọn lai vì 80%
giá trị chống gian lận nằm ở velocity ngắn hạn, và chỉ phần đó đáng trả giá streaming.

### Q5 — Train/serve parity và point-in-time

**Quyết định:** (a) định nghĩa feature viết **một lần** (SQL/dbt) và sinh cả job streaming
lẫn batch; (b) training set dựng bằng **ASOF join** feature tại `transaction_time`, không
phải feature "hiện tại"; (c) dịch vụ scoring **log lại vector feature đã dùng** vào Bronze.

**Đánh đổi:** log feature lúc serve tốn lưu trữ (~1 KB/giao dịch) vs tính lại feature lịch sử
từ Silver. Tính lại thì rẻ hơn nhưng không bao giờ khớp 100% với cái mô hình thực sự đã
thấy (event đến muộn, bug đã sửa). Chọn log vì đây là nguồn sự thật cho cả training
lẫn giải trình với pháp chế — giống `priority_at_creation` trong lab: dùng giá trị tại thời
điểm quyết định, không phải giá trị mới nhất.

### Q1 + Q8 — Dữ liệu đến muộn và idempotency

**Quyết định:** đo lateness từ Bronze (`_ingested_at − event_time`) như lab; feature batch
recompute với `lookback = ceil(P99)` (dự kiến 2–3 ngày cho app event). Riêng **nhãn**
chargeback thì không dùng lookback mà dùng **snapshot training có version**: nhãn của
giao dịch tháng 8 chỉ được coi là "chín" sau 90 ngày, và snapshot `v2026-11-30` được dựng
lại từ Bronze as-of ngày đó, không bao giờ sửa.

**Đánh đổi:** lookback 90 ngày cho cả nhãn (đơn giản, một cơ chế) vs tách label maturity.
Lookback 90 ngày nghĩa là mỗi đêm recompute 90 partition — chi phí gấp ~30 lần, và training
set thay đổi âm thầm, làm hỏng khả năng so sánh giữa hai lần train. Chọn tách vì eval phải
tái lập được.

### Q4 — Hợp đồng dữ liệu và quarantine

**Quyết định:** Pydantic/contract ở ranh giới Bronze→Silver; bản ghi sai đi vào
`quarantine_*` kèm lý do, run **không dừng**. Cảnh báo khi tỷ lệ quarantine của một nguồn
vượt 3× trung vị 7 ngày (không phải ngưỡng tuyệt đối, vì đối tác lớn có volume dao động).

**Đánh đổi:** fail-fast vs quarantine. Fail-fast an toàn hơn về chất lượng, nhưng một đối
tác gửi file chargeback lỗi format sẽ chặn toàn bộ feature của mọi user — tức là chặn
luôn việc chấm điểm. Chọn quarantine + cảnh báo, *ngoại trừ* khi quarantine > 20% batch
thì dừng, vì lúc đó gần như chắc chắn là schema drift chứ không phải vài dòng bẩn.

### Q10 — Bối cảnh Việt Nam: PII và quyền xoá

**Quyết định:** PII (họ tên, CCCD, SĐT, số tài khoản) tách sang **vault** với khoá
`user_key` giả danh; Silver/Gold chỉ giữ `user_key`. Yêu cầu xoá = **xoá khoá giải mã
trong vault** (crypto-shredding) + tombstone ở Silver như lab. Ghi chú CSKH tiếng Việt qua
một bộ NER tiếng Việt (tên người có dấu / không dấu) trước khi rời Bronze, đo bằng
precision/recall trên 500 câu gán tay mỗi quý.

**Đánh đổi:** crypto-shredding vs viết lại toàn bộ snapshot cũ. Viết lại snapshot phá
tính bất biến và tốn kém; crypto-shredding giữ snapshot tái lập được (về đặc trưng số)
mà dữ liệu nhận diện không còn đọc được. Cái giá: mọi truy vấn cần PII phải đi qua vault.

## 4. Phương án bị loại

**Loại: dùng Spark + Delta Lake ngay từ đầu cho batch.** Volume giai đoạn đầu khoảng
2–5 triệu giao dịch/ngày, vài GB/ngày — DuckDB/dbt trên một máy đủ chạy feature batch
trong vài phút, giống lab. Spark thêm cluster, chi phí cố định và độ phức tạp vận hành
cho một đội 3 người mà không mua được gì. Sẽ xem lại khi một job batch vượt 30 phút
hoặc dữ liệu một ngày vượt RAM một máy lớn; định nghĩa feature viết bằng SQL/dbt nên
việc chuyển adapter là có đường lui.

**Loại: lấy nhãn gian lận từ "giao dịch bị chặn" của mô hình cũ.** Đây là vòng lặp tự
đầu độc: mô hình chỉ học lại những gì nó đã chặn, và các giao dịch bị chặn không bao giờ
có chargeback để xác nhận. Nhãn chỉ lấy từ chargeback/khiếu nại đã xác minh, cộng một
tỷ lệ nhỏ giao dịch rủi ro được *cho qua có chủ đích* để có nhãn phản thực tế.

## 5. Liên hệ với lab

Bốn quyết định ở trên là bản phóng to của ba lỗi trong lab: MERGE theo khoá + LSN guard
(Silver), lookback đo từ Bronze (late data), tombstone và "xoá phải lan" (CDC delete),
và snapshot bất biến có version (training set). Điểm khác chính là quy mô thời gian:
nhãn đến muộn 90 ngày buộc phải tách *data lateness* (lookback) khỏi *label maturity*
(snapshot), điều mà lab chỉ gợi ý qua `got_negative_feedback`.
