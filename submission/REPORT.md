# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Vũ Minh Điềm — 2A202602858
**Repo:** https://github.com/diemvu12369/K4-Track02-Day17-VuMinhDiem-2A202602858-DataPipelineEngineering
**Commit bài nộp:** commit mới nhất trên `main` (các fix: `a63294a` Silver, `db81240` late data, `fc8165c` CDC delete; bonus B1 `eb2d805`)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5) — đọc code, định vị lỗi, đề xuất bản sửa, chạy các lệnh kiểm tra và soạn nháp report/bonus; tôi đã đọc lại và giải thích được từng dòng sửa.
**Nguồn tham khảo khác (nếu có):** slide Ngày 17; tài liệu Debezium (định dạng envelope `before`/`after`/`op`).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify 8/18; `silver_tickets has exactly one row per ticket_id` → **24 rows for 12 tickets**; T-91 có 3 hàng (`low/open`, `high/open`, `high/closed/bug`) | `gold_feature_daily reconciles with a full recompute` lệch (`c50b8851affe != 8630e04a61d1`); u05 ngày 08-12 = `(2, 0)` thay vì `(5, 1)`; `LOOKBACK_DAYS=0 < 3` | T-97 vẫn `is_deleted = false`, còn `u06` + subject/body; còn 1 hàng trong snapshot `v2026-08-16` và 2 chunk trong `gold_doc_chunks` |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT` → mỗi batch nối thêm hàng; không có khoá, không có điều kiện "mới hơn mới được ghi" | `LOOKBACK_DAYS = 0` dựa trên giả định "event tới trong vài giây"; run 08-15 chỉ tính lại partition 08-15 nên event 08-12 đến muộn bị bỏ khỏi đúng ngày event time | `staging.py` lấy `ticket_id` từ `after`; với `op='d'` thì `after = null` → `ticket_id` null → bị `WHERE ticket_id IS NOT NULL` lọc mất, delete không bao giờ tới Silver |
| **Cách sửa** | `silver.py`: `MERGE INTO silver_tickets ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT` | đo `main.py --lateness` → P99 = 3.00 ngày; `config.py`: `LOOKBACK_DAYS = 3` | `staging.py`: `coalesce(after.ticket_id, before.ticket_id, key.ticket_id)`; MERGE ở trên ghi đè thành tombstone (PII null) → Gold lọc `is_deleted` / `_op <> 'd'` |
| **Khái niệm trên slide** | Silver — có khoá; MERGE idempotent; LSN guard (batch cũ chạy lại không thắng batch mới) | Data về muộn; event time vs processing time; lookback = ceil(P99) đo từ Bronze | CDC log-based (envelope Debezium); tombstone; "xoá phải lan" Silver → training → RAG |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (P50 0, P95 2.9, max 3, n = 43) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY** (`silver_tickets` 3c15dfd43701, `gold_feature_daily` 8630e04a61d1)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là thực thể thay đổi trạng thái nên cần "một hàng = một khoá" và chỉ LSN mới hơn được ghi đè, còn feature daily là aggregate thuần của Silver theo ngày nên xoá-rồi-tính-lại cửa sổ `[day−3, day]` vừa đơn giản vừa idempotent.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ khoá + `_lsn` để một batch cũ chạy lại (có bản `c/u` của T-97) không "hồi sinh" ticket, và để downstream biết cần xoá; PII đã bị null nên vẫn đáp ứng yêu cầu xoá — cái giá là hàng tồn tại mãi.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: kết quả train/eval tái lập được và so sánh được giữa các phiên bản; dữ liệu đến muộn tạo version mới thay vì âm thầm đổi version cũ.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: vài nghìn dòng chạy trong giây trên một máy, zero-key, chạy được trên laptop/CI; dbt cho sẵn `merge`/`microbatch`/test/contract — Spark chỉ thêm cluster và chi phí mà không mua được gì ở quy mô này.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến vs quyền được xoá.** Quyền xoá thắng: bất biến là thuộc tính kỹ thuật, còn xoá là nghĩa vụ pháp lý (NĐ 13/2023). Tôi sẽ (a) thêm bước "erasure" có ghi log: dựng lại các snapshot chứa T-97 thành version mới (vd `v2026-08-12-r1`) bỏ T-97, đánh dấu version cũ `revoked` và xoá vật lý file của nó, ghi lý do + checksum mới vào bảng audit; (b) về lâu dài tách PII khỏi snapshot (chỉ giữ `user_key` + text đã che) hoặc crypto-shredding theo user để xoá = huỷ khoá. Mô hình đã train trên v08-12..08-14 cần được ghi nhận và lên lịch train lại.
2. **PII ngoài regex (tên người).** Đặt chốt ở ranh giới **Bronze → Silver** (nơi duy nhất văn bản tự do được phép thô), dùng NER tiếng Việt (vd underthesea/PhoBERT-NER) thay tên bằng `<PERSON>`, cộng một chốt thứ hai trước khi vào Gold/RAG chặn chunk còn PII. Đo bằng một tập ~500 câu gán nhãn tay (có dấu/không dấu, viết hoa lẫn lộn): recall là chỉ số chính (bỏ sót = rò rỉ), precision để không phá nội dung; thêm một contract trong verify đếm số entity PERSON còn sót ở Silver phải = 0, và theo dõi tỷ lệ phát hiện theo ngày để bắt drift.

## 5. Output (dán nguyên văn)

Chạy trên Windows bằng Git Bash, lệnh tương đương theo [SUBMISSION.md](../docs/SUBMISSION.md).

```text
$ .\.venv\Scripts\python.exe -m scripts.verify          # make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest                   # make test
..................................                                       [100%]
34 passed in 8.44s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check     # make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness         # make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ main.py --land-only; cd dbt_project; dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17   # make dbt
05:40:28  Running with dbt=1.12.5
05:40:30  Registered adapter: duckdb=1.11.0
05:40:30  Unable to do partial parsing because saved manifest not found. Starting full parse.
05:40:38  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:40:38  
05:40:38  Concurrency: 1 threads (target='dev')
05:40:38  
05:40:38  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:40:38  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.25s]
05:40:38  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:40:38  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.08s]
05:40:38  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:40:39  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.35s]
05:40:39  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:40:39  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.59s]
05:40:39  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:40:40  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.32s]
05:40:40  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:40:40  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.14s]
05:40:40  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:40:40  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.07s]
05:40:40  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:40:40  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.06s]
05:40:40  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:40:40  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.06s]
05:40:40  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:40:40  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.09s]
05:40:40  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:40:40  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.06s]
05:40:40  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:40:40  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.05s]
05:40:40  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:40:40  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.08s]
05:40:40  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:40:40  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.06s]
05:40:40  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:40:40  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.08s]
05:40:40  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:40:40  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:40:40  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.10s]
05:40:40  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:40:41  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.18s]
05:40:41  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:40:41  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.08s]
05:40:41  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:40:41  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.08s]
05:40:41  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:40:41  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.08s]
05:40:41  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:40:41  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.07s]
05:40:41  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:40:41  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.08s]
05:40:41  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.75s]
05:40:41  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:40:41  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.06s]
05:40:41  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:40:41  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.06s]
05:40:41  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:40:41  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.08s]
05:40:41  
05:40:41  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 3.81 seconds (3.81s).
05:40:42  
05:40:42  Completed successfully
05:40:42  
05:40:42  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity          # make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bonus

**B1 — LLM step có cache** (`pipeline/llm_label.py`): khoá cache = sha256(model + prompt_version + input); câu trả lời (kể cả sai schema) được cache nên chạy lại 0 call; `gold_ticket_labels` chỉ nhận nhãn hợp lệ, câu sai schema vào `llm_label_quarantine`; ước lượng token/chi phí trước khi gọi model; mỗi hàng mang `model` + `prompt_version`.

```text
$ .\.venv\Scripts\python.exe -m scripts.bonus_llm       # make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

**B2 — Brainstorm:** [`bonus/DESIGN.md`](../bonus/DESIGN.md) — pipeline feature chống gian lận cho ví điện tử/BNPL.
