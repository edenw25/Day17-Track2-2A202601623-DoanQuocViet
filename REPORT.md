# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đoàn Quốc Việt  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 30.2s
  run 2/3 … 22.8s
  run 3/3 … 22.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (+ cả hai bài mở rộng A và B).

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau mỗi lượt `make pipeline`, `gold_training_set` phình thêm; không có lỗi nào được ném ra. Chạy lại một ngày bằng Clear Task làm số hàng tăng chứ không ghi đè. |
| **Nguyên nhân** | Model `incremental` chỉ khai báo `materialized`, **không có `unique_key`** nên dbt không có khoá để so khớp và sinh ra câu `INSERT INTO … SELECT` thuần. Với `INSERT`, chạy lại cùng một partition ngày là **ghi thêm** chứ không **thay thế**. Nguồn CDC còn có `op='u'`: một ticket tạo ngày D1 rồi sửa ngày D2 đi qua mệnh đề `WHERE _ingested_at` ở hai partition ngày khác nhau, nên ngay trong **một** lượt chạy nó đã được ghi hai lần — đó là 1.310 ticket bị lặp ở lượt đầu tiên. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. Grain là **entity** (1 hàng / 1 ticket) nên khoá tự nhiên là `ticket_id`; `append` cộng dồn, còn `delete+insert` theo partition ngày cũng không xoá được bản cũ vì bản cũ nằm ở partition ngày khác. Chỉ `merge` theo khoá mới bảo đảm lần ghi sau thay thế lần trước. Mệnh đề `WHERE` theo `run_date` giữ nguyên (nó phục vụ backfill, không phải lỗi).<br>`dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. Hai tham số này chỉ **giảm tần suất kích hoạt** (không tự chạy bù hàng loạt, không cho hai run ghi song song vào cùng một bảng) — chúng **không phải root cause**. |
| **Bằng chứng** | trước: 13.790 hàng sau lượt 1 (1.310 ticket lặp) và tăng thêm ở mọi lượt sau · sau: **12.480** hàng, checksum 3 lượt giống hệt `8dd7c98653 / 8dd7c98653 / 8dd7c98653`, dòng "1 hàng / 1 ticket" ✓ |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` có 8.645 / 9.100 hàng (thiếu 455 ≈ 5%). Chỉ thiếu ở các ngày đã chạy xong từ lâu; ngày mới nhất luôn đủ. |
| **P99 độ trễ đo được** | **2,73 ngày** (p50 = 0,13 · p95 = 1,81 · max = 2,94 · 5,05% bản ghi tới kho muộn hơn 1 ngày) |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 = 2,73 ngày, đủ phủ cả `max` = 2,94 ngày quan sát được trong 14 ngày dữ liệu. |
| **Nguyên nhân** | Điều kiện lọc `where event_date > (select max(event_date) from {{ this }})` dùng **thời điểm sự kiện xảy ra** làm mốc tiến độ, trong khi pipeline lại được kích hoạt theo **thời điểm dữ liệu tới kho**. Con trỏ `max(event_date)` chỉ tiến lên, nên mọi bản ghi tới muộn (event_date nhỏ hơn mốc đã đạt) bị bỏ qua **vĩnh viễn** — không lượt chạy nào về sau nhặt lại chúng. Ví dụ: event xảy ra 08-12, tới kho 08-15; hôm 08-15 target đã có `max(event_date) = 08-14` nên `08-12 > 08-14` sai, event bị loại; hôm 08-16 mốc còn cao hơn nữa, càng không lọt. Đây là lỗi mất dữ liệu im lặng: bảng vẫn ổn định, dbt test vẫn xanh. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi điều kiện thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`, đồng thời thêm `unique_key = ['event_date', 'customer_id']` + `incremental_strategy = 'merge'`. Bắt buộc phải có vế thứ hai: window rộng khiến cùng một cặp (ngày, khách hàng) được tính lại ở nhiều lượt, nếu chỉ `insert` thì kết quả cộng dồn — tức tái tạo đúng lỗi của nhiệm vụ 1 trên một bảng khác. Grain gồm hai cột nên `unique_key` là một list. |
| **Bằng chứng** | trước: 8.645 hàng · sau: **9.100** hàng (14 ngày × 650 khách hàng), checksum 3 lượt `3db448685c` không đổi |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` là một quan sát đơn lẻ: nó là giá trị kém ổn định nhất của phân bố, chỉ
> cần một bản ghi cá biệt kẹt trong hàng đợi hai tuần là window phải rộng hai
> tuần. P99 là ngưỡng có kiểm soát: nó nói "99% dữ liệu về muộn nằm trong ngần
> này", phần 1% còn lại xử lý bằng backfill chủ động chứ không bằng cách bắt
> **mọi lượt chạy hằng ngày** phải trả giá. Và cái giá đó là giá thường trực,
> không phải trả một lần: mỗi ngày lùi thêm là thêm một ngày dữ liệu phải đọc,
> group by và merge lại **ở mọi lượt chạy về sau** — chi phí tuyến tính theo độ
> rộng window, trong khi lượng dữ liệu thực sự được cứu thì giảm dần theo đuôi
> phân bố. Ở đây P99 (2,73) và max (2,94) gần nhau nên lùi 3 ngày thoả cả hai;
> nếu chúng cách xa nhau thì P99 mới là lựa chọn đúng, kèm cảnh báo (alert) cho
> phần vượt ngưỡng.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 08-10, model phân loại dự đoán kém hẳn nhưng pipeline không dừng, `dbt test` vẫn 9/9 pass. `silver_tickets` có 6.606 hàng `priority` NULL hoặc ngoài miền 1..4; `quarantine_tickets` rỗng. |
| **Nguyên nhân** | Macro `normalize_priority` dùng `try_cast(priority_raw as integer)`, và biểu thức này sai theo **hai hướng ngược nhau**: (a) từ 08-10 nguồn đổi cách biểu diễn sang nhãn chữ (`urgent`/`high`/`medium`/`low`) — `try_cast` biến toàn bộ 7.142 bản ghi hợp lệ này thành NULL, tức vứt bỏ dữ liệu tốt vì một thay đổi *format*; (b) ngược lại nó **chấp nhận** `0`, `5`, `-1` vì chúng đúng là số nguyên, dù contract quy định 1..4. Contract của dbt đang `enforced: false` nên không ai chặn; và ngay cả khi bật, contract chỉ ràng buộc **kiểu**, không ràng buộc **miền giá trị** — `priority = 99` vẫn đi lọt vì 99 đúng là integer. Không có test miền giá trị nào tồn tại, nên lỗi trôi thẳng xuống feature của model. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1** — `'1' '2' '3' '4'` (6.846 bản ghi): đúng contract cũ → giữ nguyên.<br>**Nhóm 2** — `'urgent' 'high' 'medium' 'low'` (7.142 bản ghi): **schema evolution**, ý nghĩa không đổi chỉ đổi cách biểu diễn → map về 1/2/3/4 theo tài liệu API.<br>**Nhóm 3** — `'P1' 'P2' 'unknown' '0' '5' '-1' '' NULL` (**312** bản ghi): dữ liệu hỏng thật → trả NULL và đưa vào quarantine.<br>Tiêu chí phân biệt nhóm 2 với nhóm 3: giá trị đó có mang đúng thông tin của contract cũ chỉ khác cách biểu diễn hay không. |
| **Cách khắc phục** | (a) `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng khối `CASE` xử lý đủ ba nhóm (`try_cast … between 1 and 4` cho nhóm 1, bảng quy đổi nhãn chữ cho nhóm 2, `else null` cho nhóm 3); viết thêm `priority_reject_reason` phân biệt bốn loại lỗi để người trực đọc log là biết phải làm gì.<br>(b) `dbt/models/silver/silver_tickets.sql`: **lọc trước, xếp hạng sau** — CTE `valid` loại các bản ghi CDC mà macro trả NULL, rồi mới `row_number()`. Nếu lọc sau khi xếp hạng thì ticket nào có bản ghi *mới nhất* bị hỏng sẽ biến mất khỏi Silver (12.480 → 12.168); ở đây ta loại **bản ghi** hỏng chứ không loại cả **ticket**, ticket vẫn giữ trạng thái hợp lệ từ lần cập nhật trước.<br>(c) `dbt/models/silver/quarantine_tickets.sql`: `where {{ normalize_priority('priority_raw') }} is null` — dùng đúng macro mà `silver_tickets` dùng nên hai model không thể lệch nhau.<br>(d) `dbt/models/silver/schema.yml`: `contract.enforced: true` và thêm test `not_null` + `accepted_values [1,2,3,4]` cho cột `priority` (contract giữ kiểu, test giữ miền giá trị — cần cả hai). |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (đúng `expected/`) · `dbt test` **11/11** pass (bản gốc 9) · `silver_tickets.priority ∈ 1..4, không NULL` ✓ sạch · `gold_training_set` giữ nguyên 12.480 |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Chặn ở Silver, không chặn ở Bronze.** Bronze là bản sao trung thực của
> nguồn — nhiệm vụ của nó là *ghi lại đúng những gì nguồn đã gửi*, kể cả rác.
> Nếu Bronze từ chối bản ghi lỗi thì bằng chứng biến mất: không còn cách nào
> trả lời "nguồn thực sự gửi gì lúc 08-10", không tái hiện được sự cố, không
> backfill lại được sau khi sửa logic, và cũng không đối chiếu được với team
> backend khi tranh luận xem lỗi thuộc về ai. Việc phán xét dữ liệu là của
> tầng Silver, nơi contract được phát biểu tường minh và nơi bản ghi bị loại
> vẫn còn nguyên vẹn ở tầng dưới để điều tra.
>
> **Không dừng pipeline vì tỷ lệ lỗi quyết định phản ứng đúng.** 312 bản ghi
> hỏng trên 14.300 bản ghi CDC (2,2%) không có quyền chặn 12.480 ticket,
> 130.683 event và 31.200 chunk hoàn toàn bình thường đến tay người dùng —
> dừng DAG ở đây là tự gây ra một sự cố lớn hơn sự cố mình đang xử lý, và làm
> hỏng SLA của cả ba downstream (RAG index, classifier, routing agent). Cách
> đúng là **dead-letter**: pipeline chạy tiếp, bản ghi lỗi rơi vào
> `quarantine_tickets` — một hàng đợi có tên, có `reject_reason`, có người
> trực xử lý và có thể replay sau khi sửa. Ngưỡng dừng nên đặt theo *tỷ lệ*
> (ví dụ >5% bản ghi bị quarantine thì mới fail), vì lúc đó vấn đề không còn
> là vài bản ghi rác mà là nguồn đã đổi contract ở quy mô lớn.

---

## 4 · *(mở rộng)* Bài trong EXTRA.md — làm cả A và B

### Bài A — Query dashboard chậm

| | |
|---|---|
| **Bài đã làm** | A |
| **Nguyên nhân** | Hai lỗi cộng dồn. (1) **Small-file problem**: `data/gold_events/` là 5.000 file Parquet tí hon chứa tổng cộng 130.683 hàng; DuckDB đọc Parquet theo lô và làm tròn lên theo từng file, nên một file vài chục hàng vẫn tốn công quét ~1.000 hàng → 5.000.000 rows scanned cho một tập 130 nghìn hàng. (2) **Layout không mang thông tin filter**: dataset không partition, tên file không chứa ngày, nên engine buộc phải mở toàn bộ 5.000 file mới biết file nào có ích; thêm nữa điều kiện `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` bọc cột trong function call (không sargable) nên không đối chiếu được với tên thư mục partition lẫn thống kê min/max của row group. |
| **Cách khắc phục** | `tools/compact.py`: `COPY … TO 'data/gold_events_v2' (format parquet, partition_by (event_date), overwrite_or_ignore, row_group_size 8192)` với `order by event_date, customer_name, event_time`. Ba quyết định: partition theo `event_date` vì nó chỉ có 14 giá trị (partition theo `customer_name` sẽ tạo 9.100 thư mục — tái tạo đúng small-file problem); sắp xếp theo `customer_name` trong từng ngày để min/max của mỗi row group hẹp lại và lọc được theo khách hàng; `row_group_size 8192` thay cho mặc định 122.880, vì mặc định nhốt cả một ngày (~9.300 hàng) vào **một** row group khiến min/max phủ toàn bộ 650 khách hàng và mất hết tác dụng lọc. `queries/dashboard.sql`: trỏ vào dataset mới, bật `hive_partitioning`, viết lại filter thành `event_date = DATE '2026-08-09'` (cột đứng một mình một vế). |
| **Bằng chứng** | rows scanned **5.000.000 → 9.324** (giảm **536,3×**, yêu cầu ≥ 10×) · files **5.000 → 14** · rows on disk 130.683 không đổi · result hash `4379e4c5d9f3` **không đổi** |

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Bài đã làm** | B |
| **Nguyên nhân** | `consume()` **commit offset trước khi ghi dữ liệu** — đó là ngữ nghĩa **at-most-once**. Khi tiến trình bị `kill -9` ngay sau `commit()`, offset đã dịch qua lô hiện tại nhưng lô đó chưa hề chạm tới kho; lần khởi động lại đọc tiếp từ offset mới nên lô đó **mất vĩnh viễn**, không có cơ chế nào phát hiện hay phục hồi. |
| **Cách khắc phục** | `ingest/consumer.py`: đảo thành **ghi trước — commit sau** (at-least-once), và làm phép ghi **idempotent** để chịu được việc phát lại: thêm `primary key` cho `event_id` trong `DDL`, đổi `INSERT` thuần thành `insert … on conflict (event_id) do update set …`. Thiếu một trong hai là chưa đủ: chỉ đảo thứ tự thì crash sinh ra bản ghi trùng thay vì bản ghi mất. Chọn `DO UPDATE` chứ không `DO NOTHING` vì nếu message được phát lại với nội dung **đã đổi** ở nguồn, `DO NOTHING` giữ bản cũ và kho lệch vĩnh viễn, còn `DO UPDATE` luôn hội tụ về bản mới nhất. Exactly-once không tồn tại ở tầng giao vận; thứ chọn được là at-least-once **cộng** một phép ghi idempotent. |
| **Bằng chứng** | `make crash-test`: A = 20.000 hàng / 20.000 event_id · B chết ở lô 7, offset commit = 3.000 · C khởi động lại ghi 17.000 message → 20.000 hàng / 20.000 event_id. Không mất ✓ · không trùng ✓ · C == A ✓ → **BÀI MỞ RỘNG B: ĐẠT** |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline hai lần liên tiếp và so checksum. Mọi model `incremental` phải trả lời được câu "khoá của grain này là gì" — không có `unique_key` thì mặc định là `INSERT`, và một job chỉ đúng khi chạy đúng một lần thì không phải job đúng. |
| 2 | Đối chiếu **thời điểm sự kiện xảy ra** với **thời điểm dữ liệu tới kho**: đo phân bố độ trễ, lấy P99 làm căn cứ cho lookback. Con trỏ tiến độ tuyệt đối không được đặt trên trường thời gian của *sự kiện*, vì nó chỉ tiến lên và bỏ lại dữ liệu về muộn một cách im lặng. |
| 3 | Xem contract đã bật chưa và cột quan trọng có test miền giá trị chưa. Phân bố giá trị của các cột khoá là thứ nên nhìn đầu tiên — thay đổi ở nguồn thường không làm pipeline đỏ, nó chỉ làm chất lượng model tụt xuống, và cần phân biệt được đâu là *schema evolution* (map lại) với đâu là *dữ liệu hỏng* (quarantine). |
