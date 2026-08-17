-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Ba tháng trước truy vấn này chạy 2 giây. Bây giờ 38 giây.
-- Không ai sửa dòng nào trong file này.
--
-- Bạn ĐƯỢC PHÉP viết lại truy vấn, miễn là kết quả trả về không đổi
-- (tools/explain.py kiểm tra điều đó bằng hash của kết quả).

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
-- Đổi hai thứ, ngữ nghĩa giữ nguyên:
--   1. trỏ vào dataset đã compact (partition theo event_date, xem
--      tools/compact.py) và bật hive_partitioning để engine đọc được giá trị
--      partition từ đường dẫn;
--   2. viết lại điều kiện ngày thành dạng sargable — cột đứng một mình một
--      vế. `strftime(event_time, ...) = '...'` bọc cột trong function call nên
--      engine không đối chiếu được với tên thư mục partition, cũng không dùng
--      được thống kê min/max của row group.
from read_parquet(
        'data/gold_events_v2/**/*.parquet',
        hive_partitioning = true,
        hive_types        = {'event_date': DATE}
     )
where customer_name = 'ACME'
  and event_date = DATE '2026-08-09'
group by 1
