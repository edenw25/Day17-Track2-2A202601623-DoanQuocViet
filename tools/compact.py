#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


ROW_GROUP_SIZE = 8_192


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    n_before = con.execute(
        f"select count(*) from read_parquet('{SRC.as_posix()}/*.parquet')"
    ).fetchone()[0]

    # Ba quyết định:
    #
    #   partition_by (event_date)
    #       Dashboard lọc theo NGÀY và theo KHÁCH HÀNG. Ngày chỉ có 14 giá trị
    #       -> 14 thư mục, mỗi thư mục một file lớn. Engine đọc tên thư mục là
    #       biết bỏ qua 13/14 dữ liệu TRƯỚC khi mở file.
    #       Không partition theo customer_name: 650 giá trị -> 650×14 = 9.100
    #       thư mục, tức là tái tạo lại đúng small-file problem đang phải sửa.
    #
    #   order by event_date, customer_name, event_time
    #       Trong mỗi file, hàng của cùng một khách hàng nằm liền nhau, nên
    #       thống kê min/max của từng row group hẹp lại và Parquet reader bỏ
    #       qua được phần lớn row group khi lọc customer_name.
    #
    #   row_group_size 8.192
    #       Mặc định 122.880 > số hàng của cả một ngày (~9.300), nên cả ngày
    #       rơi vào MỘT row group: min/max phủ toàn bộ 650 khách hàng và mất
    #       hết tác dụng lọc. 8.192 chia mỗi ngày thành vài row group, mỗi
    #       group chỉ chứa một dải customer_name hẹp.
    con.execute(f"""
        copy (
            select *
            from read_parquet('{SRC.as_posix()}/*.parquet')
            order by event_date, customer_name, event_time
        ) to '{DST.as_posix()}' (
            format          parquet,
            partition_by    (event_date),
            overwrite_or_ignore,
            row_group_size  {ROW_GROUP_SIZE}
        )
    """)

    n_after = con.execute(
        f"select count(*) from read_parquet('{DST.as_posix()}/**/*.parquet')"
    ).fetchone()[0]
    n_files = len(list(DST.rglob("*.parquet")))
    con.close()

    assert n_before == n_after, f"mất hàng: {n_before:,} -> {n_after:,}"

    print(f"  đích  : {DST}  ({n_files:,} file, partition theo event_date)")
    print(f"  hàng  : {n_before:,} -> {n_after:,}  (không mất hàng)")
    print("\n  xong. Kiểm tra lại bằng:  python tools/explain.py\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
