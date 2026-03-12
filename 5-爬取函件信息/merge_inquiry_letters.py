#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""合并上交所和深交所问询函数据，并去除深交所数据中的引号"""

import csv
from pathlib import Path

BASE = Path(__file__).parent
OUT_DIR = BASE / "output"

# 上交所
sse_file = OUT_DIR / "inquiry_letters.csv"
# 深交所
szse_file = OUT_DIR / "szse_inquiry_letters.csv"
# 合并输出
merged_file = OUT_DIR / "inquiry_letters_merged.csv"

rows_out = []

# 读取上交所（无引号）
with open(sse_file, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)
    rows_out.append(header)
    sse_rows = list(reader)
    rows_out.extend(sse_rows)
    n_sse = len(sse_rows)

# 读取深交所（csv.reader 会正确解析并去掉字段外的引号）
n_szse = 0
with open(szse_file, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)  # 跳过表头
    for row in reader:
        # 确保每列都是去引号后的纯文本
        row_clean = [cell.strip('"').strip() for cell in row]
        rows_out.append(row_clean)
        n_szse += 1

# 写出合并文件（不强制加引号，与上交所风格一致）
with open(merged_file, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    writer.writerows(rows_out)
print(f"已合并: 上交所 {n_sse} 条, 深交所 {n_szse} 条")
print(f"合计: {len(rows_out)-1} 条 (不含表头)")
print(f"输出: {merged_file}")
