# -*- coding: utf-8 -*-
import pandas as pd
import os

def read_excel_with_second_row_as_header(file_path):
    """
    读取 Excel 文件：
      - 第1行：科目代码（忽略）
      - 第2行：中文列名（作为 header）
      - 第3行起：数据
    """
    # 跳过第0行（即第一行），用第1行（即第二行）作为 header
    df = pd.read_excel(file_path, header=1, sheet_name=0)
    return df

# === 读取三张表 ===
print("正在读取资产负债表（使用第二行作为列名）...")
df_bas = read_excel_with_second_row_as_header("FS_Combas.xlsx")

print("正在读取利润表...")
df_ins = read_excel_with_second_row_as_header("FS_Comins.xlsx")

print("正在读取现金流量表...")
df_scf = read_excel_with_second_row_as_header("FS_Comscfd.xlsx")

# 检查是否成功读取中文列名
print("\n资产负债表前5个列名:", df_bas.columns[:5].tolist())

# === 筛选合并报表 ===
# 注意：现在列名是中文了！所以要用中文筛选
df_bas = df_bas[df_bas['报表类型'] == 'A'].copy()
df_ins = df_ins[df_ins['报表类型'] == 'A'].copy()
df_scf = df_scf[df_scf['报表类型'] == 'A'].copy()

print(f"筛选后：资产负债表 {len(df_bas)} 行，利润表 {len(df_ins)} 行，现金流量表 {len(df_scf)} 行")

# === 合并键（全部用中文）===
merge_keys = ['证券代码', '证券简称', '统计截止日期', '报表类型', '是否发生差错更正', '差错更正披露日期']

# 合并
df_merged = pd.merge(df_bas, df_ins, on=merge_keys, how='outer')
df_final = pd.merge(df_merged, df_scf, on=merge_keys, how='outer')

# 列顺序：关键字段在前
other_cols = [col for col in df_final.columns if col not in merge_keys]
df_final = df_final[merge_keys + other_cols]

# 保存
output_file = "FS_Combined_全中文列名.xlsx"
df_final.to_excel(output_file, index=False, engine='openpyxl')
print(f"\n✅ 合并成功！共 {len(df_final)} 行，{len(df_final.columns)} 列")
print(f"输出文件: {os.path.abspath(output_file)}")

# 预览
financial_cols = [col for col in df_final.columns if col not in merge_keys][:10]
print("\n部分财务科目列名示例:")
for i, col in enumerate(financial_cols, 1):
    print(f"{i:2d}. {col}")