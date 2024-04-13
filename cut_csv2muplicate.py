# encoding='utf-8

# @Time: 2024-04-12
# @File: %
#!/usr/bin/env
from icecream import ic
import os

import pandas as pd
import math

def split_csv_into_parts(filename, num_parts=10):
    # 确保输出目录存在
    output_dir = "split_csvs"
    os.makedirs(output_dir, exist_ok=True)
    
    # 读取原始CSV文件
    df = pd.read_csv(filename)
    
    # 确定每个部分应该有多少行
    rows_per_part = math.ceil(len(df) / num_parts)
    
    # 分割并保存到新的CSV文件中
    for part in range(num_parts):
        start_row = part * rows_per_part
        end_row = min(start_row + rows_per_part, len(df))
        part_df = df.iloc[start_row:end_row]
        
        # 构建新的文件名
        part_filename = os.path.join(output_dir, f"part_{part + 1}.csv")
        # 将部分数据保存为新的CSV文件
        part_df.to_csv(part_filename, index=False)
        
        print(f"Saved {part_filename}")

# 调用函数示例
split_csv_into_parts("/home/dav/Github/ozon_mangodb/ozon_test_output.csv")
