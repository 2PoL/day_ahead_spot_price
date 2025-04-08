import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta

db_username = 'tupo'
db_password = 'npg_mbhaH6B3jdzy'
db_host = 'ep-bold-king-a52tlxdq-pooler.us-east-2.aws.neon.tech'
db_port = '5432'  # 默认为5432
db_name = 'day_ahead_spot'
target_date = '2025-04-09'


def find_similar_clearing_prices(
        df: pd.DataFrame,
        target_date: str,
        tolerance: float = 1.0,
        lookback_days: int = 5
) -> pd.DataFrame:
    """
    对目标日每个 time_slot，查找近 lookback_days 天中所有负荷率相似的记录（不限制时段），
    返回这些记录的 time_slot、清算价格、目标负荷率等信息。

    参数：
        df: 含历史数据的 DataFrame
        target_date: 字符串形式的日期，如 '2025-04-01'
        tolerance: 负荷率相似的误差范围
        lookback_days: 回溯天数

    返回：
        DataFrame，包含：
            - target_time_slot
            - target_load_rate
            - matched_time_slot
            - matched_date
            - matched_load_rate
            - matched_price
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    target_date = pd.to_datetime(target_date)

    target_df = df[df['date'] == target_date].copy()
    date_start = target_date - timedelta(days=lookback_days)
    recent_df = df[(df['date'] < target_date) & (df['date'] >= date_start)]

    results = []

    for _, row in target_df.iterrows():
        target_time_slot = row['time_slot']
        target_load_rate = row['day_ahead_load_rate_percent']

        # 匹配所有 time_slot 的相似负荷率记录
        similar_rows = recent_df[
            (np.abs(recent_df['day_ahead_load_rate_percent'] - target_load_rate) <= tolerance)
        ]

        for _, sim_row in similar_rows.iterrows():
            results.append({
                '预测日时点': target_time_slot,
                '负荷率': target_load_rate,
                'matched_time_slot': sim_row['time_slot'],
                'matched_date': sim_row['date'],
                'matched_load_rate': sim_row['day_ahead_load_rate_percent'],
                'matched_price': sim_row['day_ahead_clearing_price']
            })

    return pd.DataFrame(results)

# 获取匹配结果
engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
query = "SELECT * FROM day_ahead_marginal_data;"
df = pd.read_sql_query(query, engine)

similar_df = find_similar_clearing_prices(df, target_date, tolerance=1.0, lookback_days=30)

print(similar_df.head())
# 设置路径
output_dir = "../output/similarDateData"
os.makedirs(output_dir, exist_ok=True)  # 如果目录不存在就创建

# 生成完整文件路径
filename = f"{target_date} 相似匹配结果.xlsx"
file_path = os.path.join(output_dir, filename)

# 导出
similar_df.to_excel(file_path, index=False)
print(f"✅ 文件已保存到：{file_path}")
engine.dispose()
