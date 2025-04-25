import pandas as pd

excel_path = '../data/公有数据看板-日前(2025-01-01-2025-03-31).xlsx'

# 按照实际列名读取并重命名
df_filtered = pd.read_excel(
    excel_path,
    usecols=['日期', '时点', '(调控后)日前出清价格(元/MWh)', '日前负荷率(%)']
)
df_filtered.columns = ['date', 'time_slot', 'price', 'load_rate']

# 格式化日期为 'YYYY-MM-DD'
df_filtered['date'] = pd.to_datetime(df_filtered['date']).dt.strftime('%Y-%m-%d')

# Price 列保留两位小数
df_filtered['price'] = df_filtered['price'].round(2)


# 保存为 CSV
csv_output_path = "../data/marginal(2025-01-01-2025-03-31).csv"
df_filtered.to_csv(csv_output_path, index=False)
