import numpy as np
import pandas as pd
import pwlf
import pickle

excel_file = '../data/公有数据看板-日前(2025-01-01-2025-03-31).xlsx'
n_segments = 3

df = pd.read_excel(excel_file)
df['日期'] = pd.to_datetime(df['日期'])

x = df['日前负荷率(%)'].values
y= df['(调控后)日前出清价格(元/MWh)'].values

weights = np.ones_like(x)
weights[x <= 0.6] = 3.0
weights[x >= 0.92] = 3.0

model = pwlf.PiecewiseLinFit(x,y,weights=weights)
breaks = model.fit(n_segments)
with open('../model/pwlf_model.pkl', 'wb') as f:
    pickle.dump(model, f)

print(f"拟合系数: {model.beta}")

