import pandas as pd
import yaml
import os

# 1. 读取配置
with open("config/global_cfg.yaml", "r") as f:
    config = yaml.safe_load(f)

# 读取刚才 pull_wrds_data.py 下载的原始数据
file_path = config['wrds_raw_data']

if not os.path.exists(file_path):
    print(f"❌ 找不到文件: {file_path}")
else:
    df = pd.read_parquet(file_path)
    print(f"📂 读取原始数据成功，共 {len(df)} 行")
    print("-" * 40)
    print("🔍 各个变量的空值情况 (Missing Values):")
    print("-" * 40)
    
    # 检查每一列的非空数量
    info = df.count().to_frame(name='Non-Null Count')
    info['Missing Count'] = len(df) - info['Non-Null Count']
    info['Missing Ratio'] = (info['Missing Count'] / len(df) * 100).round(1).astype(str) + '%'
    print(info)
    
    print("-" * 40)
    print("💡 诊断分析:")
    if df['net_income'].count() == 0:
        print("🔴 致命问题：【净利润 (net_income)】全空！")
    if df['common_equity'].count() == 0:
        print("🔴 致命问题：【股东权益 (common_equity)】全空！")
    if df['shares_outstanding'].count() == 0:
        print("🔴 致命问题：【流通股数 (shares_outstanding)】全空！(无法计算市值)")
    if df['price_close'].count() == 0:
        print("🔴 致命问题：【收盘价 (price_close)】全空！(无法计算市值)")
    
    # 看看前几行的真实值
    print("\n👀 前 5 行数据预览:")
    print(df[['isin', 'net_income', 'common_equity', 'shares_outstanding', 'price_close']].head())