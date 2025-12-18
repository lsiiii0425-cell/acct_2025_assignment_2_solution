import pandas as pd
import yaml
import os

# 1. 读取清洗后的数据
with open("config/global_cfg.yaml", "r") as f:
    config = yaml.safe_load(f)

file_path = config['processed_data'] # 通常是 data/generated/analysis_data.parquet

if not os.path.exists(file_path):
    print("❌ 找不到数据文件！")
else:
    df = pd.read_parquet(file_path)
    
    print("-" * 30)
    print(f"📊 数据总行数: {len(df)}")
    print("-" * 30)
    
    # 检查每一列有多少个空值
    print("各列空值数量 (Missing Values):")
    print(df[['net_income', 'total_assets', 'ROA', 'market_cap', 'common_equity', 'PB']].isnull().sum())
    
    print("-" * 30)
    # 看看前几行实际数据长啥样
    print("前 5 行数据概览:")
    print(df[['ISIN', 'net_income', 'total_assets', 'ROA']].head())
    print("-" * 30)

    if df['net_income'].isnull().all():
        print("⚠️ 诊断结果：【净利润】全是空的！这就是问题所在！")
        print("💡 建议：2023 年数据可能不全，请把年份改成 2022 重新跑。")
    elif df['ROA'].isnull().all():
        print("⚠️ 诊断结果：【ROA】计算失败（可能是资产缺失）。")
    else:
        print("✅ 数据看起来没问题，可能是方差为 0？")