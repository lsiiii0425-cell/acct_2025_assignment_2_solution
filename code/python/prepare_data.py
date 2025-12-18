import pandas as pd
import numpy as np
import yaml
import os

def main():
    # 1. 读取配置
    with open("config/global_cfg.yaml", "r") as f:
        config = yaml.safe_load(f)

    input_path = config['wrds_raw_data']
    output_path = config['processed_data']

    print("🚀 开始数据清洗 (含单位修正)...")
    if not os.path.exists(input_path):
        print(f"❌ 找不到文件 {input_path}")
        return

    df = pd.read_parquet(input_path)
    df.columns = df.columns.str.lower()
    
    print(f"📄 原始数据读取成功，共 {len(df)} 行")

    # =======================================================
    # 【核心修复】单位换算
    # =======================================================
    # 你的 Market Cap 是“元”为单位，但 Total Assets 和 Common Equity 是“百万”为单位
    # 所以我们需要把会计数据统一乘以 1,000,000
    
    print("🔄 正在执行单位对齐：将 资产/权益/净利润 乘以 1,000,000 ...")
    
    df['total_assets'] = df['total_assets'] * 1_000_000
    df['common_equity'] = df['common_equity'] * 1_000_000
    df['net_income'] = df['net_income'] * 1_000_000

    # =======================================================
    # 常规清洗
    # =======================================================
    
    # 1. 剔除负值 (权益和资产必须为正)
    df_clean = df[
        (df['common_equity'] > 0) & 
        (df['total_assets'] > 0)
    ].copy()

    # 2. 计算指标
    # ROA = 净利润 / 总资产
    df_clean['roa'] = df_clean['net_income'] / df_clean['total_assets']
    
    # P/B = 市值 / 股东权益
    # (现在分子分母都是“元”了，除出来就是正常的倍数了，比如 1.5)
    df_clean['pb'] = df_clean['market_cap'] / df_clean['common_equity']
    
    # 3. 处理无效值 (Inf)
    df_clean.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_final = df_clean.dropna(subset=['roa', 'pb'])

    # 4. 剔除极端值
    # 正常的 P/B 一般在 0.5 ~ 20 之间，我们放宽到 0 ~ 50
    # 正常的 ROA 一般在 -50% ~ +50%
    df_final = df_final[
        (df_final['pb'] > 0) & (df_final['pb'] < 50) & 
        (df_final['roa'] > -1) & (df_final['roa'] < 1)
    ]

    # =======================================================
    # 保存结果
    # =======================================================
    if df_final.empty:
        print("❌ 依然为空！请检查数据。")
    else:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_final.to_parquet(output_path)
        print("-" * 30)
        print(f"✅ 清洗完成！最终有效样本: {len(df_final)} 行")
        print(f"   (P/B 均值: {df_final['pb'].mean():.2f})")
        print(f"   (ROA 均值: {df_final['roa'].mean():.2%})")
        print(f"数据已保存至: {output_path}")
        print("-" * 30)
        print("🚀 既然清洗成功了，立刻运行: python code/python/do_analysis.py")

if __name__ == "__main__":
    main()