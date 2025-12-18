import pandas as pd
import yaml
import os
import matplotlib.pyplot as plt
import seaborn as sns

# 1. 读取配置
with open("config/global_cfg.yaml", "r") as f:
    config = yaml.safe_load(f)

input_path = config['processed_data']

# 2. 读取清洗后的数据
if not os.path.exists(input_path):
    print("❌ 错误：找不到清洗后的数据文件！请先运行 prepare_data.py")
    exit()

df = pd.read_parquet(input_path)

# 3. 打印统计摘要 (检查数据是否正常)
print("📊 数据统计摘要:")
print(df[['roa', 'pb']].describe())

# 4. 统计分析
if len(df) < 2:
    print("❌ 样本量太少，无法计算相关性！")
else:
    corr = df['roa'].corr(df['pb'])
    print("-" * 30)
    print(f"✅ 【最终结论】ROA 与 P/B 的相关系数为: {corr:.4f}")
    print("-" * 30)

    # 5. 画图
    plt.figure(figsize=(10, 6))
    sns.regplot(x='roa', y='pb', data=df, 
                scatter_kws={'alpha':0.6}, line_kws={"color": "red"})

    plt.title(f'Relationship between ROA and P/B (German Prime Standard 2023)\nCorrelation: {corr:.2f}, N={len(df)}')
    plt.xlabel('Return on Assets (ROA)')
    plt.ylabel('Price-to-Book Ratio (P/B)')
    plt.grid(True, linestyle='--', alpha=0.5)

    # 保存图片
    output_dir = "output/figures"
    os.makedirs(output_dir, exist_ok=True)
    save_path = os.path.join(output_dir, "roa_pb_scatter.png")

    plt.savefig(save_path)
    print(f"🖼️ 图表已保存至: {save_path}")

    import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# 读取数据
df = pd.read_parquet('data/generated/analysis_data.parquet')

# 只取盈利公司
df_profit = df[df['roa'] > 0]
corr_profit = df_profit['roa'].corr(df_profit['pb'])

# 画图
plt.figure(figsize=(10, 6))
sns.regplot(x='roa', y='pb', data=df_profit, 
            scatter_kws={'alpha':0.6}, line_kws={"color": "green"})

plt.title(f'Positive Relationship: ROA vs P/B (Profitable Firms Only)\nCorrelation: {corr_profit:.2f}, N={len(df_profit)}')
plt.xlabel('Return on Assets (ROA)')
plt.ylabel('Price-to-Book Ratio (P/B)')
plt.grid(True, linestyle='--', alpha=0.5)

# 保存
plt.savefig('output/figures/roa_pb_profit_only.png')
print(f"✅ 新图已生成: output/figures/roa_pb_profit_only.png")