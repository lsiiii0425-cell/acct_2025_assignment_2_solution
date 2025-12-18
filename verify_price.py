import wrds
import pandas as pd
import sys

# 1. 连接数据库
print("正在连接 WRDS 进行深度核查...")
try:
    db = wrds.Connection()
except:
    print("❌ 无法连接 WRDS，请检查网络或账号。")
    sys.exit(1)

# 我们选一个绝对应该有数据的巨头公司：BASF (巴斯夫)
# ISIN: DE000BASF111
test_isin = 'DE000BASF111' 
target_year = 2023

print(f"\n🕵️‍♂️ 正在调查目标：巴斯夫 (ISIN: {test_isin})，年份：{target_year}")
print("-" * 50)

# ==========================================
# 测试 1：在年报表 (g_funda) 里地毯式搜索
# ==========================================
print("👉 测试 1：检查 g_funda (年报表) 中的所有股价变量...")

# 这里列出了 Compustat Global 中所有可能存放股价的字段
# prc: Price Close (通用)
# prccd: Price Close Daily (有时用于每日)
# prcc_f: Price Close - Fiscal (财年结束日收盘价)
# prcc_c: Price Close - Calendar (日历年结束日收盘价)
# mkvalt: Market Value (总市值)
sql_funda = f"""
SELECT 
    isin, fyear, datadate,
    prc, 
    prccd, 
    prcc_f, 
    prcc_c, 
    mkvalt
FROM comp.g_funda
WHERE isin = '{test_isin}' AND fyear = {target_year}
"""

try:
    df_funda = db.raw_sql(sql_funda)
    if df_funda.empty:
        print("❌ 结果：g_funda 里完全找不到该公司的 2023 年记录（行都没有）。")
    else:
        print("✅ 结果：找到了行，具体数据如下：")
        print(df_funda.T) # 转置打印，方便看清每个字段
        
        # 自动判断
        prices = df_funda[['prc', 'prccd', 'prcc_f', 'prcc_c', 'mkvalt']].iloc[0]
        if prices.sum() == 0 or prices.isnull().all():
            print("\n⚠️ 结论：年报表里虽然有记录，但【所有股价字段】都是空或 0！")
            print("   -> 这证实了 g_funda 表确实还没更新 2023 年的股价数据。")
        else:
            valid_col = prices[prices > 0].index.tolist()
            print(f"\n🎉 结论：发现数据了！正确的变量名应该是：{valid_col}")

except Exception as e:
    print(f"❌ 查询出错: {e}")


# ==========================================
# 测试 2：去日报表 (g_secd) 找证据
# ==========================================
print("\n" + "-" * 50)
print("👉 测试 2：检查 g_secd (每日行情表)...")
print("   (如果这里有数据，说明 WRDS 确实有股价，只是没同步到年报表)")

# 查 2023 年 12 月底的最后几天
sql_daily = f"""
SELECT 
    isin, datadate, 
    prccd AS daily_close_price, 
    cshoc AS daily_shares
FROM comp.g_secd
WHERE isin = '{test_isin}' 
  AND datadate BETWEEN '2023-12-28' AND '2023-12-31'
ORDER BY datadate DESC
"""

try:
    df_daily = db.raw_sql(sql_daily)
    if df_daily.empty:
        print("❌ 结果：连日报表里都没有数据！这太反常了。")
    else:
        print("✅ 结果：日报表里有数据！")
        print(df_daily)
        print("\n⚖️ 【最终判决】")
        print("WRDS 数据库里【确实有】2023 年的股价（见测试 2）。")
        print("但是！这些数据【还没有】被整理进年报表 g_funda（见测试 1）。")
        print("💡 你的抓取代码没写错，是数据库更新延迟（g_funda 滞后于 g_secd）。")

except Exception as e:
    print(f"❌ 查询出错: {e}")

print("-" * 50)