import wrds
import pandas as pd
import yaml
import os
import sys

def main():
    # 1. 读取配置
    print("正在读取配置文件...")
    with open("config/global_cfg.yaml", "r") as f:
        config = yaml.safe_load(f)

    # 强制锁定我们要抓取的年份
    target_year = 2023
    target_date_str = '2023-12-29' # 2023年最后一个交易日
    
    isin_list_path = config['prime_standard_isins']
    output_path = config['wrds_raw_data']

    # 2. 读取 ISIN
    print(f"正在读取 ISIN 名单: {isin_list_path}")
    isin_df = None
    try:
        isin_df = pd.read_excel(isin_list_path, engine='openpyxl')
    except:
        try:
            isin_df = pd.read_csv(isin_list_path, encoding='utf-8', on_bad_lines='skip')
        except:
             isin_df = pd.read_csv(isin_list_path, encoding='latin1', on_bad_lines='skip', sep=';')

    target_col = None
    for col in isin_df.columns:
        if "ISIN" in str(col).upper():
            target_col = col
            break
            
    if target_col:
        clean_series = isin_df[target_col].dropna().astype(str)
        target_isins = tuple([x for x in clean_series.tolist() if len(x) >= 11])
        print(f"✅ 成功提取到 {len(target_isins)} 个 ISIN。")
    else:
        print("❌ 错误：找不到 ISIN 列")
        return

    # 3. 连接 WRDS
    print("正在连接 WRDS...")
    db = wrds.Connection()

    # =========================================================================
    # 第一步：去 g_funda (年报表) 抓取会计数据 (净利润、权益、总资产)
    # =========================================================================
    print(f"1️⃣  正在从年报表抓取 {target_year} 年的【会计数据】(净利润、权益)...")
    
    sql_acct = f"""
    SELECT 
        isin, 
        fyear AS year_, 
        at AS total_assets, 
        nicon AS net_income, 
        ceq AS common_equity
    FROM 
        comp.g_funda
    WHERE 
        isin IN {target_isins} 
        AND fyear = {target_year}
        AND consol = 'C' 
        AND indfmt = 'INDL' 
        AND popsrc = 'I'
    """
    df_acct = db.raw_sql(sql_acct)
    print(f"   -> 抓取到 {len(df_acct)} 条会计记录。")

    # =========================================================================
    # 第二步：去 g_secd (每日行情表) 抓取年底的【市场数据】(股价、股数)
    # =========================================================================
    print(f"2️⃣  正在从每日行情表抓取 {target_date_str} 的【股价数据】...")
    
    # 我们抓取 2023-12-25 到 2023-12-31 之间的数据，取每家公司最晚的一天
    # 这样防止某家公司 29 号停牌
    sql_mkt = f"""
    SELECT 
        isin, 
        datadate,
        prccd AS price_close,
        cshoc AS shares_outstanding
    FROM 
        comp.g_secd
    WHERE 
        isin IN {target_isins} 
        AND datadate BETWEEN '2023-12-25' AND '2023-12-31'
    """
    df_mkt_raw = db.raw_sql(sql_mkt)
    
    # 只保留每家公司日期最晚的那一行 (通常是 12-29)
    df_mkt = df_mkt_raw.sort_values('datadate').groupby('isin').tail(1).copy()
    print(f"   -> 抓取到 {len(df_mkt)} 条股价记录。")

    # =========================================================================
    # 第三步：数据合并 (Merge)
    # =========================================================================
    print("3️⃣  正在合并会计数据和市场数据...")
    
    if df_acct.empty or df_mkt.empty:
        print("❌ 严重错误：会计数据或市场数据有一方为空，无法合并！")
        return

    # 按照 ISIN 进行合并
    df_final = pd.merge(df_acct, df_mkt[['isin', 'price_close', 'shares_outstanding']], on='isin', how='inner')
    
    # 计算市值
    df_final['market_cap'] = df_final['price_close'] * df_final['shares_outstanding']
    
    # 打印预览
    print("-" * 30)
    print("合并后数据预览:")
    print(df_final[['isin', 'net_income', 'price_close', 'market_cap']].head())
    print("-" * 30)

    if df_final.empty:
        print("⚠️ 合并后结果为空！(可能是 ISIN 匹配不上)")
    else:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_final.to_parquet(output_path)
        print(f"🎉 完美胜利！成功生成了 {len(df_final)} 行包含完整 2023 年数据的文件。")
        print(f"✅ 文件已保存至: {output_path}")
        print("🚀 快去运行: python code/python/prepare_data.py")

if __name__ == '__main__':
    main()