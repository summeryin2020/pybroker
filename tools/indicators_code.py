import pandas as pd
from sqlalchemy import create_engine

#导出数据库中基于板块的技术因子CSV
def export_data_by_stock_and_date(db_config, stock_code, start_date, end_date, indicator_names, output_csv_path):
    """
    此函数用于从 MySQL 数据库中提取 technical_indicators 表和 historical_data 表中的数据，并将其导出为 CSV 文件。

    参数:
    db_config (dict): 包含数据库连接信息的字典，例如 {'host': 'localhost', 'user': 'root', 'password': '2wsxCFT6', 'database': 'stock_prediction', 'charset': 'utf8mb4'}
    stock_code (str): 个股代码
    start_date (str): 时间范围的开始日期，格式为 'YYYY-MM-DD'
    end_date (str): 时间范围的结束日期，格式为 'YYYY-MM-DD'
    indicator_names (list): 要筛选的技术指标名称列表，例如 ['WINNER_RATIO', 'RSI_14', 'MA_20','14_day_volatility','30_day_volatility']
    output_csv_path (str): 导出 CSV 文件的本地路径
    """
    try:
        # 创建数据库引擎
        engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}?charset={db_config['charset']}")

        # 构建 SQL 查询语句来获取技术指标数据
        indicator_names_str = ", ".join([f"'{name}'" for name in indicator_names])
        technical_query = f"""
        SELECT 
            t.trade_date,
            t.stock_code,          
            t.indicator_name, 
            t.indicator_value
        FROM technical_indicators t
        WHERE t.indicator_name IN ({indicator_names_str})
        AND t.trade_date BETWEEN '{start_date}' AND '{end_date}'
        AND t.stock_code = '{stock_code}'
        """
        technical_df = pd.read_sql(technical_query, engine)

        # 构建 SQL 查询语句来获取历史数据
        historical_query = f"""
        SELECT 
            h.trade_date,
            h.stock_code, 
            h.open_price,
            h.high_price,
            h.low_price,
            h.close_price
        FROM historical_data h
        WHERE h.trade_date BETWEEN '{start_date}' AND '{end_date}'
        AND h.stock_code = '{stock_code}'
        """
        historical_df = pd.read_sql(historical_query, engine)

        # 将技术指标数据进行透视，使得每个指标成为单独的列
        pivoted_technical_df = technical_df.pivot(index=['stock_code', 'trade_date'], columns='indicator_name', values='indicator_value').reset_index()

        # 合并历史数据和技术指标数据
        merged_df = pd.merge(historical_df, pivoted_technical_df, on=['stock_code', 'trade_date'], how='left')

        # 修改列名
        merged_df.rename(columns={
            'trade_date': 'date',
            'stock_code': 'symbol',
            'open_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'close_price': 'close'
        }, inplace=True)

        # 确保 rsi 列存在，如果不存在则填充为 NaN
        if 'RSI_14' not in merged_df.columns:
            merged_df['rsi'] = pd.NA
        else:
            merged_df.rename(columns={'RSI_14': 'rsi'}, inplace=True)

        # 将结果导出为 CSV 文件
        merged_df.to_csv(output_csv_path, index=False)
        print(f"数据已成功导出到 {output_csv_path}")
    except Exception as e:
        print(f"导出数据时发生错误: {str(e)}")


# 示例使用
if __name__ == "__main__":
    db_config = {'host': 'localhost', 'user': 'root', 'password': '2wsxCFT6', 'database': 'stock_prediction', 'charset': 'utf8mb4'}
    stock_code = '300119'  # 示例个股代码
    start_date = '2015-01-01'
    end_date = '2024-12-31'
    indicator_names = ['RSI_14']
    output_csv_path = '300119_rsi.csv'
    export_data_by_stock_and_date(db_config, stock_code, start_date, end_date, indicator_names, output_csv_path)