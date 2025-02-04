import pandas as pd
from sqlalchemy import create_engine


def export_historical_data_to_csv(db_config, stock_code, start_date, end_date, output_csv_path):
    """
    此函数用于从 MySQL 数据库中提取 historical_data 表中的数据，并将其导出为 CSV 文件。

    参数:
    db_config (dict): 包含数据库连接信息的字典，例如 {'host': 'localhost', 'user': 'root', 'password': '2wsxCFT6', 'database': 'stock_prediction', 'charset': 'utf8mb4'}
    stock_code (str): 股票代码
    start_date (str): 时间范围的开始日期，格式为 'YYYY-MM-DD'
    end_date (str): 时间范围的结束日期，格式为 'YYYY-MM-DD'
    output_csv_path (str): 导出 CSV 文件的本地路径
    """
    try:
        # 创建数据库引擎
        engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}?charset={db_config['charset']}")

        # 构建 SQL 查询语句
        # query = f"""
        # SELECT stock_code, stock_name, trade_date, open_price, high_price, low_price, close_price, volume, turnover, free_share, total_share, free_market_cap
        # FROM historical_data
        # WHERE stock_code = '{stock_code}'
        # AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        # """
        query = f"""
                SELECT stock_code, stock_name, trade_date, open_price, high_price, low_price, close_price
                FROM historical_data
                WHERE stock_code = '{stock_code}'
                AND trade_date BETWEEN '{start_date}' AND '{end_date}'
                """
        # 使用 pandas 的 read_sql 函数执行查询
        df = pd.read_sql(query, engine)

        # 将结果导出为 CSV 文件
        df.to_csv(output_csv_path, index=False)
        print(f"数据已成功导出到 {output_csv_path}")
    except Exception as e:
        print(f"导出数据时发生错误: {str(e)}")


# 示例使用
if __name__ == "__main__":
    db_config = {'host': 'localhost', 'user': 'root', 'password': '2wsxCFT6', 'database': 'stock_prediction', 'charset': 'utf8mb4'}
    stock_code = '300158'
    start_date = '2019-01-01'
    end_date = '2024-12-31'
    output_csv_path = 'historical_data_300119.csv'
    export_historical_data_to_csv(db_config, stock_code, start_date, end_date, output_csv_path)