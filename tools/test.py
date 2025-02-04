import pandas as pd
import pybroker
from pybroker.data import DataSource
from pybroker import Strategy, ExecContext, portfolio, Trade


class CSVDataSource(DataSource):
    def __init__(self):
        super().__init__()
        # 注册自定义列
        pybroker.register_columns('dividend_per_share')

    def _fetch_data(self, symbols, start_date, end_date, _timeframe, _adjust):
        df = pd.read_csv('/home/summeryin/PycharmProjects/pybroker/tools/bank_per_share.csv')
        df['symbol'] = df['symbol'].apply(lambda x: '{:06d}'.format(x))  # 股票代码格式化
        df['date'] = pd.to_datetime(df['date'])  # 日期转换
        df = df.fillna(0)  # 缺失值填充
        df = df[df['symbol'].isin(symbols)]
        df = df[(df['date'] >= pd.Timestamp(start_date)) & (df['date'] <= pd.Timestamp(end_date))]

        print(df.head())  # 确保数据正确
        return df


csv_data_source = CSVDataSource()

df = csv_data_source.query(['600000', '000001'], '2010-01-01', '2024-12-31')


# 策略逻辑
def buy_hold_150_days(ctx: ExecContext):
    pos = ctx.long_pos()

    # 1. 如果没有持仓，并且股息率大于 1，则买入并持仓 150 天
    if not pos and ctx.dividend_per_share[-1] > 1:
        ctx.buy_shares = 10000
        ctx.hold_bars = 150  # 确保持仓 150 天

    # 2. 如果已经持仓，确保持仓时间达到 150 天后再卖出
    # if pos and (Trade.exit_date - Trade.entry_date) >= 150:
    #     ctx.sell_shares = pos.shares  # 卖出全部持仓


# 运行回测
strategy = Strategy(csv_data_source, '2010-01-01', '2024-12-31')
strategy.add_execution(buy_hold_150_days, ['000001', '600000'])

result = strategy.backtest()
print(result.orders)