import duckdb
import pandas as pd
import plotly.express as px

def plot_etf_moving_averages(etf_code: str, days: int = 80):
    """
    从etf_ma_indicators表中获取指定ETF的数据，并绘制移动平均线走势图。

    Args:
        etf_code (str): ETF代码，例如 '159915.SZ'。
        days (int): 要绘制的最近天数。
    """
    db_path = 'etf.duckdb'
    
    try:
        # 连接到DuckDB数据库
        print(f"正在连接到数据库: {db_path}")
        con = duckdb.connect(database=db_path, read_only=True)

        # 查询数据
        # 使用 ORDER BY trade_date DESC LIMIT days 筛选最近的数据
        query = f"""
        SELECT 
            trade_date,
            close,
            ma_5_day,
            ma_10_day
        FROM etf_ma_indicators
        WHERE ts_code = '{etf_code}'
        ORDER BY trade_date DESC
        LIMIT {days};
        """
        
        print(f"正在查询ETF代码为 '{etf_code}' 的最近 {days} 天数据...")
        df = con.execute(query).fetchdf()

        # 检查是否获取到数据
        if df.empty:
            print(f"未找到ETF代码为 '{etf_code}' 的数据。请检查代码或数据是否存在。")
            return

        # 将trade_date列转换为日期类型并排序
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values(by='trade_date')

        # 使用Plotly Express绘制图表
        print("正在绘制图表...")
        fig = px.line(
            df,
            x='trade_date',
            y=['close', 'ma_5_day', 'ma_10_day'],
            title=f'{etf_code} 最近 {days} 天的收盘价与移动平均线',
            labels={
                'trade_date': '交易日期',
                'value': '价格',
                'variable': '指标'
            },
            template='plotly_white'
        )

        # 更新图表布局，使其更具可读性
        fig.update_layout(
            legend_title_text='指标',
            xaxis_title=None,
            yaxis_title='价格',
            hovermode='x unified'
        )
        
        # 显示图表
        fig.show()
        
    except duckdb.OperationalError as e:
        print(f"数据库操作错误：{e}")
        print("请确保 'etf.duckdb' 文件存在，并且 'etf_ma_indicators' 表已通过dbt成功生成。")
    finally:
        # 关闭数据库连接
        if 'con' in locals() and con:
            con.close()
            print("数据库连接已关闭。")

if __name__ == '__main__':
    # 运行主函数，指定ETF代码并显示最近80天的数据
    plot_etf_moving_averages(etf_code='159915.SZ', days=80)
