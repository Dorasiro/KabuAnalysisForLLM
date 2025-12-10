from kabu import Tools, ChartGranularity, DB, GetCurrentPriceInput, GetPriceInput
from datetime import datetime

t = Tools()

if __name__ == "__main__":
    db = DB()

    print(t.get_current_price(GetCurrentPriceInput(ticker="9432.T")))

    input = GetPriceInput(ticker="7013.T", begin_range=datetime(2025, 4, 1, 0,0,0), end_range=datetime.now(), chart_granularity=ChartGranularity.DAILY)
    #print(t.get_price(input))
    print(t.do_technical_analysis(input))