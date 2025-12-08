from kabu import Backend

b = Backend()

def Test_is_ticker_exists():
    print(b.is_ticker_exists(""))
    print(b.is_ticker_exists("1669.T"))
    print(b.is_ticker_exists("NTT"))
    print(b.is_ticker_exists("1301.T"))

if __name__ == "__main__":
    Test_is_ticker_exists()
# 	ticker: str = "7013.T"
# 	# stock = yf.Ticker(ticker)
# 	# price = stock.history(period="7d")




# 	# b.insert_into_prices(ticker, price, ChartGranularity.DAILY)

# 	# print("first:", end="")
# 	# print(b.get_first_record_datetime("7013.T"))
# 	# print("end  :", end="")
# 	# print(b.get_end_record_datetime("7013.T"))

# 	begin_day: datetime = date(2025, 11, 23)
# 	end_day: datetime = date(2025, 11, 29)
# 	print(b.select_from_prices(ticker, begin_day, end_day, ChartGranularity.DAILY))

# sys.exit()