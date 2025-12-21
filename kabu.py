from enum import IntEnum, auto
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import json
from pydantic import BaseModel, Field, ValidationError
import yfinance as yf
import pandas
import pandas_ta_classic as ta
import jpholiday
from database import DB
from my_logging import Logging

# yfinanceのticker.historyに設定できるYYYY-MM-DDの形に変換
# 分足データは後から取得できないため、dateのみ考慮
def date_to_yf_history(d: date) -> str:
	return d.strftime("%Y-%m-%d")

# dateをdatetimeに変換する
# datetimeを渡した場合はそのまま返す
def convert_to_datetime(d) -> datetime:
    if type(d) is datetime:
        return d
    
    if type(d) is date:
        return datetime.combine(d, datetime.min.time())
    
    if type(d) is str:
        return datetime.strptime(d, "%Y-%m-%d")
    
    raise ValueError("引数にはdateもしくはdatetime型の変数を渡してください。")

class GetCurrentPriceInput(BaseModel):
    ticker: str

class GetPriceInput(BaseModel):
	ticker: str 
	begin_range: datetime
	end_range: datetime
	chart_granularity: str

class Backend:
	db = DB()

	def __init__(self):
		pass

	# リアルタイムの情報を返すときに使う想定　DBに格納しない
	# ex)今～の株価何円？ -> この関数を経由して返す
	def get_current_price(self, input: GetCurrentPriceInput, log: Logging = None) -> str:
		if log == None:
			log = Logging()

		try:
			if isinstance(input, dict):
				input = GetCurrentPriceInput(**input)
		except ValidationError as e:
			log.append_to_log_file_from_dict(input, f"ValidationError: {e}")
			raise ValueError("入力値の形式が不正です") from e

		if not input.ticker:
			err = "銘柄コードが指定されていません"
			log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		if not self.db.is_ticker_exists(input.ticker):
			err = "無効な銘柄コードが指定されました"
			log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)

		stock = yf.Ticker(input.ticker)
		info = stock.info
		price = stock.history(period="1d")

		# ちゃんと取得できたかを確認
		if len(price) <= 0:
			err = "データの取得に失敗しました"
			log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)

		current = info.get("currentPrice")

		result = dict(
			current = float(current) if current is not None else None,
			open = float(price["Open"].iloc[-1]),
			high = float(price["High"].iloc[-1]),
			low = float(price["Low"].iloc[-1]),
			close = float(price["Close"].iloc[-1]),
			volume = float(price["Volume"].iloc[-1])
		)

		log.append_to_log_file_from_bm(input)
		log.append_to_log_file_from_dict(result)

		return json.dumps(result, ensure_ascii=False)
	
	# 指定範囲の株価情報をDBから読みだす　DBになければyfから取得する
	# 内部用　LLMに公開する際にはDataFrameをJSONに変換する関数を挟む
	def get_price(self, input: GetPriceInput, log: Logging = None) -> pandas.DataFrame:
		if log == None:
			log = Logging()

		try:
			if isinstance(input, dict):
				input = GetPriceInput(**input)
		except ValidationError as e:
			log.append_to_log_file_from_dict(input, f"ValidationError: {e}")
			raise ValueError("入力値の形式が不正です") from e

		if not input.ticker:
			err = "銘柄コードが指定されていません。"
			log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		if not self.db.is_ticker_exists(input.ticker):
			err = "無効な銘柄コードが指定されました。"
			log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		if input.begin_range is None:
			err = "解析期間の開始日が指定されていません。"
			log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		if input.end_range is None:
			err = "解析期間の終了日が指定されていません。"
			log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		# これより後は両変数ともにdatetimeとして扱う
		input.begin_range = convert_to_datetime(input.begin_range)
		input.end_range = convert_to_datetime(input.end_range)
		
		# input.end_rangeが未来に設定されている場合は今日までとする
		now = datetime.now()
		if input.end_range > now:
			input.end_range = now
		
		# 開始日と終了日が逆転してた場合のエラー
		if input.begin_range > input.end_range:
			err = "解析機関の開始日と終了日に矛盾があります。"
			log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		# 指定した日が土日祝の場合はデータが取れないので範囲を狭める方向にずらす
		# input.begin_rangeの場合は日を進める
		while input.begin_range.weekday() >= 5 or jpholiday.is_holiday(input.begin_range):
			input.begin_range = input.begin_range + timedelta(days=1)
		# input.end_rangeの場合は日を戻す
		while input.end_range.weekday() >= 5 or jpholiday.is_holiday(input.end_range):
			input.end_range = input.end_range - timedelta(days=1)
		
		# 当面は日足のみ対応なのでchart_granularityに何が入力されていても日足を指定したものとして扱う
		chart_granularity = "daily"
		
		# 入力に問題はなさそうなので一旦ログに書き込む
		log.append_to_log_file_from_bm(input)

		# DBに格納されているデータの範囲を取得する これらは(date, time)のタプルなので注意
		# 当面は日足のみ対応なのでdateのみを抜き出す（Noneのときはそもそも抜き出せないから何もしない）
		first_record: date = self.db.get_first_record_datetime(input.ticker)
		if first_record is not None:
			first_record = first_record[0]
		end_record: date = self.db.get_end_record_datetime(input.ticker)
		if end_record is not None:
			end_record = end_record[0]

		# 該当銘柄のデータが存在しない場合はinput.begin_rangeから今日までのデータを取ってくる
		# ただし、input.begin_rangeから今日までの期間が半年未満の場合は半年分のデータを取る
		if first_record == None and end_record == None:
			history: pandas.DataFrame = None
			# 概ね半年なので180日としておく
			if now - input.begin_range >= timedelta(days=180):
				# 半年以上の場合はinput.begin_rangeから今日までのデータを取得
				history = yf.Ticker(input.ticker).history(start=date_to_yf_history(input.begin_range), end=date_to_yf_history(now.date()))
			else:
				# 半年未満の場合は今日から半年前までのデータを取得
				history = yf.Ticker(input.ticker).history(period="6mo")
			
			self.db.insert_into_prices(input.ticker, history, chart_granularity)
		
		# DBに存在するデータよりも前のデータが必要な場合
		# input.begin_rangeからfirst_recordの前日までのデータを取ってくる
		if first_record is not None and first_record > input.begin_range.date():
			history = yf.Ticker(input.ticker).history(start=date_to_yf_history(input.begin_range), end=date_to_yf_history((first_record - timedelta(days=1))))
			self.db.insert_into_prices(input.ticker, history, chart_granularity)

		current: pandas.DataFrame = None

		# DBに存在するデータよりも後のデータが必要な場合
		# end_recordの翌日から今日までのデータを取ってくる
		if end_record is not None and end_record < input.end_range.date():
			# 前日分までを取得してくる
			history = yf.Ticker(input.ticker).history(start=date_to_yf_history((end_record + timedelta(days=1))), end=date_to_yf_history(now.date() - timedelta(days=1)))
			self.db.insert_into_prices(input.ticker, history, chart_granularity)

			# 場中の場合はDBにデータを入れず、戻り値となるdfにだけデータを入れる
			if self.db.is_market_active(input.ticker):
				current = history, yf.Ticker(input.ticker).history(period="1d")
			# 場中でないかつ閉場時間を過ぎている場合は確定データが出ているからDBに入れる
			elif(self.db.is_market_closed(input.ticker)):
				history = yf.Ticker(input.ticker).history(period="1d")
				self.db.insert_into_prices(input.ticker, history, chart_granularity)
			# 取引開始前が当てはまるが取得できるデータがないので何もしない
			else:
				pass
		
		df = self.db.select_from_prices(input.ticker, input.begin_range, input.end_range, chart_granularity)
		if current != None:
			df = pandas.concat([df, current], ignore_index=True)

		# データが取れていることを確認する（が、通常は問題ないはず）
		if df is None:
			raise LookupError("データフレームが空です。")
		
		# 日付をindexからレコード内に含めるように変更
		df = df.reset_index()
		# 日付のレコードが自動的にindexとなるのでDateに直しておく
		df = df.rename(columns={"index": "Date",})

		log.append_to_log_file_from_df(df)
		return df
	
	# テクニカル分析の内部関数　引数、戻り値ともに他の関数と連携しやすいDataFrameとする
	def do_technical_analysis(self, df: pandas.DataFrame, log: Logging = None) -> pandas.DataFrame:
		if log == None:
			log = Logging()

		# 移動平均線（短期）
		df.ta.sma(length=25, append=True)
		# 移動平均線（中期）
		df.ta.sma(length=50, append=True)
		# 移動平均線（長期）
		df.ta.sma(length=75, append=True)
		# RSI
		df.ta.rsi(length=14, append=True)
		# MACD
		df.ta.macd(append=True)

		# MACD関連の列名が分かりづらいので一般的な形に直しておく
		df = df.rename(columns={
	    	"MACD_12_26_9": "MACD",
    		"MACDh_12_26_9": "MACD_histogram",
    		"MACDs_12_26_9": "MACD_signal"
		})

		# 小数点を丸める
		df = df.round(2)

		log.append_to_log_file_from_df(df)
		return df