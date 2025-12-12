from enum import IntEnum, auto
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from typing import overload
import json
import inspect
from pydantic import BaseModel, Field, ValidationError
import yfinance as yf
import MySQLdb # type: ignore
import pandas
import pandas_ta_classic as ta
import jpholiday

# チャートの粒度の定義
class ChartGranularity(IntEnum):
	# 日足
	DAILY = auto()
	# 分足
	MINUTE = auto()

# 自作ログの出力先
KABU_LOG_FILE = "kabu-log.txt"
IS_LOGGING: bool = True

# DB接続
# タイムアウトの場合はOperationalErrorで落ちる
conn = MySQLdb.connect(
    	#host="db",        # docker-compose の service 名
		host="192.168.2.199",
    	user="user",      # docker-compose.yml の MYSQL_USER
    	passwd="pass",    # MYSQL_PASSWORD
    	db="stocks",        # MYSQL_DATABASE
    	port=3306,
		connect_timeout=10
	)

cur = conn.cursor()

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

class DB:
	def __init__(self):
		pass

		# 現在時刻が取引時間中であるか
	def is_market_active(self, ticker: str) -> bool:
		cur.execute("""
			SELECT 
    			m.timezone,
    			m.open1,
    			m.close1,
    			m.open2,
    			m.close2
			FROM securities s
			JOIN markets m ON s.market_id = m.id
			WHERE s.code = %s
		""", (ticker,))

		row = cur.fetchone()
		if not row:
			return False  # 該当銘柄なし
    
    	# 現在時刻を市場のタイムゾーンで取得
		t = datetime.now(ZoneInfo(row[0])).time()
		now = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    
    	# 時間1の判定
		if row[1] <= now <= row[2]:
			return True
    
    	# 時間2がある場合のみ判定
		if row[3] and row[4]:
			if row[3] <= now <= row[4]:
				return True
    
		return False
	
	# 現在時刻が取引開始時刻よりも後であるか
	def is_market_open(self, ticker: str) -> bool:
		cur.execute("""
			SELECT 
    			m.timezone,
    			m.open1
			FROM securities s
			JOIN markets m ON s.market_id = m.id
			WHERE s.code = %s
		""", (ticker,))

		row = cur.fetchone()
		if not row:
			return False  # 該当銘柄なし
    
    	# 現在時刻を市場のタイムゾーンで取得
		t = datetime.now(ZoneInfo(row[0])).time()
		now = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    
    	# 時間1の判定
		if row[1] <= now:
			return True
    
		return False
	
	# yfinanceから取得した株価をDBに格納する
	# return：格納件数
	def insert_into_prices(self, ticker: str, prices: pandas.DataFrame, chart_granularity: ChartGranularity) -> int:
		if not ticker:
			raise ValueError("ticker is empty or none")
		
		if prices is None or prices.empty:
			raise ValueError("prices is empty or none")
		
		count = 0
		for index, row in prices.iterrows():

			time = None
			if chart_granularity == ChartGranularity.MINUTE:
				time = index.time()

			if index.date() == date.today() and self.is_market_open(ticker):
				cur.execute("""
    				INSERT INTO prices (security_id, date, time, open, high, low, close, volume)
    				VALUES ((SELECT id FROM securities WHERE code=%s), %s, %s, %s, %s, %s, %s, %s)
					ON DUPLICATE KEY UPDATE open = VALUES(open), high = VALUES(high), low  = VALUES(low), close = VALUES(close), volume = VALUES(volume)
				""", (ticker, index.date(), time, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]))

			else:
				cur.execute("""
					INSERT INTO prices (security_id, date, time, open, high, low, close, volume)
					SELECT s.id, %s, %s, %s, %s, %s, %s, %s
					FROM securities s
					WHERE s.code = %s
				""", (index.date(), time, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"], ticker))

			count += cur.rowcount

		# トランザクション処理を確定
		conn.commit()
		return count
	
	# pricesテーブルからOHLCVの値を取得する
	# データがDBに存在するかの確認はしないので注意
	def select_from_prices(self, ticker: str, begin_range: datetime, end_range: datetime, chart_granularity: ChartGranularity) -> pandas.DataFrame:
		if chart_granularity == ChartGranularity.DAILY:
			# begin_range、end_rangeにに渡されているのがdatetimeである場合はdateに変換しておく
			if type(begin_range) is datetime:
				begin_range = begin_range.date()

			if type(end_range) is datetime:
				end_range = end_range.date()
 
			cur.execute("""
				SELECT date, COALESCE(time, '00:00:00') AS time, open, high, low, close, volume
				FROM prices p
				JOIN securities s ON p.security_id = s.id
				WHERE s.code = %s AND p.date BETWEEN %s AND %s AND p.time IS NULL
            	ORDER BY p.date ASC
			""", (ticker, begin_range, end_range))
			
		else:
			raise NotImplementedError("日足以外は未実装")
		
		rows = cur.fetchall()
		dates = [pandas.to_datetime(str(d) + " " + str(t)) for d, t, *_ in rows]
		ohlcv = [row[2:] for row in rows]

		ohlcv_column = ["Open", "High", "Low", "Close", "Volume"]
		result_df = pandas.DataFrame(ohlcv, columns=ohlcv_column, index=pandas.DatetimeIndex(dates))
		for col in ohlcv_column:
			result_df[col] = pandas.to_numeric(result_df[col], errors="coerce")

		return result_df
	
	# 該当tickerが証券テーブルに登録済みであるか確認する
	def is_ticker_exists(self, ticker: str) -> bool:
		if not ticker:
			return False
		
		cur.execute("""
				SELECT 1
				FROM securities
				WHERE code = %s 
			""", (ticker,))
		
		return cur.fetchone() is not None
	
	# 該当tickerの最初のレコードの日時を取得
	def get_first_record_datetime(self, ticker: str) -> tuple[date, time] | None:
		if not ticker:
			raise ValueError("ticker is empty or none")
		
		cur.execute("""
			SELECT date, time
			FROM prices p
			JOIN securities s ON p.security_id = s.id
			WHERE s.code = %s
			ORDER BY date ASC, time ASC
			LIMIT 1
		""", (ticker,))
			
		first_record = cur.fetchone()
		# 該当するデータがDBにない場合
		if first_record is None:
			return None
		
		return first_record
	
	# 該当tickerの最後のレコードの日時を取得
	def get_end_record_datetime(self, ticker: str) -> tuple[date, time] | None:
		if not ticker:
			raise ValueError("ticker is empty or none")
		
		cur.execute("""
			SELECT date, time
			FROM prices p
			JOIN securities s ON p.security_id = s.id
			WHERE s.code = %s
			ORDER BY date DESC, time DESC
			LIMIT 1
		""", (ticker,))
			
		end_record = cur.fetchone()
		# 該当するデータがDBにない場合
		if end_record is None:
			return None
		
		return end_record

class Logging:
		# 所定のログファイルにログを追記する
	def append_to_log_file(message: str) -> None:
		if not IS_LOGGING or not message:
			return

		with open(KABU_LOG_FILE, "a", encoding="utf-8") as f:
			f.write(message + "\n")

	@overload
	def append_to_log_file_from_bm(bm: BaseModel) -> None: ...
	@overload
	def append_to_log_file_from_bm(bm: BaseModel, message: str) -> None: ...

	@staticmethod
	def append_to_log_file_from_bm(bm: BaseModel, message: str | None = None) -> None:
		if not IS_LOGGING:
			return
		
		line = f"{datetime.now()} | {bm.__class__.__name__} \n" + str(bm.model_dump())

		with open(KABU_LOG_FILE, "a", encoding="utf-8") as f:
			if not message:
				f.write(line + "\n")
			else:
				f.write(line + " | " + message + "\n")

	@overload
	def append_to_log_file_from_dict(payload: dict) -> None: ...
	@overload
	def append_to_log_file_from_dict(payload: dict, message: str) -> None: ...

	@staticmethod
	def append_to_log_file_from_dict(payload: dict, message: str | None = None) -> None:
		if not IS_LOGGING:
			return
		
		def_name = inspect.currentframe().f_back.f_code.co_name

		if not message:
			line = f"{datetime.now()} | {def_name} \n"
		else:
			line = f"{datetime.now()} | {def_name}  | {message}\n"

		line += str(payload)

		with open(KABU_LOG_FILE, "a", encoding="utf-8") as f:
			f.write(line + "\n")

	@overload
	def append_to_log_file_from_df(df: pandas.DataFrame) -> None: ...
	@overload
	def append_to_log_file_from_df(df: pandas.DataFrame, message: str) -> None: ...

	@staticmethod
	def append_to_log_file_from_df(df: pandas.DataFrame, message: str | None = None) -> None:
		if not IS_LOGGING:
			return
		
		def_name = inspect.currentframe().f_back.f_code.co_name

		if not message:
			line = f"{datetime.now()} | {def_name} \n"
		else:
			line = f"{datetime.now()} | {def_name}  | {message}\n"

		# dfの大きさが6以下の場合は全行表示
		if len(df) <= 6:
			line += df.to_string()
		# dfの大きさが6を超える場合は最初の３行と最後の３行を表示
		else:
			line += df.head(3).to_string() + "\n"
			line += "(～中略～)\n"
			line += df.tail(3).to_string()

		line += "\n" + f"({len(df)} rows)\n"

		with open(KABU_LOG_FILE, "a", encoding="utf-8") as f:
			f.write(line + "\n")

class GetCurrentPriceInput(BaseModel):
    ticker: str

class GetPriceInput(BaseModel):
	ticker: str 
	begin_range: datetime
	end_range: datetime
	chart_granularity: ChartGranularity

class Backend:
	db = DB()
	log = Logging()

	def __init__(self):
		pass

	# リアルタイムの情報を返すときに使う想定　DBに格納しない
	# ex)今～の株価何円？ -> この関数を経由して返す
	def get_current_price(self, input: GetCurrentPriceInput) -> str:
		try:
			if isinstance(input, dict):
				input = GetCurrentPriceInput(**input)
		except ValidationError as e:
			self.log.append_to_log_file_from_dict(input, f"ValidationError: {e}")
			raise ValueError("入力値の形式が不正です") from e

		if not input.ticker:
			err = "銘柄コードが指定されていません"
			self.log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		if not self.db.is_ticker_exists(input.ticker):
			err = "無効な銘柄コードが指定されました"
			self.log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)

		stock = yf.Ticker(input.ticker)
		info = stock.info
		price = stock.history(period="1d")

		# ちゃんと取得できたかを確認
		if len(price) <= 0:
			err = "データの取得に失敗しました"
			self.log.append_to_log_file_from_bm(input, err)
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

		self.log.append_to_log_file_from_bm(input)
		self.log.append_to_log_file_from_dict(result)

		return json.dumps(result, ensure_ascii=False)
	
	# 指定範囲の株価情報をDBから読みだす　DBになければyfから取得する
	# 内部用　LLMに公開する際にはDataFrameをJSONに変換する関数を挟む
	def get_price(self, input: GetPriceInput) -> pandas.DataFrame:
		try:
			if isinstance(input, dict):
				input = GetPriceInput(**input)
		except ValidationError as e:
			self.log.append_to_log_file_from_dict(input, f"ValidationError: {e}")
			raise ValueError("入力値の形式が不正です") from e

		if not input.ticker:
			err = "銘柄コードが指定されていません。"
			self.log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		if not self.db.is_ticker_exists(input.ticker):
			err = "無効な銘柄コードが指定されました。"
			self.log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		if input.begin_range is None:
			err = "解析期間の開始日が指定されていません。"
			self.log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		if input.end_range is None:
			err = "解析期間の終了日が指定されていません。"
			self.log.append_to_log_file_from_bm(input, err)
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
			self.log.append_to_log_file_from_bm(input, err)
			raise ValueError(err)
		
		# 指定した日が土日祝の場合はデータが取れないので範囲を狭める方向にずらす
		# input.begin_rangeの場合は日を進める
		while input.begin_range.weekday() >= 5 or jpholiday.is_holiday(input.begin_range):
			input.begin_range = input.begin_range + timedelta(days=1)
		# input.end_rangeの場合は日を戻す
		while input.end_range.weekday() >= 5 or jpholiday.is_holiday(input.end_range):
			input.end_range = input.end_range - timedelta(days=1)
		
		# 当面は日足のみ対応なのでchart_granularityに何が入力されていても日足を指定したものとして扱う
		chart_granularity = ChartGranularity.DAILY
		
		# 入力に問題はなさそうなので一旦ログに書き込む
		self.log.append_to_log_file_from_bm(input)

		db = DB()

		# DBに格納されているデータの範囲を取得する これらは(date, time)のタプルなので注意
		# 当面は日足のみ対応なのでdateのみを抜き出す（Noneのときはそもそも抜き出せないから何もしない）
		first_record: date = db.get_first_record_datetime(input.ticker)
		if first_record is not None:
			first_record = first_record[0]
		end_record: date = db.get_end_record_datetime(input.ticker)
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
			
			db.insert_into_prices(input.ticker, history, chart_granularity)
		
		# DBに存在するデータよりも前のデータが必要な場合
		# input.begin_rangeからfirst_recordの前日までのデータを取ってくる
		if first_record is not None and first_record > input.begin_range.date():
			history = yf.Ticker(input.ticker).history(start=date_to_yf_history(input.begin_range), end=date_to_yf_history((first_record - timedelta(days=1))))
			db.insert_into_prices(input.ticker, history, chart_granularity)

		# DBに存在するデータよりも後のデータが必要な場合
		# end_recordの翌日から今日までのデータを取ってくる
		if end_record is not None and end_record < input.end_range.date():
			# 不足分のデータが今日のみの場合
			if end_record + timedelta(days=1) == now.date():
				# 現在時刻が取引時間開始時刻以降の場合はデータを取ってくる
				if db.is_market_open(input.ticker):
					history = yf.Ticker(input.ticker).history(period="1d")
					db.insert_into_prices(input.ticker, history, chart_granularity)
			# 不足分のデータが今日のみでない場合は今日のデータも存在しないので取ってくる
			else:
				history = yf.Ticker(input.ticker).history(start=date_to_yf_history((end_record + timedelta(days=1))), end=date_to_yf_history(now.date()))
				db.insert_into_prices(input.ticker, history, chart_granularity)
		
		df = db.select_from_prices(input.ticker, input.begin_range, input.end_range, chart_granularity)
		# データが取れていることを確認する（が、通常は問題ないはず）
		if df is None:
			raise LookupError("データフレームが空です。")
		
		# 日付をindexからレコード内に含めるように変更
		df = df.reset_index()
		# 日付のレコードが自動的にindexとなるのでDateに直しておく
		df = df.rename(columns={"index": "Date",})

		self.log.append_to_log_file_from_df(df)
		return df
	
	# テクニカル分析の内部関数　引数、戻り値ともに他の関数と連携しやすいDataFrameとする
	def do_technical_analysis(self, df: pandas.DataFrame) -> pandas.DataFrame:
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

		self.log.append_to_log_file_from_df(df)
		return df