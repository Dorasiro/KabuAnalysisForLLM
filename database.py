import datetime as dt
import MySQLdb
import pandas
from zoneinfo import ZoneInfo

class DB:
    # 初回get_cursor実行時に接続される
    conn: MySQLdb.Connection = None

    def __init__(self):
        pass

    # DB接続
    # タイムアウトの場合はOperationalErrorで落ちる
    @staticmethod
    def get_connection() ->  MySQLdb.Connection:
        return MySQLdb.connect(
            #host="db",        # docker-compose の service 名
            host="192.168.2.199",
            user="user",      # docker-compose.yml の MYSQL_USER
            passwd="pass",    # MYSQL_PASSWORD
            db="stocks",        # MYSQL_DATABASE
            port=3306,
            connect_timeout=10
        )
    
    # connからcursorを取得
    # 接続が切れていた場合は再接続する
    @classmethod
    def get_cursor(cls):
        try:
            # 初回はそのまま接続
            if cls.conn is None:
                cls.conn = cls.get_connection()
            else:
                cls.conn.ping(reconnect=True)
        except Exception:
            # 再接続
            cls.conn = cls.get_connection()
        
        return cls.conn.cursor()

    # 現在時刻が取引時間中であるか
    def is_market_active(self, ticker: str) -> bool:
        cur = DB.get_cursor()
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
        t = dt.datetime.now(ZoneInfo(row[0])).time()
        now = dt.timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    
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
        cur = DB.get_cursor()
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
        t = dt.datetime.now(ZoneInfo(row[0])).time()
        now = dt.timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    
        # 時間1の判定
        if row[1] <= now:
            return True
    
        return False
    
    # yfinanceから取得した株価をDBに格納する
    # return：格納件数
    def insert_into_prices(self, ticker: str, prices: pandas.DataFrame, chart_granularity: str) -> int:
        if not ticker:
            raise ValueError("ticker is empty or none")
        
        if prices is None or prices.empty:
            raise ValueError("prices is empty or none")
        
        count = 0
        for index, row in prices.iterrows():

            time = None
            if chart_granularity == "minute":
                time = index.time()

            cur = DB.get_cursor()
            if index.date() == dt.date.today() and self.is_market_open(ticker):
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
        DB.conn.commit()
        return count
    
    # pricesテーブルからOHLCVの値を取得する
    # データがDBに存在するかの確認はしないので注意
    def select_from_prices(self, ticker: str, begin_range: dt.datetime, end_range: dt.datetime, chart_granularity: str) -> pandas.DataFrame:
        if chart_granularity == "daily":
            # begin_range、end_rangeにに渡されているのがdatetimeである場合はdateに変換しておく
            if type(begin_range) is dt.datetime:
                begin_range = begin_range.date()

            if type(end_range) is dt.datetime:
                end_range = end_range.date()
 
            cur = DB.get_cursor()
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
        
        cur = DB.get_cursor()
        cur.execute("""
                SELECT 1
                FROM securities
                WHERE code = %s 
            """, (ticker,))
        
        return cur.fetchone() is not None
    
    # 該当tickerの最初のレコードの日時を取得
    def get_first_record_datetime(self, ticker: str) -> tuple[dt.date, dt.time] | None:
        if not ticker:
            raise ValueError("ticker is empty or none")
        
        cur = DB.get_cursor()
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
    def get_end_record_datetime(self, ticker: str) -> tuple[dt.date, dt.time] | None:
        if not ticker:
            raise ValueError("ticker is empty or none")
        
        cur = DB.get_cursor()
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