import datetime as dt
import enum
import pydantic
import pandas
import kabu
from my_model import MyModel
from my_logging import Logging

#import workspace.KabuAnalysisForLLM.kabu as kabu
#from workspace.KabuAnalysisForLLM.my_model import MyModel
#from workspace.KabuAnalysisForLLM.my_logging import Logging

def data_frame_to_dict(df: pandas.DataFrame) -> dict:
	# まず Python の基本型に変換
	df = df.astype(object)
	# numpy 型や datetime64 を Python の型に変換
	df = df.applymap(lambda x: x.isoformat() if hasattr(x, "isoformat") else x)
	return df.to_dict(orient="records")


class GetCurrentPriceInput(kabu.GetCurrentPriceInput, MyModel):
	ticker: str = pydantic.Field( ..., description="""
		必ず証券コード＋市場サフィックスを指定してください。日本株は .T を付けます
		例：トヨタの場合は7203.T、NTTの場合は9432.T、IHIの場合は7013.T
		会社名ではなく証券コードを入力してください。""",
	)

class GetPriceInput(kabu.GetPriceInput, MyModel):
	ticker: str = pydantic.Field(..., description="証券コード+市場サフィックス（トヨタの場合は7203.Tなど）")
	begin_range: dt.datetime = pydantic.Field(..., description="分析期間の開始日（yyyy-mm-dd）")
	end_range: dt.datetime = pydantic.Field(..., description="分析期間の終了日（yyyy-mm-dd）")
	chart_granularity: str = pydantic.Field(..., description="チャートの粒度（日足：daily）")

class Tools:
	class Valves(pydantic.BaseModel):
		is_logging: bool = pydantic.Field(default=False, description="ログファイルを生成するかどうか")
		log_file_name: str = pydantic.Field(default="kabu-log.txt", description="ログファイル名")
	
	b = kabu.Backend()
	log: Logging = None

	def __init__(self):
		self.valves = self.Valves()

		self.log = Logging(self.valves.log_file_name, self.valves.is_logging)

	#def get_current_price(self, input: GetCurrentPriceInput) -> str:
		#return self.b.get_current_price(input, self.log)
    
	# 指定範囲の株価情報をDBから読みだす　DBになければyfから取得する
	def get_price(self, input: GetPriceInput) -> dict:
		return data_frame_to_dict(self.b.get_price(input, self.log))
    
	#def do_technical_analysis(self, input: GetPriceInput) -> str:
	#	df: pandas.DataFrame = self.b.get_price(input)
	#	return self.b.do_technical_analysis(df).to_json(orient="records", date_format="iso")
    
t = Tools()
log = Logging("kabu-log.txt")
t.log = log
#print(t.get_current_price(GetCurrentPriceInput(ticker="9432.T")))
print(t.get_price(GetPriceInput(ticker="7013.T", begin_range=dt.datetime(2025, 4, 1, 0,0,0), end_range=dt.datetime.now(), chart_granularity="daily")))
#print(t.do_technical_analysis(GetPriceInput(ticker="7013.T", begin_range=dt.datetime(2025, 4, 1, 0,0,0), end_range=dt.datetime.now(), chart_granularity="daily")))