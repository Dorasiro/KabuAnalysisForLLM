import datetime as dt
import enum
import pydantic
import my_model
import kabu

class GetCurrentPriceInput(kabu.GetCurrentPriceInput, my_model.MyModel):
    ticker: str = pydantic.Field( ..., description="""
  		必ず証券コード＋市場サフィックスを指定してください。日本株は .T を付けます
  		例：トヨタの場合は7203.T、NTTの場合は9432.T、IHIの場合は7013.T
  		会社名ではなく証券コードを入力してください。""",
  	)

class GetPriceInput(kabu.GetPriceInput, my_model.MyModel):
	ticker: str = pydantic.Field(..., description="証券コード+市場サフィックス（トヨタの場合は7203.Tなど）")
	begin_range: dt.datetime = pydantic.Field(..., description="分析開始日")
	end_range: dt.datetime = pydantic.Field(..., description="分析終了日")
	chart_granularity: kabu.ChartGranularity = pydantic.Field(..., description="チャートの粒度（日足：0）")

class Tools:
    b = kabu.Backend()

    def get_current_price(self, input: GetCurrentPriceInput) -> str:
      return self.b.get_current_price(input)
    
    # 指定範囲の株価情報をDBから読みだす　DBになければyfから取得する
    def get_price(self, input: GetPriceInput) -> str:
      return self.b.get_price(input).to_json(orient="records", date_format="iso")
    
t = Tools()
# print(t.get_current_price(GetCurrentPriceInput(ticker="9432.T")))
print(t.get_price(GetPriceInput(ticker="7013.T", begin_range=dt.datetime(2025, 4, 1, 0,0,0), end_range=dt.datetime.now(), chart_granularity=kabu.ChartGranularity.DAILY)))