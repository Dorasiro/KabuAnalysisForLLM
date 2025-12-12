import pydantic
import my_model
import kabu

class GetCurrentPriceInput(kabu.GetCurrentPriceInput, my_model.MyModel):
    ticker: str = pydantic.Field( ..., description="""
		必ず証券コード＋市場サフィックスを指定してください。日本株は .T を付けます
		例：トヨタの場合は7203.T、NTTの場合は9432.T、IHIの場合は7013.T
		会社名ではなく証券コードを入力してください。""",
    )

class Tools:
    b = kabu.Backend()

    def get_current_price(self, input: GetCurrentPriceInput) -> str:
        return self.b.get_current_price(input)
    
t = Tools()
print(t.get_current_price(GetCurrentPriceInput(ticker="9432.T")))