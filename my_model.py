import pydantic

# 全てのフィールドにdescriptionが必須なBaseModel
class MyModel(pydantic.BaseModel):
    @pydantic.model_validator(mode="after")
    def check_descriptions(self):
        for name, field in self.__class__.model_fields.items():
            if field.description is None:
                raise ValueError(f"フィールド： '{name}'  にdescriptionが登録されていません。")
        return self