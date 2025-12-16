import inspect
from typing import overload
import datetime as dt
from pydantic import BaseModel, Field, ValidationError
import pandas

class Logging:
    # 自作ログの出力先
    KABU_LOG_FILE = "kabu-log.txt"
    IS_LOGGING: bool = True

    # ログを作りたくない場合はファイル名を入れなくてよい
    def __init__(self, logFileName = "", isLogging: bool = True):
        if not logFileName:
            self.IS_LOGGING = False
            return
        
        self.KABU_LOG_FILE = logFileName
        self.IS_LOGGING = isLogging

    # 所定のログファイルにログを追記する
    def append_to_log_file(self, message: str) -> None:
        if not self.IS_LOGGING or not message:
            return

        with open(self.KABU_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(message + "\n")

    @overload
    def append_to_log_file_from_bm(bm: BaseModel) -> None: ...
    @overload
    def append_to_log_file_from_bm(bm: BaseModel, message: str) -> None: ...

    def append_to_log_file_from_bm(self, bm: BaseModel, message: str | None = None) -> None:
        if not self.IS_LOGGING:
            return
        
        line = f"{dt.datetime.now()} | {bm.__class__.__name__} \n" + str(bm.model_dump())

        with open(self.KABU_LOG_FILE, "a", encoding="utf-8") as f:
            if not message:
                f.write(line + "\n")
            else:
                f.write(line + " | " + message + "\n")

    @overload
    def append_to_log_file_from_dict(payload: dict) -> None: ...
    @overload
    def append_to_log_file_from_dict(payload: dict, message: str) -> None: ...

    def append_to_log_file_from_dict(self, payload: dict, message: str | None = None) -> None:
        if not self.IS_LOGGING:
            return
        
        def_name = inspect.currentframe().f_back.f_code.co_name

        if not message:
            line = f"{dt.datetime.now()} | {def_name} \n"
        else:
            line = f"{dt.datetime.now()} | {def_name}  | {message}\n"

        line += str(payload)

        with open(self.KABU_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    @overload
    def append_to_log_file_from_df(df: pandas.DataFrame) -> None: ...
    @overload
    def append_to_log_file_from_df(df: pandas.DataFrame, message: str) -> None: ...

    def append_to_log_file_from_df(self, df: pandas.DataFrame, message: str | None = None) -> None:
        if not self.IS_LOGGING:
            return
        
        def_name = inspect.currentframe().f_back.f_code.co_name

        if not message:
            line = f"{dt.datetime.now()} | {def_name} \n"
        else:
            line = f"{dt.datetime.now()} | {def_name}  | {message}\n"

        # dfの大きさが6以下の場合は全行表示
        if len(df) <= 6:
            line += df.to_string()
        # dfの大きさが6を超える場合は最初の３行と最後の３行を表示
        else:
            line += df.head(3).to_string() + "\n"
            line += "(～中略～)\n"
            line += df.tail(3).to_string()

        line += "\n" + f"({len(df)} rows)\n"

        with open(self.KABU_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")