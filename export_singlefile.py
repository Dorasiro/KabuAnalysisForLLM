# build_singlefile.py
from pathlib import Path
import re

FILES = [
    "my_logging.py",
    "my_model.py",
    "database.py",
    "kabu.py",
    "tools.py",
]

OUTPUT = Path("build/tools_singlefile.py")
OUTPUT.parent.mkdir(exist_ok=True)

REMOVE_IMPORTS = [
    r"from my_model import",
    r"from my_logging import",
    r"from database import",
    r"import kabu",
]

def should_remove(line: str) -> bool:
    return any(re.search(pattern, line) for pattern in REMOVE_IMPORTS)

def fix_line(line: str) -> str:
    # kabu. を削除（クラス継承や関数呼び出しに対応）
    line = line.replace("kabu.", "")
    return line

with OUTPUT.open("w", encoding="utf-8") as out:
    out.write("# AUTO-GENERATED FILE FOR OWU\n\n")

    for file in FILES:
        path = Path(file)
        out.write(f"# ===== {file} =====\n")

        for line in path.read_text(encoding="utf-8").splitlines():
            if should_remove(line):
                continue
            out.write(fix_line(line) + "\n")

        out.write("\n\n")