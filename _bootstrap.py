import pathlib, base64, sys
data = sys.stdin.read()
pathlib.Path(r"C:\dev\claude-code\eps-momentum-usacktest_top5.py").write_text(data, encoding="utf-8")
print("Written", len(data), "chars")
