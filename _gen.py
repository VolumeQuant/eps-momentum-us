import pathlib
import sqlite3
import sys
from collections import defaultdict

def generate():
    Q = chr(39)
    DQ = chr(34)
    NL = chr(10)
    lines = []
    a = lines.append
    # Will build the script
    a("import sqlite3")
    a("import sys")
    a("from collections import defaultdict")
    content = NL.join(lines)
    print(content)

generate()
