import fdb
from sys import platform
import os

def run():
    microsip_dir = ""
    if platform == "linux" or platform == "linux2" or platform == "darwin":
        microsip_dir = "/Microsip datos/"
    elif platform == "win32":
        microsip_dir = "C:\Microsip datos\\"

    if not os.path.isdir(microsip_dir):
        print("Microsip data not found!")
        exit()

    fdb_files = [f for f in os.listdir(microsip_dir) if f.lower().endswith(".fdb")]

    if len(fdb_files) == 0:
        print("FDB file not found!")
        exit()
    elif len(fdb_files) > 1:
        print("Multiple FDB files found: " + ", ".join(fdb_files))
    else:
        print("Found FDB file: " + fdb_files[0])


    most_recent_fdb_modify_time = 0
    most_recent_fdb_file = ""
    for f in fdb_files:
        full_path = microsip_dir + f
        mtime = os.path.getmtime(full_path)
        if mtime > most_recent_fdb_modify_time:
            most_recent_fdb_modify_time = mtime
            most_recent_fdb_file = f

    if len(fdb_files) > 1:
        print("Choosing most recently modified FDB file: " + most_recent_fdb_file)

    DB_PATH = microsip_dir + most_recent_fdb_file
    DB_USER = "sysdba"
    DB_PASSWORD = "masterkey"

    con = fdb.connect(dsn=DB_PATH, user=DB_USER, password=DB_PASSWORD, charset='UTF8')
    print("Successfully connected")