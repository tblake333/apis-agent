import os
from sys import platform

def get_microsip_fdb_file_path() -> str:
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
        
        return microsip_dir + most_recent_fdb_file
