import os
import shutil

def archive_files():
    raw_path = "data/raw/"
    archive_path = "data/archive/"

    os.makedirs(archive_path, exist_ok=True)

    for file in os.listdir(raw_path):
        shutil.move(os.path.join(raw_path, file), archive_path)

    print("Files archived successfully")
