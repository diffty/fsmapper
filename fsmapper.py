import os
import shutil
import hashlib

import sqlite3 as sl
from sqlite3 import Connection


BLOCK_SIZE = 2^16
PATH = "C:/Users/DiFFtY/Downloads/1964_GEPD_Edition"


def get_file_hash(file_path):
    file_hash = None
    hasher = hashlib.md5()

    with open(file_path, "rb") as fp:
        buffer = fp.read(BLOCK_SIZE)

        while len(buffer) > 0:
            hasher.update(buffer)
            buffer = fp.read(BLOCK_SIZE)
            file_hash = hasher.hexdigest()

    return file_hash


def connect_database(db_file_path):
    db = sl.connect(db_file_path)

    response = db.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='file';
    """)

    found_table = response.fetchone()

    if found_table is None:
        db.execute("""
            CREATE TABLE file (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                path TEXT,
                hash TEXT,
                size INTEGER,
                atime REAL,
                mtime REAL,
                ctime REAL
            );
        """)
    
    return db


def add_file_to_database(db: Connection, file_path: str):
    file_fingerprint = get_file_hash(file_path)
    file_infos = os.stat(file_path)
    sql = db.execute(f"""
        INSERT INTO file (path, hash, size, atime, mtime, ctime)
        VALUES (
            "{file_path}",
            "{file_fingerprint}",
            {file_infos.st_size},
            {file_infos.st_atime},
            {file_infos.st_mtime},
            {file_infos.st_ctime}
            )
    """)
    db.commit()


db = connect_database("files.db")

for curr_path, folders, files in os.walk(PATH):
    for f in files:
        add_file_to_database(db, f"{curr_path}/{f}")

db.close()
