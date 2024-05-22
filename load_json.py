import os
import sys
from dotenv import load_dotenv
from conn import getCQLSession
from cassandra.cqlengine.query import BatchStatement
from cassandra.query import BatchType
import csv
import json
import time

load_dotenv()

session = getCQLSession(os.environ["MODE"])
table = os.environ["DSE_TABLE"]

cmd_insert = f"""
INSERT INTO {table} (id, title, emb)
VALUES (:id, :title, :emb)
"""

prepared_stmt_insert = session.prepare(cmd_insert)


def load_jsonl(file_path, batch_size=50, skip =0):
    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    count = 0
    start = time.time()
    end = time.time()
    print(f"Starting")
    with open(file_path, 'r') as file:
        print(f"Skiping")
        for _ in range(skip):
            next(file)
        count = skip
        print(f"Skipped {count} records")
            
        for line in file:
            try:
                count += 1
                row = json.loads(line.strip())
                batch.add(prepared_stmt_insert, {"id": row['id'], "title": row['title'], "emb": row["titleVector"]})
                if count % batch_size == 0:
                    rs = session.execute(batch)
                    batch.clear()

#                if count % 1000 == 0:
#                    print(f"""{count} records inserted.""")

                if count % 10000 == 0:
                    end = time.time()
                    print(f"Time to load: {end - start} : {count} records ( {10000 / (end - start)} recs/sec)")
                    start = time.time()

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in line: {e}")
    
        rs = session.execute(batch)
        batch.clear()
    return count




def main():
    filepath = sys.argv[1]
    skip  = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    print(f"Loading file {filepath}")
    print(f"Skipping first {skip} lines")
    start = time.time()
    count = load_jsonl(filepath, 100, skip)
    end = time.time()
    print(f"Time to load: {end - start} : {count} records")


if __name__ == "__main__":
    main()
