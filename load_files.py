import os
import sys
from dotenv import load_dotenv
from conn import getCQLSession
from cassandra.cqlengine.query import BatchStatement
from cassandra.query import BatchType
import csv
import time
import json


load_dotenv()

session = getCQLSession(os.environ["MODE"])
table = os.environ["DSE_TABLE"]

cmd_insert = f"""
INSERT INTO {table} (id, title, emb)
VALUES (:id, :title, :emb)
"""

prepared_stmt_insert = session.prepare(cmd_insert)

def load_jsonl(file_path, batch_size=25):
    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    count = 0
    start = time.time()
    end = time.time()
    with open(file_path, 'r') as file:
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


def read_csv(file_path):
    data = []
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data.append(row)
    return data


def save_data(data, batch_size=20):
    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    count = 0
    for row in data:
        batch.add(prepared_stmt_insert, {"id": row['asin'], "title": row['title'], "emb": [
                  float(x) for x in row['emb'].split(',')]})
        count += 1
        if count % batch_size == 0:
            rs = session.execute(batch)
            batch.clear()

        if count % 1000 == 0:
            print(f"""{count} records inserted.""")

    rs = session.execute(batch)
    batch.clear()

    return count

def main():
    directory = sys.argv[1]
    print(f"Loading directory {directory}")
    files = os.listdir(directory)
    count = 0

    # Iterate over files in directory
    for filename in files:
        count += 1
        file_path = os.path.join(directory, filename)
        print(f"Loading {filename} ( {count}/{len(files)} )")
        start = time.time()
        qty = load_jsonl(file_path, 400)
        end = time.time()
        print(f"Time to load: {end - start} : {qty} records ( {qty / (end - start)} recs/sec)")
        os.rename(file_path, os.path.join(directory+"/processed", filename))


if __name__ == "__main__":
    main()
