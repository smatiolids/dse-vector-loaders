import os
import sys
from dotenv import load_dotenv
from conn import getCQLSession
from cassandra.cqlengine.query import BatchStatement
from cassandra.query import BatchType
import csv
import time
import json
from random import randrange, sample

load_dotenv()

session = getCQLSession(os.environ["MODE"])
table = os.environ["DSE_TABLE"]

cmd_insert = f"""
UPDATE {table} SET metadata = :metadata WHERE id = :id
"""

prepared_stmt_insert = session.prepare(cmd_insert)

def generate_random_dict():
    keys = [f'k{str(i).zfill(2)}' for i in range(1, 100)]
    values = [f'v{str(i).zfill(2)}' for i in range(100)]
    
    random_keys = sample(keys, 20)
    random_values = sample(values, 20)
    
    return {key: value for key, value in zip(random_keys, random_values)}

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
                batch.add(prepared_stmt_insert, {"id": row['id'], "metadata": generate_random_dict()})
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

def get_file(directory_path):
    files_and_dirs = os.listdir(directory_path)
    files = [f for f in files_and_dirs if os.path.isfile(os.path.join(directory_path, f))]
    return len(files), files[randrange(len(files))] if len(files) > 0 else 'NONE'
      

def main():
    directory = sys.argv[1]
    print(f"Loading directory {directory}")
    count = 0

    # Iterate over files in directory
    while True:
        remaining , filename = get_file(directory)
        if remaining == 0:
            break
        count += 1
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            file_path_processing = os.path.join(directory+"/processing", filename) 
            os.rename(file_path, file_path_processing)
            print(f"Loading {filename} ( {count} | {remaining} )")
            start = time.time()
            qty = load_jsonl(file_path_processing, 400)
            end = time.time()
            print(f"Time to load: {end - start} : {qty} records ( {qty / (end - start)} recs/sec)")
            os.rename(file_path_processing, os.path.join(directory+"/processed", filename))
    print("No more files to process")


if __name__ == "__main__":
    main()
