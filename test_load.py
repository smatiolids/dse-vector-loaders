import os
from dotenv import load_dotenv
from conn import getCQLSession
from cassandra.cqlengine.query import BatchStatement
from cassandra.query import BatchType

load_dotenv()
table = os.environ["DSE_TABLE"]

def main():
    session = getCQLSession(os.environ["MODE"])
    print(session)
    query = "SELECT * FROM {table} limit 10"
    result_set = session.execute(query)

    # Iterate over the results and print each row
    for row in result_set:
        print(row)

if __name__ == "__main__":
    main()
