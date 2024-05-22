import os
from dotenv import load_dotenv
from conn import getCQLSession
from cassandra.cqlengine.query import BatchStatement
from cassandra.query import BatchType

load_dotenv()

def main():
    session = getCQLSession(os.environ["MODE"])
    print(session)

if __name__ == "__main__":
    main()
