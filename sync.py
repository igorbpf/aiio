from time import time, sleep
import pyodbc
import uuid

def create_query():
    query = "insert into asset_asset values "
    for i in range(999):
        inputs = [uuid.uuid4(), 'Ambev', 'ABEV3', 1, 2, 3]
        query += "('{}', '{}', '{}', {}, {}, {}),".format(*inputs)

    inputs = [uuid.uuid4(), 'Ambev', 'ABEV3', 1, 2, 3]
    query += "('{}', '{}', '{}', {}, {}, {});".format(*inputs)

    return query


def send_to_db(cur, query, i):
    print("Writing #{}".format(i))
    sleep(3)
    cur.execute(query)
    print("Done #{}".format(i))


def main():

    conn_str = (
        "DRIVER={Postgres};"
        "DATABASE=aiodb;"
        "UID=postgres;"
        "PWD=mikaju38;"
        "SERVER=localhost;"
        "PORT=5432;"
        )


    conn = pyodbc.connect(conn_str, autocommit=True)

    cur = conn.cursor()

    for i in range(1000):
        query = create_query()
        send_to_db(cur, query, i)

    cur.close()
    conn.close()


if __name__ == "__main__":
    t0 = time()
    main()
    print(time() - t0)
    # try:
    #     t0 = time()
    #     main()
    # except Exception as e:
    #     print(e)
    # finally:
    #     print(time() - t0)
