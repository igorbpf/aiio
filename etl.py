import asyncio
import pandas as pd
import pyodbc
import aioodbc
import uuid
import time

# def create_connection(conn_string):
#     return await odbc.connect(conn_string)


def create_query(base, frame):

    query = "insert into {} values ".format(base);
    rows = frame.shape[0]

    for i, row in frame.iterrows():
        if i < (rows - 1):
            query += "('{}', '{}', '{}', {}, {}, {}),".format(*row.values.tolist())
        else:
            query += "('{}', '{}', '{}', {}, {}, {});".format(*row.values.tolist())
    return query


async def send_to_db(conn_string, loop, query, i):
    conn = await aioodbc.connect(dsn=conn_string, loop=loop, autocommit=True)
    cur = await conn.cursor()
    print("Writing #{}".format(i))
    # await asyncio.sleep(3)
    await cur.execute(query)
    print("Done #{}".format(i))
    await cur.close()
    await conn.close()


async def main(loop, conn):

    conn_str_2 = (
        "DRIVER={Postgres2};"
        "DATABASE=fakehadoop;"
        "UID=postgres;"
        "PWD=mikaju38;"
        "SERVER=localhost;"
        "PORT=5433;"
        )

    # conn = create_connection(conn_str_1)
    # conn = await aioodbc.connect(dsn=conn_str_1, loop=loop, autocommit=True)

    base = "asset_asset"

    query = "select * from {} limit 5000000;".format(base)

    print("Reading db")
    data_gen = pd.read_sql(query, con=conn, chunksize=1000)

    max_connections = 99
    dltasks = set()

    for i, frame in enumerate(data_gen):
        if len(dltasks) >= max_connections:
            query = create_query(base, frame)
            _done, dltasks = await asyncio.wait(dltasks, return_when=asyncio.FIRST_COMPLETED)
        dltasks.add(loop.create_task(send_to_db(conn_str_2, loop, create_query(base, frame), i)))

    await asyncio.wait(dltasks)

if __name__ == '__main__':
    try:
        t0 = time.time()
        conn_str_1 = (
            "DRIVER={Postgres};"
            "DATABASE=aiodb;"
            "UID=postgres;"
            "PWD=mikaju38;"
            "SERVER=localhost;"
            "PORT=5432;"
            )
        conn = pyodbc.connect(conn_str_1)
        print("Connection createad")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(loop, conn))
        conn.close()
    except Exception as e:
        print(e)
    finally:
        print(time.time() - t0)
