from time import time
import aioodbc
import asyncio
import uuid


def create_query():
    query = "insert into asset_asset values "
    for i in range(999):
        inputs = [uuid.uuid4(), 'Ambev', 'ABEV3', 1, 2, 3]
        query += "('{}', '{}', '{}', {}, {}, {}),".format(*inputs)

    inputs = [uuid.uuid4(), 'Ambev', 'ABEV3', 1, 2, 3]
    query += "('{}', '{}', '{}', {}, {}, {});".format(*inputs)

    return query


async def send_to_db(conn_str, loop, query, i):
    conn = await aioodbc.connect(dsn=conn_str, loop=loop, autocommit=True)
    cur = await conn.cursor()
    print("Writing #{}".format(i))
    await asyncio.sleep(3)
    await cur.execute(query)
    print("Done #{}".format(i))
    await cur.close()
    await conn.close()


async def main(loop):

    conn_str = (
        "DRIVER={Postgres};"
        "DATABASE=aiodb;"
        "UID=postgres;"
        "PWD=mikaju38;"
        "SERVER=localhost;"
        "PORT=5432;"
        )

    # conn_str = "Driver=SQLite3;Database=app/db.sqlite3"

    n_concurrent = 64
    dltasks = set()
    i = 0

    while i < 20000:
        if len(dltasks) >= n_concurrent:
            _done, dltasks = await asyncio.wait(dltasks, return_when=asyncio.FIRST_COMPLETED)
        dltasks.add(loop.create_task(send_to_db(conn_str, loop, create_query(), i)))
        i += 1
    await asyncio.wait(dltasks)


if __name__ == "__main__":
    # t0 = time()
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main(loop))
    # loop.close()
    # print(time() - t0)
    #
    try:
        t0 = time()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(loop))
        # ;loop.close()
    except Exception as e:
        print(e)
    finally:
        print(time() - t0)
