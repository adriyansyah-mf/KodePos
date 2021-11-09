import asyncio
import json
import httpx
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Unicode, Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("sqlite+aiosqlite:///20211109_sqla.sqlite")
# engine = create_async_engine("mysql+aiomysql://root:root@b1:3306/kodepos")
# engine = create_async_engine("postgresql+asyncpg://postgres:postgres@b1:5432/kodepos")
meta = MetaData()

KodePos = Table(
    "kode_pos", meta,
    Column("id", Integer, primary_key=True),
    Column("provinsi", Unicode(length=100), nullable=False),
    Column("kota", Unicode(length=100), nullable=False),
    Column("kecamatan", Unicode(length=100), nullable=False),
    Column("kelurahan", Unicode(length=100), nullable=False),
    Column("kode_pos", Unicode(length=5), nullable=False, unique=False),
)


async def main():
    url = "https://kodeposku.com/kodepos.json"
    response = httpx.get(url)
    data = json.loads(response.read())
        
    async with engine.begin() as conn:
        await conn.run_sync(meta.drop_all)
        await conn.run_sync(meta.create_all)
   
    async with engine.begin() as conn:
        for x in range(81265):
            x+=1
            query = KodePos.insert().values(dict(
                kode_pos=data[x]['postal_code'],
                provinsi=data[x]['province'],
                kota=data[x]['city'],
                kecamatan=data[x]['district'],
                kelurahan=data[x]['urban'],
            ))
            await conn.execute(query)

        # query = KodePos.update().values(dict(
        #     kelurahan="Semut"
        # )).where(KodePos.c.kode_pos == "12345")
        # await conn.execute(query)

        # query = select(
        #     KodePos.c.id, KodePos.c.kode_pos
        # ).select_from(KodePos).where(KodePos.c.kode_pos == "12345")
        # results = await conn.execute(query)
        # for row in results:
        #     print(row)
        #     print(row.id, row.kode_pos)
        #     print(row._mapping)
        #     print(type(row._mapping))
        #     print(type(row._asdict()))

        # query = KodePos.delete().where(KodePos.c.kode_pos == "12345")
        # await conn.execute(query)

        await conn.commit()

    await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
