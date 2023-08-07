from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie
from pymongo import MongoClient
import asyncio

from config.CrawlerInstance import crawlerinstance

async def initiate_database():
    # client = AsyncIOMotorClient("mongodb://127.0.0.1:27017")
    client = AsyncIOMotorClient("mongodb+srv://yash23malode:9dtb8MGh5aCZ5KHN@cluster.u0gqrzk.mongodb.net/")
    # client = MongoClient('mongodb://127.0.0.1:27017')
    await init_beanie(database=client['prakat23'], document_models=[crawlerinstance])
    print('inside initiate_database')
