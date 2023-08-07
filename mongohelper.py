import pymongo

CLIENT = 'mongodb://localhost:27017/'
database = 'DATABASE NAME'
collection = 'COLLECTION NAME'


class MongoHelper():
     
    def __init__(self, database_name, collection_name, client=CLIENT):
        self.database_name=database_name
        self.collection_name=collection_name
        self.client=client

        self.mongoclient = pymongo.MongoClient(self.client)
        self.db = self.mongoclient[database_name]
        self.connect =  self.db[self.collection_name]

    def insert(self, data):
        self.connect.insert_one(data)

    def find_data_by_url(self,url):
        return self.connect.find({"url":url})

    def get_collection(self):
        list=[]
        data = self.connect.find()
        for documents in data :
            list.append(documents)
        return list
    