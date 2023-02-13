import os
from pymongo import MongoClient

class MongoDB_Class:

    """ Environment Variables """
    # Mongo Host and Port
    MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
    MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))

    # Mongo Database and Collection
    DATABASE = os.getenv("DATABASE", "rules_db")
    COLLECTION = os.getenv("COLLECTION", "jobs")

    def __init__(self):
        mongo_host = MongoDB_Class.MONGO_HOST+":"+str(MongoDB_Class.MONGO_PORT)
        self.mongo_client = MongoClient("mongodb://"+mongo_host+"/")
        return
    
    def insertMongoRecord(self, record):
        mongo_db = self.mongo_client[MongoDB_Class.DATABASE]
        db_collection = mongo_db[MongoDB_Class.COLLECTION]
        db_collection.insert_one(record)
        return
    
    def updateMongoStatus(self, filters, status):
        mongo_db = self.mongo_client[MongoDB_Class.DATABASE]
        db_collection = mongo_db[MongoDB_Class.COLLECTION]
        db_collection.update_one(filters, {"$set": {'status': status}})
        return

    def findMongoDocument(self, job_id):
        mongo_db = self.mongo_client[MongoDB_Class.DATABASE]
        db_collection = mongo_db[MongoDB_Class.COLLECTION]
        return db_collection.find_one({"job-id": job_id})
    
    def updateMongoPerformanceMetrics(self, client, collection, filters, metadata):
        mongo_db = self.mongo_client[client]
        analysis_collection = mongo_db[collection]

        # Get the current metadata
        metadata_dict = analysis_collection.find_one(filters)
        if "performance" in metadata_dict:
            metadata_dict = metadata_dict["performance"]
        else:
            metadata_dict = {}

        metadata_dict[metadata["label"]] = metadata["value"]
            
        # Update the metadata
        analysis_collection.update_one(filters, {"$set": {'performance': metadata_dict}})
        return
