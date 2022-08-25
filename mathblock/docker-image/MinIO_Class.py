# Import Libraries
import os
from minio import Minio
import io

class MinIO_Class:
    MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
    MINIO_PORT = int(os.getenv("MINIO_PORT", 9000))
    MINIO_USER = os.getenv("MINIO_USER", "diastema")
    MINIO_PASS = os.getenv("MINIO_PASS", "diastema")

    def __init__(self):
        minio_host = MinIO_Class.MINIO_HOST+":"+str(MinIO_Class.MINIO_PORT)
        self.minio_client = Minio(
            minio_host,
            access_key=MinIO_Class.MINIO_USER,
            secret_key=MinIO_Class.MINIO_PASS,
            secure=False
        )
        return
    
    def put_object(self, bucket, object_as_path, bytes_input, size):
        self.minio_client.put_object(bucket, object_as_path, bytes_input, size)
        return

    def remove_object(self, bucket, object_as_path):
        self.minio_client.remove_object(bucket, object_as_path)
        return
    
    def make_bucket(self, bucket):
        if self.minio_client.bucket_exists(bucket):
            pass
        else:
            self.minio_client.make_bucket(bucket)
        return
    
    # Used to remove all the objects that are not ending with *-0.csv of a path in a MinIO bucket 
    def remove_not_usable_objects(self, bucket, object_path):
        objects = self.minio_client.list_objects(bucket, prefix=object_path, recursive=True)
        for obj in objects:
            if(not obj.object_name.endswith("-0.csv")):
                self.minio_client.remove_object(bucket, obj.object_name)
        pass
    
    # Get all the values of a feature as a list
    def get_object(self, input_bucket, file_path, feature):
        values = []

        objects = self.minio_client.list_objects(input_bucket, prefix=file_path, recursive=True)
        for obj in objects:
            data = self.minio_client.get_object(input_bucket, obj.object_name).read()
            data = data.decode("utf-8")
            data = data.split("\n")
            if '' in data:
                data.remove('')
            if len(data) == 0:
                continue
            for i in range(len(data)):
                data[i] = data[i].split(",")
            position = -1
            for i in range(len(data[0])):
                if(data[0][i] == feature):
                    position = i
            del data[0]
            for i in range(len(data)):
                data[i] = data[i][position]
                data[i] = data[i].replace("\r", "")
                if(data[i] == ""):
                    data[i] = "None"
            values.extend(data)
        return values
