# Import custom Libraries
from normalizing import normalised

# Import custom Classes
from MinIO_Class import MinIO_Class

# Import Libraries
import io

def subtraction(playbook, job, last_bucket_1, last_bucket_2):
    # Make the bucket of the operation
    operation_bucket = ""
    if(job["next"] != 0):
        operation_bucket = playbook["output"]+"/subtraction-"+normalised(job["step"])
    else:
        operation_bucket = playbook["output"]

    # Make the MinIO Object
    minio_obj = MinIO_Class()

    # Get the MinIO Bucket and object paths
    operation_bucket_list = operation_bucket.split("/")
    minio_bucket = operation_bucket_list[0]
    del operation_bucket_list[0]
    minio_object_path = "/".join(operation_bucket_list)+"/"

    # Make the MinIO Operation bucket only if there is other jobs to do as well
    if(job["next"] != 0):
        minio_obj.put_object(minio_bucket, minio_object_path, io.BytesIO(b""), 0,)
    
    # Make the operation ################################


    # ################################ THIS IS FILE ADDED AS A TEST ################################
    file_name = "operation-"+normalised(job["step"])+"-"+normalised(job["next"])+".csv"
    minio_obj.put_object(minio_bucket, minio_object_path + file_name, io.BytesIO(b"results"), 7)

    # If this is the final operation then remove all the other results from MinIO
    if(job["next"] == 0):
        minio_obj.remove_not_usable_objects(minio_bucket, minio_object_path)

    # Return the output bucket
    return operation_bucket