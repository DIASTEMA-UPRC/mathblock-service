# Import custom Libraries
from tools import normalised, utf8len, brake_minio_path, get_results_of_operation, isBool

# Import custom Classes
from MinIO_Class import MinIO_Class

# Import Libraries
import io

def value_builder(playbook, job, given_value):
    # Get the bucket path for the value
    output_bucket = ""
    if(job["next"] != 0):
        output_bucket = playbook["output"]+"/value-"+normalised(job["step"])
    else:
        output_bucket = playbook["output"]
    
    # Make the MinIO Object
    minio_obj = MinIO_Class()

    # Get the MinIO Bucket and object paths
    minio_bucket = brake_minio_path(output_bucket)[0]
    minio_object_path = brake_minio_path(output_bucket)[1]

    # Make the MinIO value bucket only if there is other jobs to do as well
    if(job["next"] != 0):
        minio_obj.put_object(minio_bucket, minio_object_path, io.BytesIO(b""), 0,)
    
    # Build the result file's name and data
    file_name = "operation-"+normalised(job["step"])+"-"+normalised(job["next"])+".csv"

    results_data = [given_value]
    # Write the value
    result_text = "result"
    for result in results_data:
        result_text += "\n"+str(result)
    
    result_bytes_length = utf8len(result_text)
    result_bytes = bytes(result_text, encoding='utf-8')
    
    # Write the MinIO result object
    minio_obj.put_object(minio_bucket, minio_object_path + file_name, io.BytesIO(result_bytes), result_bytes_length)

    # If this is the final operation then remove all the other results from MinIO
    if(job["next"] == 0):
        minio_obj.remove_not_usable_objects(minio_bucket, minio_object_path)

    return [output_bucket, "result", "value"]