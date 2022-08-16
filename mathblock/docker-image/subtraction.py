# Import custom Libraries
from tools import normalised, utf8len, brake_minio_path, isfloat

# Import custom Classes
from MinIO_Class import MinIO_Class

# Import Libraries
import io

def subtraction(playbook, job, left_bucket, right_bucket):   # THE BUCKETS ARE [bucket, feature]
    # Make the bucket of the operation
    operation_bucket = ""
    if(job["next"] != 0):
        operation_bucket = playbook["output"]+"/subtraction-"+normalised(job["step"])
    else:
        operation_bucket = playbook["output"]

    # Make the MinIO Object
    minio_obj = MinIO_Class()

    # Get the MinIO Bucket and object paths
    minio_bucket = brake_minio_path(operation_bucket)[0]
    minio_object_path = brake_minio_path(operation_bucket)[1]

    # Make the MinIO Operation bucket only if there is other jobs to do as well
    if(job["next"] != 0):
        minio_obj.put_object(minio_bucket, minio_object_path, io.BytesIO(b""), 0,)
    
    # Start computing the operation
    # Build the result file's name and data
    file_name = "operation-"+normalised(job["step"])+"-"+normalised(job["next"])+".csv"

    # Make the operation
    operation(minio_obj, minio_bucket, minio_object_path, file_name, left_bucket, right_bucket)

    # If this is the final operation then remove all the other results from MinIO
    if(job["next"] == 0):
        minio_obj.remove_not_usable_objects(minio_bucket, minio_object_path)

    # Return the output bucket
    return [operation_bucket, "result"]

# Function to handle the subtraction operation
def operation(minio_obj, minio_bucket, minio_object_path, file_name, left_var, right_var):
    # Get input buckets and objects 
    left_bucket = brake_minio_path(left_var[0])[0]
    right_bucket = brake_minio_path(right_var[0])[0]
    left_object = brake_minio_path(left_var[0])[1]
    right_object = brake_minio_path(right_var[0])[1]

    # Get input features
    left_feature = left_var[1]
    right_feature = right_var[1]

    left_data = minio_obj.get_object(left_bucket, left_object, left_feature)
    right_data = minio_obj.get_object(right_bucket, right_object, right_feature)

    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if(not isfloat(left_data[i])) or (not isfloat(right_data[i])):
            results_data.append(None)
            continue
        results_data.append(float(left_data[i]) - float(right_data[i]))
    
    result_text = "result"
    for result in results_data:
        result_text += "\n"+str(result)
    
    result_bytes_length = utf8len(result_text)
    result_bytes = bytes(result_text, encoding='utf-8')
    
    minio_obj.put_object(minio_bucket, minio_object_path + file_name, io.BytesIO(result_bytes), result_bytes_length)

    return