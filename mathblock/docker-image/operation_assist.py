# Import custom Libraries
from tools import normalised, utf8len, brake_minio_path, isfloat, get_results_of_operation

# Import custom Classes
from MinIO_Class import MinIO_Class

# Import Libraries
import io

"""
This program is used to assist all the operations of the service.
"""

def operation_with_two_inputs(playbook, job, left_bucket, right_bucket, operation_name):
    # Get the bucket path for a new operation
    operation_bucket = ""
    if(job["next"] != 0):
        operation_bucket = playbook["output"]+"/"+operation_name+"-"+normalised(job["step"])
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
    two_input_operation_exec(minio_obj, minio_bucket, minio_object_path, file_name, left_bucket, right_bucket, operation_name)

    # If this is the final operation then remove all the other results from MinIO
    if(job["next"] == 0):
        minio_obj.remove_not_usable_objects(minio_bucket, minio_object_path)

    return [operation_bucket, "result"]

    # Function to handle the operation
def two_input_operation_exec(minio_obj, minio_bucket, minio_object_path, file_name, left_var, right_var, operation_name):
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

    # Execute the operation
    results_data = get_results_of_operation(operation_name, left_data, right_data)
    
    # Write the results
    result_text = "result"
    for result in results_data:
        result_text += "\n"+str(result)
    
    result_bytes_length = utf8len(result_text)
    result_bytes = bytes(result_text, encoding='utf-8')
    
    # Write the MinIO result object
    minio_obj.put_object(minio_bucket, minio_object_path + file_name, io.BytesIO(result_bytes), result_bytes_length)

    return