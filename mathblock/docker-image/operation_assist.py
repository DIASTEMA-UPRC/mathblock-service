# Import custom Libraries
from tools import normalised, utf8len, brake_minio_path, get_results_of_operation, isBool

# Import custom Classes
from MinIO_Class import MinIO_Class

# Import Libraries
import io

"""
This program is used to assist all the operations of the service.
"""

def operation_handler(playbook, job, left_bucket, right_bucket, operation_name):
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
    result_kind = operation_executor(minio_obj, minio_bucket, minio_object_path, file_name, left_bucket, right_bucket, operation_name)
    # The above operation will tell you what list of [bucket, result/mean/variance, value] to return

    # If this is the final operation then remove all the other results from MinIO
    if(job["next"] == 0):
        minio_obj.remove_not_usable_objects(minio_bucket, minio_object_path)

    return [operation_bucket, "result", result_kind]

def operation_executor(minio_obj, minio_bucket, minio_object_path, file_name, left_var, right_var, operation_name):
    # Get input buckets and objects 
    left_bucket = brake_minio_path(left_var[0])[0]
    left_object = brake_minio_path(left_var[0])[1]
    right_bucket = ""
    right_object = ""

    if(not isBool(right_var)):
        right_bucket = brake_minio_path(right_var[0])[0]
        right_object = brake_minio_path(right_var[0])[1]
    
    # Get input features
    left_feature = left_var[1]
    right_feature = ""

    if(not isBool(right_var)):
        right_feature = right_var[1]
    
    # get the values for computing (If they are not given by the user as constant values)
    left_data = minio_obj.get_object(left_bucket, left_object, left_feature)
    right_data = []

    if(not isBool(right_var)):
        right_data = minio_obj.get_object(right_bucket, right_object, right_feature)
    
    # Normalize datasets based on the value of column attribute of each one
    value_output = False
    if(not isBool(right_var)):
        value_output = normalize_datasets(left_data, right_data, left_var[2], right_var[2])
    else:
        value_output = normalize_dataset(left_data, left_var[2])
    
    results_data = get_results_of_operation(operation_name, left_data, right_data)

    # Write the results
    result_text = "result"
    for result in results_data:
        result_text += "\n"+str(result)
    
    result_bytes_length = utf8len(result_text)
    result_bytes = bytes(result_text, encoding='utf-8')
    
    # Write the MinIO result object
    minio_obj.put_object(minio_bucket, minio_object_path + file_name, io.BytesIO(result_bytes), result_bytes_length)

    # Find the operation's output kind
    result_kind = "column"
    if((operation_name == "mean_value") or
            (operation_name == "variance_value") or
            (operation_name == "amount_of")):
        result_kind = "value"
    
    if value_output:
        result_kind = "value"

    return result_kind

def normalize_datasets(left_data, right_data, left_kind, right_kind):
    # If both inputs are values then return that a value output is True
    if(left_kind == "value" and right_kind == "value"):
        return True
    
    # If there is no value input then do nothing and return that a value output is False
    if(left_kind != "value" and right_kind != "value"):
        return False
    
    # Below only one of the two is a value. So find it and make its length like the second one
    # Then return that ther is no value output (False)
    if(left_kind == "value"):
        value = left_data[0]
        data_length = len(right_data)
        for i in range(data_length-1):
            left_data.append(value)
        return False
    
    if(right_kind == "value"):
        value = right_data[0]
        data_length = len(left_data)
        for i in range(data_length-1):
            right_data.append(value)
        return False

def normalize_dataset(input_data, input_kind):
    # If input is a value then return that a value output is True
    if(input_kind == "value"):
        return True
    else:
        return False