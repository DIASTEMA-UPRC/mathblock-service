# Import Libraries
import math

def normalised(obj):
    return str(obj).lower()

def utf8len(s):
    return len(s.encode('utf-8'))

# Brakes the Bucket and the Object path of a MinIO bucket
def brake_minio_path(bucket_and_path):
    bucket_and_path_list = bucket_and_path.split("/")
    minio_bucket = bucket_and_path_list[0]
    del bucket_and_path_list[0]
    minio_object_path = "/".join(bucket_and_path_list)+"/"
    return [minio_bucket, minio_object_path]

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

""" Operation Results """

def get_results_of_operation(operation_name, left_data, right_data):
    if operation_name == "addition":
        return addition(left_data, right_data)
    if operation_name == "subtraction":
        return subtraction(left_data, right_data)
    if operation_name == "division":
        return division(left_data, right_data)
    if operation_name == "multiplication":
        return multiplication(left_data, right_data)
    if operation_name == "logarithm":
        return logarithm(left_data, right_data)
    if operation_name == "exponential":
        return exponential(left_data, right_data)

def addition(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if(not isfloat(left_data[i])) or (not isfloat(right_data[i])):
            results_data.append(None)
            continue
        results_data.append(float(left_data[i]) + float(right_data[i]))
    
    return results_data

def subtraction(left_data, right_data):
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
    
    return results_data

def division(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if(not isfloat(left_data[i])) or (not isfloat(right_data[i])):
            results_data.append(None)
            continue
        if(float(right_data[i]) == 0):
            results_data.append(None)
            continue
        results_data.append(float(left_data[i]) / float(right_data[i]))
    
    return results_data

def multiplication(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if(not isfloat(left_data[i])) or (not isfloat(right_data[i])):
            results_data.append(None)
            continue
        results_data.append(float(left_data[i]) * float(right_data[i]))
    
    return results_data

def logarithm(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if(not isfloat(left_data[i])) or (not isfloat(right_data[i])):
            results_data.append(None)
            continue
        if(not (float(left_data[i]) > 0)) or (not (float(right_data[i]) > 0)):
            results_data.append(None)
            continue
        results_data.append(math.log(float(right_data[i]), float(left_data[i])))
    
    return results_data

def power(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if(not isfloat(left_data[i])) or (not isfloat(right_data[i])):
            results_data.append(None)
            continue
        if(float(left_data[i]) == 0) and (float(right_data[i]) == 0):
            results_data.append(None)
            continue
        results_data.append(float(left_data[i]) ** float(right_data[i]))
    
    return results_data