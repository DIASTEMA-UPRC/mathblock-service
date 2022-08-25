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

def isBool(variable):
    if (type(variable) == bool):
        return True
    else:
        return False

def toBool(variable):
    if (variable.lower() == "true"):
        return True
    if (variable.lower() == "false"):
        return False

def canBeBool(variable):
    if (variable.lower() == "true"):
        return True
    if (variable.lower() == "false"):
        return True
    return False

""" Operation Results """

def get_results_of_operation(operation_name, left_data, right_data):
    """ ARITHMETIC """
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
    if operation_name == "power":
        return power(left_data, right_data)
    
    """ LOGICAL """
    if operation_name == "logical_and":
        return logical_and(left_data, right_data)
    if operation_name == "logical_or":
        return logical_or(left_data, right_data)
    if operation_name == "logical_not":
        return logical_not(left_data)
    
    """ COMPARISON """
    if operation_name == "equal":
        return equal(left_data, right_data)
    if operation_name == "not_equal":
        return not_equal(left_data, right_data)
    if operation_name == "less_or_equal":
        return less_or_equal(left_data, right_data)
    if operation_name == "greater_or_equal":
        return greater_or_equal(left_data, right_data)
    if operation_name == "greater_than":
        return greater_than(left_data, right_data)
    if operation_name == "less_than":
        return less_than(left_data, right_data)

    """ STATISTICAL """
    if operation_name == "mean_value":
        return mean_value(left_data)
    if operation_name == "variance_value":
        return variance_value(left_data)
    if operation_name == "amount_of":
        return amount_of(left_data, right_data)

""" ARITHMETIC """
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
        if(float(left_data[i]) == 1):
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

""" LOGICAL """
def logical_and(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if(not canBeBool(left_data[i])) or (not canBeBool(right_data[i])):
            results_data.append(None)
            continue
        results_data.append(toBool(left_data[i]) and toBool(right_data[i]))
    
    return results_data

def logical_or(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if(not canBeBool(left_data[i])) or (not canBeBool(right_data[i])):
            results_data.append(None)
            continue
        results_data.append(toBool(left_data[i]) or toBool(right_data[i]))
    
    return results_data

def logical_not(input_data):
    results_data = []

    for i in range(len(input_data)):
        if(not canBeBool(input_data[i])):
            results_data.append(None)
            continue
        results_data.append(not (toBool(input_data[i])))
    
    return results_data

""" COMPARISON """
def equal(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if((left_data[i] == "None") or (right_data[i] == "None")):
            results_data.append(None)
            continue
        if((left_data[i] == "") or (right_data[i] == "")):
            results_data.append(None)
            continue
        results_data.append(left_data[i] == right_data[i])
    
    return results_data

def not_equal(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if((left_data[i] == "None") or (right_data[i] == "None")):
            results_data.append(None)
            continue
        if((left_data[i] == "") or (right_data[i] == "")):
            results_data.append(None)
            continue
        results_data.append(left_data[i] != right_data[i])
    
    return results_data

def less_or_equal(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if((left_data[i] == "None") or (right_data[i] == "None")):
            results_data.append(None)
            continue
        if((left_data[i] == "") or (right_data[i] == "")):
            results_data.append(None)
            continue
        results_data.append(left_data[i] <= right_data[i])
    
    return results_data

def greater_or_equal(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if((left_data[i] == "None") or (right_data[i] == "None")):
            results_data.append(None)
            continue
        if((left_data[i] == "") or (right_data[i] == "")):
            results_data.append(None)
            continue
        results_data.append(left_data[i] >= right_data[i])
    
    return results_data

def greater_than(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if((left_data[i] == "None") or (right_data[i] == "None")):
            results_data.append(None)
            continue
        if((left_data[i] == "") or (right_data[i] == "")):
            results_data.append(None)
            continue
        results_data.append(left_data[i] > right_data[i])
    
    return results_data

def less_than(left_data, right_data):
    results_data = []

    max_len = max(len(left_data), len(right_data))
    for i in range(max_len):
        if(i >= len(left_data) or i >= len(right_data)):
            results_data.append(None)
            continue
        if((left_data[i] == "None") or (right_data[i] == "None")):
            results_data.append(None)
            continue
        if((left_data[i] == "") or (right_data[i] == "")):
            results_data.append(None)
            continue
        results_data.append(left_data[i] < right_data[i])
    
    return results_data

""" STATISTICAL """
def mean_value(input_data):
    if len(input_data) <= 0:
        return [None]
    
    sum_of_data = 0
    for i in range(len(input_data)):
        if(not isfloat(input_data[i])):
            return [None]
        sum_of_data += float(input_data[i])
        
    return [sum_of_data / len(input_data)]

def variance_value(input_data):
    if len(input_data) <= 1:
        return [None]
    
    # Get mean value
    sum_of_data = 0
    for i in range(len(input_data)):
        if(not isfloat(input_data[i])):
            return [None]
        sum_of_data += float(input_data[i])
    mean_stat_val = sum_of_data / len(input_data)

    sum_of_data = 0
    for i in range(len(input_data)):
        sum_of_data += (float(input_data[i]) - mean_stat_val) ** 2
        
    return [sum_of_data / (len(input_data) - 1)]

def amount_of(left_data, right_data):
    if len(left_data) <= 0:
        return [None]

    amount = 0
    for i in range(len(left_data)):
        if(not canBeBool(left_data[i])) or (not canBeBool(right_data[i])):
            return [None]
        if((left_data[i] == "None") or (right_data[i] == "None")):
            return [None]
        if((left_data[i] == "") or (right_data[i] == "")):
            return [None]
        if(toBool(left_data[i]) == toBool(right_data[i])):
            amount += 1
    
    return [amount]