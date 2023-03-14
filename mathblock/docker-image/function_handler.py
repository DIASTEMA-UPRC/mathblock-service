# Import custom Classes
from MongoDB_Class import MongoDB_Class
from MinIO_Class import MinIO_Class

# Import custom Functions for jobs
from operations import subtraction, addition, division, multiplication, logarithm, power
from operations import logical_and, logical_or, logical_not
from operations import equal, not_equal, less_or_equal, greater_or_equal, greater_than, less_than
from operations import mean_value, variance_value, amount_of
from value_assist import value_builder

# Import Python libraries
import time
import psutil
import os

""" Global Variables """
# A dictionary containing information about the bucket and the value of each argument
input_buckets = {}

""" Functions used for the json handling """
# Request a job
def job_requestor(job_json, jobs_anwers_dict, playbook):
    kind = job_json["info"]["kind"]
    step = job_json["step"]
    from_step = job_json["from"]

    # If argument then only save its bucket
    if(kind == "arg"):
        print("[INFO] Argument Found.")

        # Get the ID of the argument
        arg_id = job_json["info"]["arg_id"]

        # Check if arg is value or feature
        if input_buckets[arg_id][0] == "value":
            jobs_anwers_dict[step] = value_builder(playbook, job_json, input_buckets[arg_id][1])
        else:
            jobs_anwers_dict[step] = [input_buckets[arg_id][0], input_buckets[arg_id][1], "column"]
    
    # If operation then find operation and then execute it
    if(kind == "operation"):
        name = job_json["info"]["name"]

        """ ARITHMETIC """
        if(name == "addition"):
            print("[INFO] Addition Found.")
            jobs_anwers_dict[step] = addition(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "subtraction"):
            print("[INFO] Subtraction Found.")
            jobs_anwers_dict[step] = subtraction(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "division"):
            print("[INFO] Division Found.")
            jobs_anwers_dict[step] = division(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "multiplication"):
            print("[INFO] Multiplication Found.")
            jobs_anwers_dict[step] = multiplication(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "logarithm"):
            print("[INFO] Logarithm Found.")
            jobs_anwers_dict[step] = logarithm(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "power"):
            print("[INFO] Power Found.")
            jobs_anwers_dict[step] = power(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        """ LOGICAL """
        if(name == "logical_and"):
            print("[INFO] Logical_And Found.")
            jobs_anwers_dict[step] = logical_and(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "logical_or"):
            print("[INFO] Logical_Or Found.")
            jobs_anwers_dict[step] = logical_or(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "logical_not"):
            print("[INFO] Logical_Not Found.")
            jobs_anwers_dict[step] = logical_not(playbook, job_json, jobs_anwers_dict[from_step])
        
        """ COMPARISON """
        if(name == "equal"):
            print("[INFO] Equal Found.")
            jobs_anwers_dict[step] = equal(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "not_equal"):
            print("[INFO] Not_Equal Found.")
            jobs_anwers_dict[step] = not_equal(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "less_or_equal"):
            print("[INFO] Less_Or_Equal Found.")
            jobs_anwers_dict[step] = less_or_equal(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "greater_or_equal"):
            print("[INFO] Greater_Or_Equal Found.")
            jobs_anwers_dict[step] = greater_or_equal(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "greater_than"):
            print("[INFO] Greater_Than Found.")
            jobs_anwers_dict[step] = greater_than(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "less_than"):
            print("[INFO] Less_than Found.")
            jobs_anwers_dict[step] = less_than(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        """ STATISTICAL """
        if(name == "mean_value"):
            print("[INFO] Mean_Value Found.")
            jobs_anwers_dict[step] = mean_value(playbook, job_json, jobs_anwers_dict[from_step])
        
        if(name == "variance_value"):
            print("[INFO] Variance_Value Found.")
            jobs_anwers_dict[step] = variance_value(playbook, job_json, jobs_anwers_dict[from_step])
        
        if(name == "amount_of"):
            print("[INFO] Amount_Of Found.")
            jobs_anwers_dict[step] = amount_of(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
    return

# Access jobs by viewing them Depth-first O(N)
def jobs(job_step, jobs_dict, jobs_anwers_dict, playbook, joins):
    flagged = False
    if(type(jobs_dict[job_step]["from"]) == list and not(job_step in joins)):
        joins[job_step] = 1
    elif(type(jobs_dict[job_step]["from"]) == list and (job_step in joins)):
        joins[job_step] += 1
    
    if(type(jobs_dict[job_step]["from"]) == list):
        if(joins[job_step] < len(jobs_dict[job_step]["from"])):
            flagged = True
        else:
            job_requestor(jobs_dict[job_step], jobs_anwers_dict, playbook)
    else:
        job_requestor(jobs_dict[job_step], jobs_anwers_dict, playbook)
    
    # Depth-first approach
    next_step = jobs_dict[job_step]["next"]
    
    if (next_step == 0):
        pass
    elif(flagged == True):
        pass
    else:
        jobs(next_step, jobs_dict, jobs_anwers_dict, playbook, joins)
            
    return

# Handle the playbook
def handler(playbook):
    print("[INFO] Finding starting jobs - Datasets.")
    # The jobs of the playbook.
    json_jobs = playbook["function"]["expression"]

    # handle jobs as a dictionary - O(N)
    jobs_dict = {}
    for job in json_jobs:
        jobs_dict[job["step"]] = job
    
    # Find starting jobs - O(N)
    starting_jobs = []
    for job_step, job in jobs_dict.items():
        if job["from"] == 0:
            starting_jobs.append(job_step)
    
    print("[INFO] Starting Jobs Found.")

    # Use a dictionary as a storage for each job answer
    jobs_anwers_dict = {}   # 0: bucket, 1: feature
    joins = {}
    
    # for each starting job, start the analysis
    print("[INFO] Starting the Depth-First Algorithm.")
    for starting_job_step in starting_jobs:
        job = jobs_dict[starting_job_step]
        # navigate through all the jobs and execute them in the right order
        jobs(starting_job_step, jobs_dict, jobs_anwers_dict, playbook, joins)
    
    # Print jobs_anwers_dict for testing purposes
    for job_step, answer in jobs_anwers_dict.items():
        print("[INFO]", job_step, "->", answer[0], "with:", answer[1], "| being:", answer[2])

    return

def bucket_dict_initialization(json_body):
    inputs = json_body["inputs"]
    args = json_body["function"]["args"]
    k = 0
    for arg in args:
        if "feature" in arg:
            input_buckets[arg["arg_id"]] = [inputs[k], arg["feature"]]
            k += 1
        if "value" in arg:
            input_buckets[arg["arg_id"]] = ["value", arg["value"]]

    return

def function_thread(json_body):
    start_time = time.time()    # Get the start time
    # Get the RAM usage
    process = psutil.Process(os.getpid())
    ram_usage = process.memory_info().rss / 1024 / 1024

    # Get the disk usage
    disk_usage = psutil.disk_usage('/').used / 1024 / 1024

    # Get the full RAM
    full_ram = psutil.virtual_memory().total / 1024 / 1024

    # Get job id
    job_id = json_body["job-id"]

    # Add the job in Mongo with status 
    mongo_obj = MongoDB_Class()
    mongo_record = {"job-id" : job_id, "status" : "progress"}
    mongo_obj.insertMongoRecord(mongo_record)

    bucket_dict_initialization(json_body)

    # Send the playbook for handling
    handler(json_body)

    # The job is now complete
    # The mongo_record is the filter for the update of the completed job
    mongo_obj.updateMongoStatus(mongo_record, "complete")

    # Calculate the execution time in milliseconds
    end_time = time.time()
    milliseconds = (end_time - start_time) * 1000
    
    # Make milliseconds integer
    milliseconds = int(milliseconds)

    # Update the MongoDB performance metrics
    mongo_record = { 
        "ram-usage" : int (ram_usage),
        "ram-existing" : int (full_ram),
        "disk-usage" : int (disk_usage),
        "execution-speed" : int (milliseconds) 
    }

    # Update Mongo Web Application metadata
    mongo_obj.updateMongoPerformanceMetrics("UIDB", "pipelines", { "analysisid" :  json_body["analysis-id"]}, {"label": job_id, "value": mongo_record})
    return
