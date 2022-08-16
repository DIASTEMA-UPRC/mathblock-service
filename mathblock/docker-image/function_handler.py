# Import custom Classes
from MongoDB_Class import MongoDB_Class
from MinIO_Class import MinIO_Class

# Import custom Functions for jobs
from subtraction import subtraction

""" Functions used for the json handling """
# Request a job
def job_requestor(job_json, jobs_anwers_dict, playbook):
    kind = job_json["info"]["kind"]
    step = job_json["step"]
    from_step = job_json["from"]

    # If argument then only save its bucket
    if(kind == "arg"):
        print("[INFO] Argument Found.")
        # Get the bucket of the argument
        arg_id = job_json["info"]["arg_id"] - 1
        arg_bucket = playbook["inputs"][arg_id]
        jobs_anwers_dict[step] = [arg_bucket, playbook["function"]["args"][arg_id]["feature"]]
    
    # If operation then find operation and then execute it
    if(kind == "operation"):
        name = job_json["info"]["name"]

        if(name == "addition"):
            pass
        
        if(name == "subtraction"):
            print("[INFO] Subtraction Found.")
            jobs_anwers_dict[step] = subtraction(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])
        
        if(name == "division"):
            pass
        
        if(name == "multiplication"):
            pass
        
        if(name == "logarithm"):
            pass
        
        if(name == "exponential"):
            pass
        
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
    # for step in next_steps:
    #     if(step == 0): # If ther is no next job then do not try to go deeper
    #         pass
    #     elif(flagged == True): # If this job is flagged do not try to go deeper
    #         pass
    #     else:
    #         jobs(step, jobs_dict, jobs_anwers_dict, playbook, joins)
    
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
        print("[INFO]", job_step, "->", answer[0], "with:", answer[1])

    return


def function_thread(json_body):
    # Get job id
    job_id = json_body["job-id"]

    # Add the job in Mongo with status 
    mongo_obj = MongoDB_Class()
    mongo_record = {"job-id" : job_id, "status" : "progress"}
    mongo_obj.insertMongoRecord(mongo_record)

    # Send the playbook for handling
    handler(json_body)

    # The job is now complete
    # The mongo_record is the filter for the update of the completed job
    mongo_obj.updateMongoStatus(mongo_record, "complete")
    return