# Import custom Libraries
from function_handler import function_thread as function_t

# Import custom Classes
from MongoDB_Class import MongoDB_Class

# Import Libraries
import os
from flask import Flask, request, Response, make_response
import threading

""" Environment Variables """
# Flask app Host and Port
HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", 5000))

""" Global variables """
# The name of the flask app
app = Flask(__name__)

""" Flask endpoints """
# Start Mathblock Service Route
@app.route("/function", methods=["POST"])
def function_job():
    # Get json body to handle a function
    print("[INFO] Accepted Start Request.")
    json_body = request.json

    # Make a new thread for the execution of the new function
    print("[INFO] Starting a new Thread for the execution of a function.")
    thread = threading.Thread(target = function_t, args = (json_body, ))
    thread.start()
    print("[INFO] Function started.")

    return Response(status=202)

# Get Mathblock Service progress Route
@app.route("/function/progress", methods=["GET"])
def function_job_progress():
    # Get the id of the function job
    job_id = request.args.get('id')
    print(job_id)

    # Find the Mongo Document and check if it is finished
    mongo_obj = MongoDB_Class()
    mongo_doc = mongo_obj.findMongoDocument(job_id)

    # Return the status of the job
    return mongo_doc["status"]

""" Main """
# Main code
if __name__ == "__main__":
    app.run(HOST, PORT, True)