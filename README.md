# DIASTEMA Mathblock Service

## Description
DIASTEMA uses a service to execute mathematical expressions on different datasets in order to produce new datasets containing valuable information [[1]](https://github.com/DIASTEMA-UPRC/mathblock-service/blob/main/README.md#references).

This service receives a JSON graph that mentions in it all the necessary information for the execution of a mathematical expression. This information refers to which operation should run first before others, what operation each one is, various arguments, ids, and more.

The service mentioned above is called Mathblock Service. It communicates with the Diastema Orchestrator as well as the Mongo and MinIO Servers existing inside of the Diastema cloud. It is responsible to execute an expression on many different given datasets as attributes and generate a new dataset inside of a given output MinIO bucket.

The Mathblock service is capable of accessing and executing any graph that has specified starting and ending nodes. It does not matter how many starting nodes there are, but there has to be only one ending node inside each graph. The ending node is only one, because this is how mathematical operations are working. For example the addition has two inputs and only one output. The Mathblock algorithm is copying the Diastema Orchestrator's algorithm as their logic is the same for parsing graphs.

## Repository Contents

- mathblock/docker-image
  - The source code of the Mathbloc service. It also contains a python virtual environment with all the needed libraries. This directory can be used to make a docker image of the service as well.
- mathblock/dummy/datasets
  - Some testing datasets that can be used to test the service locally.
- mathblock/dummy/minio-dummy-bucket
  - A dummy MinIO bucket called "metis" giving the ability to test the service locally using a MinIO server.
- mathblock/dummy/single-operation-jsons
  - JSON files that are showing how the body of a REST call on the API of this service should be for different single mathematical operations.
- mathblock/dummy/multi-operation-jsons
  - JSON files that are showing how the body of a REST call on the API of this service should be for functions containing many different mathematical operations, argument and values.

## Example of use
In this section, an example of how to use the source code of this repository is shown, using the files from the dummy and helping repositories.

### Prerequisites
Below is an example of prerequisites:
- Docker
- Windows OS
- MongoDB
- Postman
- MinIO
- MongoDB

You can execute the functionality with other prerequisites and commands as well!

### MongoDB Initialization
1. Make sure that you are using Mongo and it is running on your system. You can check this by opening a CMD and typing the below command:
```
mongosh
```
After opening the Mongo shell, run the following:
```
use UIDB
db.dropDatabase()
use UIDB
db.pipelines.insert( { "analysisid" : "039ff178fb8a5" })
cls
db.datasets.find()
db.pipelines.find()

```

This command should open the Mongo Shell. for the rest of this guide you will not need Mongo Shell open so you can close this CMD.

Mongo is used by the Mathblock Service only to be able to communicate with the Orchestrator and Web Application better.

### Service Startup
2. Clone this repository locally.
3. Go to the reposiroty below:
```
mathblock-service/mathblock/docker-image/
```
4. Type the command below to build the docker image of the service:
```
docker build --tag mathblock-service-image .
```
5. Run the container of the above image:
```
docker run -p 127.0.0.1:5000:5000 ^
--name mathblock-service ^
--restart always ^
-e HOST=0.0.0.0 ^
-e PORT=5000 ^
-e MINIO_HOST=host.docker.internal ^
-e MINIO_PORT=9000 ^
-e MINIO_USER=diastema ^
-e MINIO_PASS=diastema ^
-e MONGO_HOST=host.docker.internal ^
-e MONGO_PORT=27017 ^
mathblock-service-image
```

### MinIO Initialization
6. Make sure that you have your MinIO running. You can check this by opening a browser and going to the below URL:
```
localhost:9000
```
The above URL by default is hosting the MinIO GUI.

7. Copy the contents of the folder below in your MinIO path:
```
mathblock-service/mathblock/dummy/minio-dummy-bucket
```

8. Procced to add the below directories inside of the metis folder:
- analysis-511345633/function-7
- analysis-563345633/function-5
- analysis-5633456323/function-1

The above directories are made by the Orchestration service in the Diastema platform.

### Usage
Now the Mathblock service, MongoDB and MinIO are running.

The next step is to call the service to execute some functions.

9. Open Postman and execute the following requests:
- Function call:
   - POST
   - URL: http://localhost:5000/function
   - JSON BODY: The "arithmetic.json" from the repository named "mathblock/dummy/single-operation-jsons/arithmetic"

By executing the above request and getting a "STATUS CODE 202 CREATED" you know that the service got your requests and has started processing it. It will update MongoDB with the progress of the execution and will start to interact with MinIO to complete the given function.

You can use the other JSONs inside of the repositories "mathblock/dummy/single-operation-jsons" and "dummy/multi-operation-jsons" as well.

Your execution will be stored in the MinIO bucket below:
```
metis/analysis-5633456323/function-1
```

10. To see the progress of the execution, use Postman by executing the following request:
- Function Progress Check:
   - GET
   - URL: http://localhost:5000/function/progress?id=34345345s3d4643

With the above call, you will get a JSON body with the schema analyzed below.
```
{
           "status": "progress" | "error" | "complete",
           "message": "..."
}
```

The "status" can be:
- "complete": Your execution is done
- "progress": Your execution is not done yet
- "error": Your execution terminated with an error

The "message" attribute will exist only if the "status" is "error" and it will contain the error that occurred.

## References
- [1] https://diastema.gr/
