# Import custom Libraries
from operation_assist import operation_with_two_inputs
"""
A program to handle all the operations
- # THE INPUT AND OUTPUT BUCKETS ARE IN THIS FORMAT[bucket, feature]
"""

# Make each operation with its inputs and return the result bucket and feature
def subtraction(playbook, job, left_bucket, right_bucket):
    return operation_with_two_inputs(playbook, job, left_bucket, right_bucket, "subtraction")

def addition(playbook, job, left_bucket, right_bucket):
    return operation_with_two_inputs(playbook, job, left_bucket, right_bucket, "addition")

def division(playbook, job, left_bucket, right_bucket):
    return operation_with_two_inputs(playbook, job, left_bucket, right_bucket, "division")

def multiplication(playbook, job, left_bucket, right_bucket):
    return operation_with_two_inputs(playbook, job, left_bucket, right_bucket, "multiplication")

def logarithm(playbook, job, left_bucket, right_bucket):
    return operation_with_two_inputs(playbook, job, left_bucket, right_bucket, "logarithm")

def power(playbook, job, left_bucket, right_bucket):
    return operation_with_two_inputs(playbook, job, left_bucket, right_bucket, "power")