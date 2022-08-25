# Import custom Libraries
from operation_assist import operation_handler
"""
A program to handle all the operations
- THE INPUT AND OUTPUT BUCKETS ARE IN THIS FORMAT[bucket, feature]
"""

""" ARITHMETIC """
# Make each operation with its inputs and return the result bucket and feature
def subtraction(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "subtraction")

def addition(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "addition")

def division(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "division")

def multiplication(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "multiplication")

def logarithm(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "logarithm")

def power(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "power")

""" LOGICAL """

def logical_and(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "logical_and")

def logical_or(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "logical_or")

def logical_not(playbook, job, input_bucket):
    return operation_handler(playbook, job, input_bucket, False, "logical_not")

""" COMPARISON """

def equal(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "equal")

def not_equal(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "not_equal")

def less_or_equal(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "less_or_equal")

def greater_or_equal(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "greater_or_equal")

def greater_than(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "greater_than")

def less_than(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "less_than")

""" STATISTICAL """

def mean_value(playbook, job, input_bucket):
    return operation_handler(playbook, job, input_bucket, False, "mean_value")

def variance_value(playbook, job, input_bucket):
    return operation_handler(playbook, job, input_bucket, False, "variance_value")

def amount_of(playbook, job, left_bucket, right_bucket):
    return operation_handler(playbook, job, left_bucket, right_bucket, "amount_of")