"""
Common utils
"""

import os
import datetime
import urllib
from cv2 import countNonZero, subtract, split, imdecode, IMREAD_COLOR, error  # pylint: disable=no-name-in-module
import numpy as np
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession  # pylint: disable=import-error


def execute_job(job):
    """
    Execute job
    """
    load_dotenv()

    mongo_url = os.getenv('API_MONGO_URL')

    job(mongo_url)


def create_session(mongo_url):
    """
    Create session
    """

    session = SparkSession \
        .builder \
        .appName('erxes-ai') \
        .master('local[*]') \
        .config('spark.mongodb.input.uri', mongo_url) \
        .config('spark.mongodb.output.uri', mongo_url) \
        .getOrCreate()

    session.sparkContext.setLogLevel('ERROR')

    return session


def load_collection(session, schema, name):
    """
    Load collection
    """

    session.read.format('mongo').option('collection', name).schema(
        schema).load().createOrReplaceTempView(name)


def image_equality_checker(path1, path2):
    """
    Image equality checker
    """

    try:
        original_resp = urllib.urlopen(path1)
        original = np.asarray(bytearray(original_resp.read()), dtype="uint8")
        original = imdecode(original, IMREAD_COLOR)

        duplicate_resp = urllib.urlopen(path2)
        duplicate = np.asarray(bytearray(duplicate_resp.read()), dtype="uint8")
        duplicate = imdecode(duplicate, IMREAD_COLOR)
    except error:
        return False

    if original.shape != duplicate.shape:
        return False

    difference = subtract(original, duplicate)

    blue, green, red = split(difference)

    if countNonZero(blue) == 0 and countNonZero(green) == 0 and countNonZero(red) == 0:
        return True


def get_database():
    """
    Get database
    """
    load_dotenv()

    client = MongoClient(os.getenv('MONGO_URI'))
    return client['erxes-ai']

def save_job_results(job_type, data):
    """
    Save job results to database
    """
    database = get_database()
    job_results = database.job_results

    if job_type == 'customerScoring':
        parent_job = {
            'type': job_type,
            'data': {'count': len(data)},
            'is_notified': False,
            'created_at': datetime.datetime.utcnow()
        }

        parent_job_id = job_results.insert_one(parent_job).inserted_id

        child_jobs = []

        for result in data:
            child_jobs.append({
                'parent_id': str(parent_job_id),
                'type': job_type,
                'created_at': datetime.datetime.utcnow(),
                'data': result,
            })

        job_results.insert_many(child_jobs)

    if job_type == 'suggestion':
        job = {
            'type': job_type,
            'data': data,
            'is_notified': False,
            'created_at': datetime.datetime.utcnow()
        }

        prev_entry = job_results.find_one({'type': job_type, 'data.message': data['message']})

        if prev_entry:
            return job_results.update_one({'_id': prev_entry['_id']}, {'$set': {'is_notified': False}})

        database.job_results.insert_one(job)

    if job_type == 'mergeCustomers' and data:
        parent_job = {
            'type': job_type,
            'data': {'count': len(data)},
            'is_notified': False,
            'created_at': datetime.datetime.utcnow()
        }

        parent_job_id = job_results.insert_one(parent_job).inserted_id

        child_jobs = []

        for result in data:
            child_jobs.append({
                'parent_id': str(parent_job_id),
                'type': job_type,
                'created_at': datetime.datetime.utcnow(),
                'data': result,
            })

        job_results.insert_many(child_jobs)
