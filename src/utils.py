"""
Common utils
"""

import os
import urllib
from cv2 import countNonZero, subtract, split, imdecode, IMREAD_COLOR, error  # pylint: disable=no-name-in-module
import numpy as np
from dotenv import load_dotenv
from pyspark.sql import SparkSession  # pylint: disable=import-error


def execute_job(job):
    """
    Execute job
    """
    load_dotenv()

    mongo_url = os.getenv('MONGO_URL')

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
