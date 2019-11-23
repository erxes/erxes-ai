"""
Server
"""

from bson.objectid import ObjectId
from pymongo import ASCENDING
from flask import Flask, jsonify, request
from src.utils import get_database

APP = Flask(__name__)

@APP.route('/get-job-types')
def get_job_types():
    """
    Get job types
    """

    database = get_database()
    job_results = database.job_results

    results = []

    for job_type in ['mergeCustomers', 'fillCompanyInfo', 'customerScoring', 'suggestion']:
        results.append({
            'name': job_type,
            'notificationsCount': job_results.count({'type': job_type, 'parent_id': None, 'is_notified': False}),
        })

    return jsonify(results)

@APP.route('/get-jobs')
def get_jobs():
    """
    Get jobs
    """

    job_type = request.args.get('type')
    is_notified = request.args.get('isNotified')
    limit = request.args.get('limit', 10)

    database = get_database()
    job_results = database.job_results

    selector = {'type': job_type, 'parent_id': None}

    if is_notified:
        selector['is_notified'] = is_notified

    jobs = []

    for job in job_results.find(selector, limit=int(limit)).sort('created_at', ASCENDING):
        jobs.append({
            '_id': str(job['_id']),
            'isNotified': job['is_notified'],
            'data': job['data']
        })

    return jsonify(jobs)

@APP.route('/get-job-detail')
def get_job_detail():
    """
    Get job detail
    """

    _id = request.args.get('_id')

    database = get_database()
    job_results = database.job_results

    job = job_results.find_one({'_id': ObjectId(_id)})

    # mark as notified
    job_results.update_one({'_id': ObjectId(_id)}, {'$set': {'is_notified': True }})

    subs = []

    for job in job_results.find({'parent_id': _id}, limit=1000):
        subs.append(job['data'])

    return jsonify({'type': job['type'], 'data': job['data'], 'subs': subs})