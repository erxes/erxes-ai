"""
Customer scoring. Run every 3 days
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, MapType  # pylint: disable=import-error
from src.producer import publish
from src.utils import load_collection, create_session, execute_job, save_job_results

CONFORMITY_SCHEMA = StructType([
    StructField('_id', StringType()),
    StructField('mainType', StringType()),
    StructField('mainTypeId', StringType()),
    StructField('relType', StringType()),
    StructField('relTypeId', StringType())
])

CUSTOMER_SCHEMA = StructType([
    StructField('_id', StringType()),
    StructField('profileScore', IntegerType(), True),
    StructField('firstName', StringType()),
    StructField('lastName', StringType()),
    StructField('primaryPhone', StringType()),
    StructField('primaryEmail', StringType()),
    StructField('visitorContactInfo', MapType(StringType(), StringType(), True), True),
])


def job(mongo_url):
    """
    Job
    """

    print 'Started customer scoring on %s....' % mongo_url

    session = create_session(mongo_url)

    load_collection(session, CUSTOMER_SCHEMA, 'customers')
    load_collection(session, CONFORMITY_SCHEMA, 'conformities')

    aggre_df = session.sql('''
        SELECT
            customers._id,
            customers.profileScore,
            customers.firstName,
            customers.lastName,
            customers.primaryPhone,
            customers.primaryEmail,
            customers.visitorContactInfo,
            aggregated.totalDeals as totalDeals,
            aggregated.totalTickets as totalTickets,
            aggregated.totalTasks as totalTasks
        FROM
            (SELECT
                confs.mainTypeId as customerId,
                COUNT(confs.deal) as totalDeals,
                COUNT(confs.ticket) as totalTickets,
                COUNT(confs.task) as totalTasks

                FROM
                    (SELECT
                            mainTypeId,
                            CASE WHEN relType='deal' THEN 1 END deal,
                            CASE WHEN relType='ticket' THEN 1 END ticket,
                            CASE WHEN relType='task' THEN 1 END task
                        FROM
                            conformities
                        WHERE
                            mainType='customer'
                    ) AS confs

                GROUP BY
                    confs.mainTypeId
            ) AS aggregated

        RIGHT JOIN customers ON customers._id=aggregated.customerId

        WHERE customers.profileScore > 0 OR customers.profileScore IS NULL
    ''')

    def mapper(entry):
        """
        Mapper
        """

        score = 0
        explanation = {}
        name = ''

        if entry.visitorContactInfo:
            score += 1
            explanation['visitorContactInfo'] = '+1'
            name = name + 'Visitor contact info: ' + str(entry.visitorContactInfo)

        if entry.firstName:
            score += 5
            explanation['firstName'] = '+5'
            name = name + 'First name: ' + entry.firstName

        if entry.lastName:
            score += 5
            explanation['lastName'] = '+5'
            name = name + 'Last name: ' + entry.lastName

        if entry.primaryEmail:
            score += 5
            explanation['primaryEmail'] = '+5'
            name = name + 'Primary email: ' + entry.primaryEmail

        if entry.primaryPhone:
            score += 5
            explanation['primaryPhone'] = '+5'
            name = name + 'Primary phone: ' + entry.primaryPhone

        total_tasks = entry.totalTasks or 0
        total_tickets = entry.totalTickets or 0
        total_deals = entry.totalDeals or 0

        score += total_tasks * 20
        score += total_tickets * 30
        score += total_deals * 50

        explanation['totalTasks'] = '+%s' % (total_tasks * 20)
        explanation['totalTickets'] = '+%s' % (total_tickets * 30)
        explanation['totalDeals'] = '+%s' % (total_tickets * 50)

        return {'_id': entry['_id'], 'name': name, 'isUpdated': entry.profileScore != score, 'score': score, 'explanation': explanation}

    score_map = aggre_df.rdd \
        .map(mapper) \
        .filter(lambda entry: entry['isUpdated'] and entry['score'] > 0) \
        .map(lambda entry: {'_id': entry['_id'], 'name': entry['name'], 'score': entry['score'], 'explanation': entry['explanation']}) \
        .collect()

    if score_map:
        save_job_results('customerScoring', score_map)
        publish({'jobType': 'customerScoring', 'scoreMap': score_map})

execute_job(job)
