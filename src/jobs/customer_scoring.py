"""
Customer scoring. Run every 3 days
"""

from pyspark.sql.types import StringType, StructField, StructType, MapType  # pylint: disable=import-error
from src.producer import publish
from src.utils import load_collection, create_session, execute_job

CONFORMITY_SCHEMA = StructType([
    StructField('_id', StringType()),
    StructField('mainType', StringType()),
    StructField('mainTypeId', StringType()),
    StructField('relType', StringType()),
    StructField('relTypeId', StringType())
])

CUSTOMER_SCHEMA = StructType([
    StructField('_id', StringType()),
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

        LEFT JOIN customers ON customers._id=aggregated.customerId
    ''')

    def mapper(entry):
        """
        Mapper
        """

        score = 0

        if entry.visitorContactInfo:
            score += 1

        if entry.firstName:
            score += 5

        if entry.lastName:
            score += 5

        if entry.primaryEmail:
            score += 5

        if entry.primaryPhone:
            score += 5

        score += entry.totalTasks * 20
        score += entry.totalTickets * 30
        score += entry.totalDeals * 50

        return {'_id': entry['_id'], 'score': score}

    score_map = aggre_df.rdd.map(mapper).collect()

    publish({'action': 'customerScoring', 'scoreMap': score_map})


execute_job(job)
