"""
Merge customers. Run every 3 days
"""

from pyspark.sql.types import StringType, StructField, StructType  # pylint: disable=import-error
from src.utils import image_equality_checker, create_session, execute_job, load_collection
from src.producer import publish

CUSTOMER_SCHEMA = StructType([
    StructField('_id', StringType()),
    StructField('firstName', StringType()),
    StructField('lastName', StringType()),
    StructField('avatar', StringType()),
    StructField('status', StringType()),
])


def job(mongo_url):
    """
    Job
    """

    print 'Started merge customers on %s....' % mongo_url

    session = create_session(mongo_url)
    load_collection(session, CUSTOMER_SCHEMA, 'customers')

    customers_df = session.sql('''
        SELECT
            _id,
            firstName,
            lastName,
            avatar,
            status

            FROM customers

            WHERE
                status <> 'Deleted' AND
                firstName IS NOT NULL AND
                lastName IS NOT NULL AND
                avatar IS NOT NULL
    ''')

    def compare(sub_list):
        """
        Compare
        """

        used_indexes = []
        last_index = len(sub_list)

        for i in range(last_index - 1):
            customer = sub_list[i]

            print('checking .....', customer['_id'])

            identical_customer_ids = [customer['_id']]

            for j in range(i + 1, last_index):
                if j in used_indexes:
                    continue

                sub_customer = sub_list[j]

                if not image_equality_checker(customer['avatar'], sub_customer['avatar']):
                    continue

                used_indexes.append(j)
                identical_customer_ids.append(sub_customer['_id'])

            if len(identical_customer_ids) > 1:
                publish({'action': 'mergeCustomers', 'customerIds': identical_customer_ids})

    customers_df.rdd \
        .map(lambda cus: ((cus.firstName, cus.lastName), [{'_id': cus['_id'], 'avatar': cus.avatar}])) \
        .reduceByKey(lambda _idaa1, _idaa2: _idaa1 + _idaa2) \
        .filter(lambda item: len(item[1]) > 1) \
        .values() \
        .filter(lambda subList: len(subList) >= 2) \
        .foreach(compare)


execute_job(job)
