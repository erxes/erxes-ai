"""
Integration emptiness. Run every day
"""

from pyspark.sql.types import StringType, StructField, StructType, ArrayType  # pylint: disable=import-error
from src.utils import load_collection, create_session, execute_job
from src.producer import publish

CHANNEL_SCHEMA = StructType([
    StructField('_id', StringType()),
    StructField('integrationIds', ArrayType(StringType(), True), True),
    StructField('memberIds', ArrayType(StringType(), True), True)
])

BRAND_SCHEMA = StructType([
    StructField('_id', StringType()),
    StructField('name', StringType()),
])

INTEGRATION_SCHEMA = StructType([
    StructField('_id', StringType()),
    StructField('name', StringType()),
    StructField('brandId', StringType()),
])


def job(mongo_url):
    """
    Job
    """

    print 'Started integration emptyness on %s....' % mongo_url

    session = create_session(mongo_url)

    load_collection(session, CHANNEL_SCHEMA, 'channels')
    load_collection(session, BRAND_SCHEMA, 'brands')
    load_collection(session, INTEGRATION_SCHEMA, 'integrations')

    # Channels without integration ===========================
    channel_ids = session.sql('''
        SELECT _id
        FROM channels
        WHERE
            integrationIds IS NULL OR
            SIZE(integrationIds) = 0
    ''').rdd.map(lambda channel: channel['_id']).collect()

    publish({'action': 'channelsWithoutIntegration', 'channelIds': channel_ids})

    # Channels without members ===========================
    channel_ids_without_member_ids = session.sql('''
        SELECT _id
        FROM channels
        WHERE
            memberIds IS NULL OR
            SIZE(memberIds) = 0
    ''').rdd.map(lambda channel: channel['_id']).collect()

    publish({'action': 'channelsWithoutMembers', 'channelIds': channel_ids_without_member_ids})

    # Brands without integration ===========================
    brand_ids = session.sql('''
        SELECT
            brands._id,
            COUNT(integrations._id)
        FROM brands
        LEFT JOIN integrations ON integrations.brandId = brands._id
        GROUP BY brands._id
        HAVING COUNT(integrations._id) = 0
    ''').rdd.map(lambda brand: brand['_id']).collect()

    publish({'action': 'brandsWithoutIntegration', 'brandIds': brand_ids})


execute_job(job)
