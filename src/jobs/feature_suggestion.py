"""
Feature suggestion. Run every day
"""

from pyspark.sql.types import StringType, StructField, StructType  # pylint: disable=import-error
from src.utils import load_collection, create_session, execute_job
from src.producer import publish

ID_SCHEMA = StructType([
    StructField('_id', StringType()),
])


def counter(session, name, message):
    """
    Counter
    """

    load_collection(session, ID_SCHEMA, name)

    count = session.sql('SELECT COUNT(*) FROM %s' % name).rdd.collect()[0][0]

    if count == 0:
        publish({'action': 'featureSuggestion', 'message': message})


def job(mongo_url):
    """
    Job
    """

    print 'Started feature suggestion on %s....' % mongo_url

    session = create_session(mongo_url)

    counter(session, 'channels', 'notUsingChannels')
    counter(session, 'integrations', 'notUsingIntegrations')
    counter(session, 'brands', 'notUsingBrands')
    counter(session, 'engage_messages', 'notUsingEngages')
    counter(session, 'deals', 'notUsingDeals')
    counter(session, 'tasks', 'notUsingTasks')
    counter(session, 'tickets', 'notUsingTickets')
    counter(session, 'knowledgebase_article', 'notUsingKb')
    counter(session, 'response_templates', 'notUsingResponseTemplates')
    counter(session, 'email_templates', 'notUsingEmailTemplates')
    counter(session, 'scripts', 'notUsingEmailScripts')
    counter(session, 'tags', 'notUsingEmailTags')

execute_job(job)
