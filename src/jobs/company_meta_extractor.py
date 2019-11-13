"""
Company meta extractor. Run every 3 days
"""

import requests
from bs4 import BeautifulSoup
from pyspark.sql.types import StringType, StructField, StructType, MapType  # pylint: disable=import-error
from src.producer import publish
from src.utils import create_session, load_collection, execute_job

COMPANY_SCHEMA = StructType([
    StructField('_id', StringType()),
    StructField('avatar', StringType()),
    StructField('links', MapType(StringType(), StringType(), True), True),
])


def job(mongo_url):
    """
    Job
    """

    print 'Started company meta extractor on %s....' % mongo_url

    session = create_session(mongo_url)

    load_collection(session, COMPANY_SCHEMA, 'companies')

    companies_df = session.sql('''
        SELECT
            _id,
            links.website

            FROM companies

            WHERE
                avatar IS NULL AND
                links.website IS NOT NULL AND
                links.website <> ''
    ''')

    companies = companies_df.collect()

    results = []

    for company in companies:
        try:
            modifier = {}

            website = company.website

            if 'http' not in website:
                website = 'http://%s' % website

            response = requests.get(website, timeout=3)
            soup = BeautifulSoup(response.text)

            icon_tag = soup.find('link', rel='shortcut icon')
            meta_description_tag = soup.find('meta', property='og:description')
            meta_title_tag = soup.find('meta', property='og:title')

            if icon_tag:
                modifier['avatar'] = '%s%s' % (company.website, icon_tag.attrs['href'])

            if meta_description_tag:
                modifier['description'] = meta_description_tag.attrs['content']

            if not modifier['description'] and meta_title_tag:
                modifier['description'] = meta_title_tag.attrs['content']

            if modifier:
                results.append({'_id': company['_id'], 'modifier': modifier})

        except Exception as error:  # pylint:disable=broad-except
            print error
            continue

    publish({'action': 'fillCompanyInfo', 'results': results})


execute_job(job)
