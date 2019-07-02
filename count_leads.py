import os, datetime, pickle
from sqlalchemy import create_engine
from connect_mysql import auth_db
import pandas as pd

# Suggestions for improvement
#
# build sql query to only choose specific columns
#
# referrals
#   club, first_name, last_name, email, mobile, campaign, created_at
#
# leads
#   first_name, last_name, email, mobile_phone, created_at
#
# Note: consider only importing phone & email

# results will feed into dashboard
# consider writing/appending to csv instead
def list_to_pickle(file_to_write, list_to_write):
    with open(file_to_write, 'wb') as fp:
        pickle.dump(list_to_write, fp)


def get_date_range(timespan=1):
    '''Build a date range for queries
    '''
    now=f"{datetime.datetime.now():%Y-%m-%d}"
    start=f"{datetime.datetime.now()-datetime.timedelta(timespan):%Y-%m-%d}"
    date_range = "'" + start + "'" + " and " + "'" + now + "'"

    return date_range

#def build_query():
#    return query

def main(list_path='./sample_data'):

    # load credentials & create engine
    engine = create_engine(auth_db())

    sql = "SELECT campaign, COUNT(campaign) as leads " +\
    "from referrals " +\
    "WHERE created_at " +\
    "BETWEEN " + get_date_range() +\
    " GROUP BY campaign"

    df = pd.read_sql(sql, engine, index_col=None)

    print(df)

if __name__ == '__main__':
    main()
