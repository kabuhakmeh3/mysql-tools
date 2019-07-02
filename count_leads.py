import os, datetime, pickle
import pandas as pd
from sqlalchemy import create_engine
from connect_mysql import auth_db


# Database relevant columns
#
# referrals
#   campaign, created_at
#
# leads
#   created_at


def get_date_range(timespan=1):
    '''Build a date range for queries
    '''
    now=f"{datetime.datetime.now():%Y-%m-%d}"
    start=f"{datetime.datetime.now()-datetime.timedelta(timespan):%Y-%m-%d}"
    date_range = "'" + start + "'" + " and " + "'" + now + "'"

    return date_range

def query_referrals(dates, engine):
    '''Generate SQL query & return lead counts grouped by campaign
    '''
    sql = "SELECT campaign, COUNT(campaign) as leads " +\
    "from referrals " +\
    "WHERE created_at " +\
    "BETWEEN " + dates +\
    " GROUP BY campaign"

    df = pd.read_sql(sql, engine, index_col=None)

    return df

def query_leads(dates, engine):
    '''Generate SQL query & process df to match referral format
    '''

    sql= "SELECT COUNT(created_at) as leads " +\
    "from leads " +\
    "WHERE created_at " +\
    "BETWEEN " + dates

    df = pd.read_sql(sql, engine, index_col=None)
    df['campaign'] = 'payment'
    df = df[['campaign', 'leads']]

    return df

def main(target_path='./sample_data'):

    # load credentials & create engine
    db_engine = create_engine(auth_db())

    # get correct dates
    now = datetime.datetime.now()-datetime.timedelta(1)
    date_range = get_date_range(timespan=1)

    # query referral database
    df_ref = query_referrals(dates=date_range, engine=db_engine)

    # query lead database
    df_lead = query_leads(dates=date_range, engine=db_engine)

    # combine query results
    df = pd.concat([df_ref, df_lead])
    df['date'] = f"{now:%Y-%m-%d}"
    df = df[['date','campaign','leads']]

    # write output to file
    target_file = os.path.join(target_path, 'lead_totals.csv')
    df.to_csv(target_file, mode='a', header=False, index=False)

if __name__ == '__main__':
    main()
