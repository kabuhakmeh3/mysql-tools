import os, datetime, pickle
import pandas as pd
from sqlalchemy import create_engine
from connect_mysql import auth_db


# Database relevant columns
#
# referrals
#   campaign, created_at, club
#
# leads
#   created_at, crm_club_id

def load_pickle(path_to_pickle):
    with open(path_to_pickle, 'rb') as p:
         return pickle.load(p)

market_ids = load_pickle('./market_ids.pickle')

def map_market(club):
    for market in market_ids:
        if club in market_ids[market]:
            return market
    return 'NoMarketFound'

def get_date_range(timespan=1):
    '''Build a date range for queries

    Default - count leads for previous day
    '''
    
    now=f"{datetime.datetime.now():%Y-%m-%d}"
    start=f"{datetime.datetime.now()-datetime.timedelta(timespan):%Y-%m-%d}"
    date_range = "'" + start + "'" + " and " + "'" + now + "'"

    return date_range

def query_referrals(dates, engine):
    '''Generate SQL query & return lead counts grouped by campaign
    '''

    sql = "SELECT campaign, club, COUNT(1) as leads " +\
    "from referrals " +\
    "WHERE created_at " +\
    "BETWEEN " + dates +\
    " GROUP BY club"

    df = pd.read_sql(sql, engine, index_col=None)
    df['market'] = df['club'].apply(map_market)
    cols_to_keep = ['campaign','market','leads']
    return df[cols_to_keep]

def query_leads(dates, engine):
    '''Generate SQL query & process df to match referral format
    '''

    sql= "SELECT paramount_club_id as club, COUNT(1) as leads " +\
    "from leads " +\
    "WHERE created_at " +\
    "BETWEEN " + dates +\
    " GROUP BY club"

    df = pd.read_sql(sql, engine, index_col=None)
    df['campaign'] = 'payment'
    df['market'] = df['club'].apply(map_market)
    df = df[['campaign', 'market', 'leads']]
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
    df = df[['date','campaign','market','leads']]

    # write output to file
    target_file = os.path.join(target_path, 'mkt_lead_totals.csv')
    df.to_csv(target_file, mode='a', header=False, index=False)

if __name__ == '__main__':
    main()
