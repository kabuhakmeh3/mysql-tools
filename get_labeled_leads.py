import os
from sqlalchemy import create_engine
from connect_mysql import auth_db
import pandas as pd

# Suggestions for improvement
#
# build sql query to only choose specific columns
#
# referrals
#   club, first_name, last_name, email, mobile, campaign
#
# leads
#   first_name, last_name, email, mobile_phone
#

def main(list_path='./sample_data'):

    # load credentials & create engine
    engine = create_engine(auth_db())

    # define sql queries for each table
    sql_refr = "SELECT email, mobile, created_at, campaign from referrals"
    sql_lead = "SELECT email, mobile_phone as mobile, created_at, abandoned from leads"

    # read & merge lead data
    df_refr = pd.read_sql(sql_refr, engine, index_col=None)
    df_lead = pd.read_sql(sql_lead, engine, index_col=None)

    # write all leads
    df_refr.to_csv(os.path.join(list_path, 'referrals.csv'), index=False)
    df_lead.to_csv(os.path.join(list_path, 'payment_leads.csv'), index=False)

if __name__ == '__main__':
    main()
