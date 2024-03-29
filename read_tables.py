import os, pickle
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
# Note: consider only importing phone & email

def list_to_pickle(file_to_write, list_to_write):
    with open(file_to_write, 'wb') as fp:
        pickle.dump(list_to_write, fp)


def main(list_path='./sample_data'):
    
    # load credentials & create engine
    engine = create_engine(auth_db())
    
    # define sql queries for each table
    sql_refr = "SELECT email, mobile from referrals"
    sql_lead = "SELECT email, mobile_phone as mobile from leads"

    # read & merge lead data
    df_refr = pd.read_sql(sql_refr, engine, index_col=None)
    df_lead = pd.read_sql(sql_lead, engine, index_col=None)

    df = pd.concat([df_refr, df_lead])

    # create lists of lead contact info
    list_email = df['email'].dropna().tolist()
    list_phone = df['mobile'].dropna().tolist()

    # write lists to file
    list_to_pickle(os.path.join(list_path, 'emails.pckl'), list_email)
    list_to_pickle(os.path.join(list_path, 'phones.pckl'), list_phone)

if __name__ == '__main__':
    main()
