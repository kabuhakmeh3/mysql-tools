from sqlalchemy import create_engine
from connect_mysql import auth_db
import pandas as pd
import pickle

# dynmically build sql query to only choose specific columns
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

engine = create_engine(auth_db())

sql_refr = "SELECT email, mobile from referrals"
sql_lead = "SELECT email, mobile_phone as mobile from leads"

df_refr = pd.read_sql(sql_refr, engine, index_col=None)
df_lead = pd.read_sql(sql_lead, engine, index_col=None)

df = pd.concat([df_refr, df_lead])

print('Read {} records from database'.format(df.shape[0]))

list_email = df['email'].dropna().tolist()
list_phone = df['mobile'].dropna().tolist()

print('{} emails and {} phone numbers read'.format(
    len(list_email), len(list_phone)))

list_to_pickle('./sample_data/emails.pckl', list_email)
list_to_pickle('./sample_data/phones.pckl', list_phone)
