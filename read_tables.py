from sqlalchemy import create_engine
from connect_mysql import auth_db
import pandas as pd

# dynmically build sql query to only choose specific columns
#
# referrals
#   club, first_name, last_name, email, mobile, campaign
#
# leads
#   first_name, last_name, email, mobile_phone
#
# Note: consider only importing phone & email

engine = create_engine(auth_db())

sql_refr = "SELECT email, mobile from referrals"

sql_lead = "SELECT email, mobile_phone as mobile from leads"

df_refr = pd.read_sql(sql_refr, engine, index_col=None)
df_lead = pd.read_sql(sql_lead, engine, index_col=None)

df = pd.concat([df_refr, df_lead])

print(df.head())
