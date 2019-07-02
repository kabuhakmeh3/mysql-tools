from sqlalchemy import create_engine
from connect_mysql import auth_db
import pandas as pd

engine = create_engine(auth_db())

sql = "SELECT * from referrals WHERE campaign='buddy'"

df = pd.read_sql(sql, engine, index_col=None)
