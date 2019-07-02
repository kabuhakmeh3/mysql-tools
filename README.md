# mysql-tools

Contains tools and scripts to read data from mysql database 

Required packages include:

+ pandas
+ pymysql
+ sqlalchemy

Primary data queried is lead & referral tables. 

These tables are compared against daily enrollments to ensure correct sales
attribution.

To check leads daily, include the following in a script
crontab:

```
python queryLeads.py
```
