"""
__author__ = "Nate Cutler"
__email__ = "ncutler211@gmail.com"
"""

import pandas as pd
import numpy as np
import psycopg2

cases = pd.read_csv('https://docs.google.com/spreadsheets/d/1AWLK06JOlSKImgoHNTbj7oXR5mRfsL2WWeQF6ofMq1g/gviz/tq?tqx=out:csv')

def get_conn_cur():
 # UPDATE WITH YOUR INFO!

 conn = psycopg2.connect(
    host="test-hw-db.ci0lfamthzhb.us-west-1.rds.amazonaws.com",
    database="ista_322_db",
    user="nc",
    password="Triste211!",
    port='5432'
    )

 cur = conn.cursor()
 return(conn, cur)

def get_column_names(table_name): # arguement of table_name
 conn, cur = get_conn_cur() # get connection and cursor

 # Now select column names while inserting the table name into the WERE
 column_name_query = """SELECT column_name FROM information_schema.columns
    WHERE table_name = '%s' """ %table_name

 cur.execute(column_name_query) # exectue
 my_data = cur.fetchall() # store

 cur.close() # close
 conn.close() # close

 return(my_data) # return

# Check table_names
def get_table_names():
 conn, cur = get_conn_cur() # get connection and cursor

 # query to get table names
 table_name_query = """SELECT table_name FROM information_schema.tables """

 cur.execute(table_name_query) # execute
 my_data = cur.fetchall() # fetch results

 cur.close() #close cursor
 conn.close() # close connection

 return(my_data) # return your fetched results

# make sql_head function
def sql_head(table_name):
 conn, cur = get_conn_cur() # get connection and cursor

 # Now select column names while inserting the table name into the WERE
 head_query = """SELECT * FROM %s LIMIT 5; """ %table_name

 cur.execute(head_query) # exectue
 colnames = [desc[0] for desc in cur.description] # get column names
 my_data = cur.fetchall() # store first five rows

 cur.close() # close
 conn.close() # close

 df = pd.DataFrame(data = my_data, columns = colnames) # make into df

 return(df) # return

# Make judge_id in cases
cases_df = cases
cases_df['judge_id'] = pd.factorize(cases_df['judge_name'])[0]
# select necessary columns to make cases_df
cases_df = cases_df.loc[:, ['case_id','casetype_id','case_year','judge_id','category_id','category_name',]]

def run_query(query_string):

 conn, cur = get_conn_cur() # get connection and cursor

 cur.execute(query_string) # executing string as before

 my_data = cur.fetchall() # fetch query data as before

 # here we're extracting the 0th element for each item in cur.description
 colnames = [desc[0] for desc in cur.description]

 cur.close() # close
 conn.close() # close

 return(colnames, my_data) # return column names AND data

tc = """CREATE TABLE cases (case_id BIGINT PRIMARY KEY,
        casetype_id BIGINT NOT NULL,
        case_year BIGINT NOT NULL,
        judge_id BIGINT NOT NULL,
        category_id BIGINT NOT NULL,
        category_name VARCHAR(255) NOT NULL)"""
conn, cur = get_conn_cur()
cur.execute(tc)
conn.commit()
cur.close()
conn.close()

#Convert data to tuples
cases_np = cases_df.to_numpy()
data_tups = [tuple(x) for x in cases_np]

#Checking Insert query
iq = """INSERT INTO cases(case_id, casetype_id, case_year, judge_id, category_id, category_name) VALUES(%s, %s, %s, %s, %s, %s);"""
iq % data_tups[4]

#Uploading data
conn, cur = get_conn_cur()
cur.executemany(iq, data_tups)
conn.commit()
conn.close()

judges_df = cases.loc[:, ['judge_id','judge_name','party_name','gender_name','race_name',]]

judges_df = judges_df.drop_duplicates()

tc = """CREATE TABLE judges (judge_id BIGINT PRIMARY KEY,
        judge_name VARCHAR(255) NOT NULL,
        party_name VARCHAR(255) NOT NULL,
        gender_name VARCHAR(255) NOT NULL,
        race_name VARCHAR(255) NOT NULL);"""
conn, cur = get_conn_cur()
cur.execute(tc)
conn.commit()
cur.close()
conn.close()

judges_np = judges_df.to_numpy()
data_tups = [tuple(x) for x in judges_np]

iq = """INSERT INTO judges(judge_id, judge_name, party_name, gender_name, race_name) VALUES(%s, %s, %s, %s, %s);"""

#Uploading data
conn, cur = get_conn_cur()
cur.executemany(iq, data_tups)
conn.commit()
conn.close()

casetype_df = cases.loc[:, ['casetype_id','casetype_name']]

# Removing Duplicates
casetype_df = casetype_df.drop_duplicates()

tc = """CREATE TABLE casetype (casetype_id BIGINT PRIMARY KEY,
        casetype_name VARCHAR(255) NOT NULL);"""
conn, cur = get_conn_cur()
cur.execute(tc)
conn.commit()
cur.close()
conn.close()

casetype_np = casetype_df.to_numpy()
data_tups = [tuple(x) for x in casetype_np]

iq = """INSERT INTO casetype(casetype_id, casetype_name) VALUES(%s, %s);"""

#Uploading data
conn, cur = get_conn_cur()
cur.executemany(iq, data_tups)
conn.commit()
conn.close()

run_query("""SELECT COUNT(DISTINCT(judges.judge_id)) FROM cases
    JOIN judges ON cases.judge_id = judges.judge_id
        WHERE casetype_id = (SELECT casetype_id FROM casetype
                  WHERE casetype_name = 'criminal court motions'); """)

# Make a groupby called cases_rollup. This should group by party_name and categrory name. It should aggregate the count and sum of libcon_id
cases_rollup = cases.groupby(['party_name','category_name'])['libcon_id'].agg(['count', 'sum'])

# reset your index
cases_rollup = cases_rollup.reset_index()

# rename your columns now. Keep the first to the same but call the last two 'total_cases' and 'num_lib_decisions'
cases_rollup.rename(columns={'count': 'total_cases', 'sum': 'num_lib_decisions'}, inplace=True)

# make your metric called 'percent_liberal'
cases_rollup['percent_liberal'] = round(cases_rollup['num_lib_decisions'] / cases_rollup['total_cases'] * 100)

# Reorganize cases_rollup
cases_rollup = cases_rollup[['total_cases','party_name','category_name','num_lib_decisions','percent_liberal']]

tc = """CREATE TABLE rollup (total_cases BIGINT PRIMARY KEY,
        party_name VARCHAR(255) NOT NULL,
        category_name VARCHAR(255) NOT NULL,
        num_lib_decisions BIGINT NOT NULL,
        percent_liberal FLOAT NOT NULL);"""
conn, cur = get_conn_cur()
cur.execute(tc)
conn.commit()
cur.close()
conn.close()

rollup_np = cases_rollup.to_numpy()
data_tups = [tuple(x) for x in rollup_np]

q = """INSERT INTO rollup(total_cases, party_name, category_name, num_lib_decisions, percent_liberal) VALUES(%s, %s, %s, %s, %s);"""

#Uploading data
conn, cur = get_conn_cur()
cur.executemany(iq, data_tups)
conn.commit()
conn.close()