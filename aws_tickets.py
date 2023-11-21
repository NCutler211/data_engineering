"""
__author__ = "Nate Cutler"
__credits__ = some functions provided by Dan Charbonneau
__email__ = "ncutler211@gmail.com"
"""

import psycopg2
import pandas as pd

AWS_host_name = "ticketsdb.cp9xqenk8fzu.us-east-1.rds.amazonaws.com"
AWS_dbname = "ticketsdb"
AWS_user_name = "postgres"
AWS_password = "ista322ticketsdb"

def get_conn_cur():
    """
    Establishes a connection to the PostgreSQL database and returns the connection and cursor objects.
    
    Returns:
    tuple: PostgreSQL connection and cursor objects.
    """
    conn = psycopg2.connect(
        host=AWS_host_name,
        database=AWS_dbname,
        user=AWS_user_name,
        password=AWS_password,
        port='5432'
    )
    cur = conn.cursor()
    return conn, cur

def run_query(query_string):
    """
    Executes a SQL query and returns column names and fetched data.
    
    Args:
    query_string (str): SQL query string to execute.
    
    Returns:
    tuple: Column names and fetched data.
    """
    conn, cur = get_conn_cur()
    cur.execute(query_string)
    my_data = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return colnames, my_data

def get_column_names(table_name):
    """
    Retrieves column names of a specific table in the database.
    
    Args:
    table_name (str): Name of the table.
    
    Returns:
    list: Column names of the table.
    """
    conn, cur = get_conn_cur()
    column_name_query =  """SELECT column_name FROM information_schema.columns
           WHERE table_name = '%s' """ %table_name
    cur.execute(column_name_query)
    my_data = cur.fetchall()
    cur.close()
    conn.close()
    return my_data

def get_table_names():
    """
    Retrieves names of tables in the database.
    
    Returns:
    list: Names of tables.
    """
    conn, cur = get_conn_cur()
    table_name_query = """SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public' """
    cur.execute(table_name_query)
    my_data = cur.fetchall()
    cur.close()
    conn.close()
    return my_data

def sql_head(table_name):
    """
    Retrieves the first 5 rows of a specific table.
    
    Args:
    table_name (str): Name of the table.
    
    Returns:
    pd.DataFrame: DataFrame containing the first 5 rows of the table.
    """
    conn, cur = get_conn_cur()
    sales_df = pd.read_sql(
        """ SELECT * FROM %s LIMIT 5;""" %table_name, conn)
    return sales_df

# ... (Remaining code and SQL queries can be organized similarly as per their functionality)


# Get big venues... so those with >= than 10000 seats
sq = """ SELECT * FROM venue
          WHERE venue_seats > 10000
          LIMIT 5;"""
run_query(sq)

# Get venues in AZ
sq = """ SELECT * FROM venue
          WHERE venue_state = 'AZ'
          LIMIT 5;"""
run_query(sq)

#Get users who have a first name that starts with H
sq = """ SELECT * FROM users
          WHERE first_name LIKE 'H%'
          LIMIT 5;"""
run_query(sq)

# Get all .edu email addresses... just the email addresses
sq = """ SELECT email FROM users
          WHERE email LIKE '%.edu'
          LIMIT 5;"""
run_query(sq)

# Find the top five venues that hosted the most events: Alias the count of events as 'events_hosted'. Also return the venue ID
sq = """ SELECT venue_id, COUNT(event_id) as events_hosted FROM event
          GROUP BY venue_id
          ORDER BY events_hosted DESC
          LIMIT 5;"""
run_query(sq)

# Get the number of events hosted in each month. You'll need to use `date_part()` in your select to select just the months.
# Alias this as 'month' and then the count of the number of events hosted as 'events_hosted'
sq = """ SELECT date_part('month', start_time) as month, COUNT(event_id) as events_hosted FROM event
          GROUP by month;"""
run_query(sq)

# Get the top five sellers who made the most commission. Alias their total commission made as 'total_com'.
# Also get their average commission made and alias as 'avg_com'. Be sure to also display the seller_id
sq = """ SELECT seller_id, SUM(commission) as total_com, AVG(commission) as avg_com FROM sales
          GROUP BY seller_id
          ORDER BY total_com DESC
          LIMIT 5;"""
run_query(sq)

### HAVING application
# Using the same query as the last groupby, instead of getting the top five sellers get all sellers who have made a total commission greater than $4000
sq = """ SELECT seller_id, SUM(commission) as total_com, AVG(commission) as avg_com FROM sales
          GROUP BY seller_id
          HAVING SUM(commission) > 4000;"""
run_query(sq)

# Using the same query as the first groupby, instead of returning the top five venues, return just the ID's of venues that have had greater than 60 events
sq = """ SELECT venue_id FROM event
          GROUP BY venue_id
          HAVING COUNT(event_id) > 60;"""
run_query(sq)

# Join users information to each sale
sq = """ SELECT * from users
          LEFT JOIN sales ON sales.buyer_id = users.user_id
          LIMIT 5;"""
run_query(sq)

# For each event attach the venue information
sq = """ SELECT * from event
          LEFT JOIN venue ON venue.venue_id = event.venue_id
          LIMIT 5;"""
run_query(sq)

# Get all purchases from users who live in Arizona
sq = """ SELECT * from sales
          WHERE buyer_id in (SELECT user_id FROM users WHERE state = 'AZ')
          LIMIT 5;"""
run_query(sq)

# Get event information for all events that took place in a venue where the name ended in 'Stadium'
sq = """ SELECT * from event
          WHERE venue_id in (SELECT venue_id FROM venue WHERE venue_name LIKE '%Stadium')
          LIMIT 5;"""
run_query(sq)

# Get event information where the total sales for that event were greater than $50000
sq = """ SELECT * FROM event
          WHERE event_id in (SELECT event_id FROM sales GROUP BY event_id HAVING SUM(price_paid) > 50000)
          LIMIT 5;"""
run_query(sq)