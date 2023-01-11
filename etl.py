"""
Script that connects to the redshift cluster, 
loads data from S3 into staging tables 
and then inserts into the facts and dimensionn tables
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Takes data from the S3 buckets and loads into the staging tables on Redshift using the COPY command
    
    Parameters
    ----------
    cur : object
        Database connection cursor object
    
    conn : object
        Database connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Takes data from the staging tables and inserts it into the facts and dimension tables on Redshift
    
    Parameters
    ----------
    cur : object
        Database connection cursor object
    
    conn : object
        Database connection object
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads the config file to get the redshift cluster parameters and connect to redshift.
    Load data into the staging tables
    Load data into the facts and dimension tables
    Close connection to redshift
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()