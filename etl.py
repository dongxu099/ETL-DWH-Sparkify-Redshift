import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load JSON input data (log_data and song_data) from the S3 bucket 
    to the staging tables (staging_events and staging_songs) in Redshift
    
    :param cur: cursory object associated with the connection.
    :param conn: psycopg2.connection object with access to Postgres database
    
    Output:
    log_data in staging_events table.
    song_data in staging_songs table.
    """
    for query in copy_table_queries:
        try:
            print('------------------------------------')
            print('------------------------------------')
            print('Processing query: {}'.format(query))            
            cur.execute(query)
            conn.commit()
            print('------------------')
            print('Query processed OK.')        
        except psycopg2.Error as e:
            print("Error: Issue loading staging table: " + query)
            print(e)
    print("Staging tables loaded successfully.")

    
def insert_tables(cur, conn):
    """Insert data from the staging tables to the dimension and fact table
    
    :param cur: cursory object associated with the connection.
    :param conn: psycopg2.connection object with access to Postgres database
    
    Output:
    Dimension tables and fact table with data inserted from staging tables
    """
    for query in insert_table_queries:
        try:
            print('------------------------------------')
            print('------------------------------------')
            print('Processing query: {}'.format(query))
            cur.execute(query)
            conn.commit()
            print('------------------')
            print('Query processed OK.')     
        except psycopg2.Error as e:
            print("Error: Issue inserting records to: " + query)
            print(e)
    print("Analytics tables inserted successfully.")

    
def main():
    '''Connect to DB and call load_staging_tables and insert_tables
    
    Output:
    Analytics tables.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading staging tables from S3...')
    load_staging_tables(cur, conn)
    
    print('Inserting the staging data into star schema tables...')    
    insert_tables(cur, conn)

    conn.close()
    print('Completed')

if __name__ == "__main__":
    main()