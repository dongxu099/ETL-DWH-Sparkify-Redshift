import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """drop all the tables listed in drop_table_queries
    if they exist in the redshift cluster
    
    :param cur: cursory object associated with the connection. Allows to execute SQL commands.
    :param conn: psycopg2.connection object with access to Postgres database
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue dropping table: " + query)
            print(e)
    print("Tables dropped successfully.")
    
    
def create_tables(cur, conn):
    """create all the tables listed in create_table_queries
    to the redshift cluster
    
    :param cur: cursory object associated with the connection. Allows to execute SQL commands.
    :param conn: psycopg2.connection object with access to Postgres database
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue creating table: " + query)
            print(e)
    print("Tables created successfully.")

    
def main():
    """Read credentials from dwh.cfg and connect to the Redshift cluster,
    drop (if necessary) and create tables, close DB connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Dropping existing tables if any...')
    drop_tables(cur, conn)
    
    print('Creating tables...')
    create_tables(cur, conn)

    conn.close()
    print('Completed.')
    
if __name__ == "__main__":
    main()