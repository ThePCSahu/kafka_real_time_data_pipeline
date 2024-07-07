import mysql.connector
from mysql.connector import Error


class MysqlProductReader:
    def __init__(self, host, user, password, database, port=3306, fetch_query_location="latest_products_query.sql"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        with open(fetch_query_location, 'r') as file:
            self.fetch_query = file.read()

    # Connect to mysql database
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password,
                database = self.database
                )
            if self.connection.is_connected():
                self.connection.autocommit = True
                print("Successfully connected to the database")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            self.connection = None
    # Connect to mysql database
    def disconnect(self):
        if(self.connection is not None and self.connection.is_connected()):
            self.connection.close()
            print("Database connection closed")

    # Perform incremental fetch from mysql
    def fetch_products_by_last_read_timestamp(self,last_read_timestamp):
        if self.connection is None or not self.connection.is_connected():
            print("Not connected to the database")
            return []
        
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(self.fetch_query,(last_read_timestamp,))
            results = cursor.fetchall()
            cursor.close()
            return results
        except Error as e:
            print(f"Error fetching data: {e}")
            return []
    
        # Perform incremental fetch from mysql
    def fetch_max_last_updated(self):
        if self.connection is None or not self.connection.is_connected():
            print("Not connected to the database")
            return []
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT MAX(last_updated) FROM buyonline.product")
            results = cursor.fetchone()
            max_date = results[0]
            cursor.close()
            return max_date
        except Error as e:
            print(f"Error fetching data: {e}")
            return []

