# -*- coding: utf-8 -*-
"""
Class to handle the Database connection
The database to use is MySQL.
"""
import mysql.connector

class DB():
    def __init__(self, host, port, user, password, db = None, logger = None):
        """
        Initialization of the databse
        The IP and PORT to the MySQL server is needed, also the user and
        password.
        Also the database name must be specified.
        """
        
        self.__user = user
        self.__password = password
        self.host = host
        self.port = port
        self.db = db
        self.con = None
        self.cursor = None
        self.tables = None
        
        if logger is None:
            from utils.createLogger import createLogger
            self.logger = createLogger()
        else:
            self.logger = logger
        
    def connect(self):
        """
        Create the connection with the database in the server.
        """
        self.con = mysql.connector.connect(user = self.__user,
                                      password = self.__password,
                                      host=self.host,
                                      port=self.port)
        try:
            self.cursor = self.con.cursor(dictionary=True)
            if self.db is None:
                self.cursor.execute("SHOW databases;")
                print("The available databases are:\n")
                print(self.cursor.fetchall())
                self.db = input('Input the database you want to use or create:')
            self.cursor.execute("USE {}".format(self.db))
            self.list_tables()
            return self.con
        except mysql.connector.Error as err:
            self.logger.error("Database {} does not exists.".format(self.db))
            if err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
                self.create_db()
                self.logger.info("Database {} created successfully.".format(self.db))
                self.cursor.execute("USE {}".format(self.db))
                self.list_tables()
                self.logger.debug("Now you are using the database: {}".format(self.db))
                return self.con
    
    def close(self):
        self.con.close()
 

    def check_connection(self):
        if self.con == None or not self.con.is_connected:
            self.logger.debug("Restoring connection")
            self.connect()
            self.cursor = self.con.cursor(dictionary=True)
        if self.cursor is None:
            self.logger.debug("The cursor is None, re setting it")
            self.cursor = self.con.cursor(dictionary=True)
            

    def exist_table(self, table):
        """
        check if a table exist
        """
        table = self.rename_table_query(table)
        self.check_connection()
        query = "SHOW TABLES LIKE '{}';".format(table)
        self.cursor.execute(query)
        if len(self.cursor.fetchall())>0:
            return True
        else:
            return False

    def create_db(self):
        """
        Create a database file if the file is not found.
        """
        try:
            self.check_connection()
            self.cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.db))
            self.con.database = self.db
        except mysql.connector.Error as err:
            self.logger.error("Failed creating database: {}".format(err))

    def create_ticker_table(self, ticker):
        """
        Create a table in the DB with the name provided.
        """
        ticker = self.rename_table_query(ticker)
        self.check_connection()
        try:
            self.check_connection()
            query = "CREATE TABLE {} (event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, trade_id TEXT NOT NULL, price REAL, quantity REAL, bid_id TEXT, ask_id TEXT, trade_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, maker BOOL);".format(ticker)
            print(query)
            self.cursor.execute(query)
            self.con.commit()
            self.logger.debug("Table {} was created in the DB".format(ticker))
            #update the list of tables
            self.list_tables()
            return True
        except:
            self.logger.exception("Error trying to create the ticker table")
            return False

    def list_tables(self):
        """
        list all the tables in the database
        """
        query = "SHOW TABLES;"
        self.check_connection()
        self.cursor.execute(query)
        self.tables = self.cursor.fetchall()
        return self.tables
        
    def describe_table(self, table):
        """
        Describe the database table
        """
        table = self.rename_table_query(table)
        self.check_connection()            
        self.cursor.execute("DESCRIBE {}".format(table))
        print(self.cursor.fetchall())

    def remove_table(self, table):
        """
        Remove the table specified
        """
        self.check_connection()
        table = self.rename_table_query(table)
        
        answer = input('Are you sure you want to delete the table {} (y/n): '.format(table))
        if answer.lower() == 'y':
            self.cursor.execute("DROP TABLE {};".format(table))
            self.con.commit()
        
    def read_table(self, table='BTCUSDT', start_date=None, end_date=None):
        """
        Read all the data in the table specified ordered by event_time from 
        newest to oldest.
        The table name is equivalent to the ticker of the asset.
        if start_date or end_date or both are defined, the result will be 
        filtered by the dates. This dates must be strings with the format
        '%Y-%m-%d %H:%M:%S'
        This method return a list of dicts, with the column name and the value.
        """
        table = self.rename_table_query(table)
        self.check_connection()
        if start_date and end_date:
            query = "SELECT * FROM {} WHERE trade_time between '{}' and '{}' ORDER BY trade_time ASC ;".format(table, start_date, end_date)
        elif start_date:
            query = "SELECT * FROM {} WHERE trade_time>'{}' ORDER BY trade_time ASC ;".format(table, start_date)
        else:
            query = "SELECT * FROM {} ORDER BY trade_time ASC;".format(table)
        print(query)
        self.cursor.execute(query)
        return self.cursor.fetchall()
            
    def read_last_row(self, table='BTCUSDT'):
        """
        read the last row in time from the database
        """
        self.check_connection()
        self.rename_table_query(table)
        query = "SELECT * FROM {} ORDER BY event_time DESC LIMIT 1;".format(table)
        self.cursor.execute(query)
        return self.cursor.fetchall()[0]

    def read_last_price(self, table='BTCUSDT'):
        """
        read the last row in time from the database
        """
        self.check_connection()
        self.rename_table_query(table)
        query = "SELECT price FROM {} ORDER BY event_time DESC LIMIT 1;".format(table)
        self.cursor.execute(query)
        return self.cursor.fetchone()

    def rename_table_query(self,table):
        """
        rename the table name to remove spaces, comma, dots, hyphens and slashs
        """
        return table.replace(".","").replace("-","").replace('/','').replace(" ","").upper()


    def appendMD(self, table, MD):
        """
        Method to append the market data to the specified table
        If the table is not found, it will be created.
        The MD is an Dict where the key are the columns of the Ticker Table.
        """
        table = self.rename_table_query(table)
        self.check_connection()
        
        if not self.exist_table(table):
            self.create_ticker_table(table)
        
        placeholders = ', '.join(['%s'] * len(MD))
        columns = ', '.join(MD.keys())
        query = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
        try:
            self.cursor.execute(query, list(MD.values()))
            self.con.commit()
        except Exception:
            self.logger.exception("Error trying to insert MarketData into the table {}".format(table))
        