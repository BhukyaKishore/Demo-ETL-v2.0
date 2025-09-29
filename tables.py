'''
Author : Bhavani Kishore
Date : 26/09/2025

Defines MySQL database tables with appropriate schemas, primary keys, and foreign key constraints.
Ensures relational integrity and cascading actions between tables.
Creates tables if they don’t exist to prepare the database for data loading.

'''
#importing files
import mysql.connector
import datetime
import json
import pandas as pd
# load config
with open('config.json', 'r') as file:
    config = json.load(file)

mydb = None
mycursor = None

def db_connect():
    """ Ensure the database exists, then connect and provide a global mycursor."""
    global mydb, mycursor
    # if already connected and healthy, reuse
    if mydb is not None and hasattr(mydb, "is_connected") and mydb.is_connected():
        return mydb

    try:
        dbname = config['database']['db']

        # 1) Temporary connection (no database) to ensure database exists
        tmp = mysql.connector.connect(
            host=config['database']['host'],
            user=config['database']['user'],
            password=config['database']['password']
        )
        tmp_cursor = tmp.cursor()
        # create DB (use backticks to protect identifier)
        tmp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{dbname}`;")
        tmp.commit()   # safe to call
        tmp.close()

        # 2) Now connect using the database
        mydb = mysql.connector.connect(
            host=config['database']['host'],
            user=config['database']['user'],
            password=config['database']['password'],
            database=dbname
        )
        mycursor = mydb.cursor()
        return mydb

    except Exception as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in db_connect: {err}\n")
        return None


def users():
    """
    Create the 'users' table in the database if it does not exist.
    Defines user-related fields and timestamps with default metadata.
    Logs any errors encountered during table creation.
    """

    db_connect()
    try:
        mycursor.execute("""
            create table if not exists users (
                id                   int primary key,
                name                 varchar(600),
                username             varchar(600),
                email                varchar(600),
                phone                varchar(600),
                website              varchar(600),
                address_street       varchar(600),
                address_suite        varchar(600),
                address_city         varchar(600),
                address_zipcode      varchar(600),
                address_geo_lat      varchar(600),
                address_geo_lng      varchar(600),
                company_name         varchar(600),
                company_catchPhrase  varchar(600),
                company_bs           varchar(600),
                created_by VARCHAR(255) DEFAULT "Bavani Kishore",
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(255) DEFAULT "Bavani Kishore",
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
    except mysql.connector.Error as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in users: {err}\n")

def posts():
    """
    Create the 'posts' table with a foreign key referencing 'users'.
    Includes post-specific fields and timestamps.
    Logs any errors encountered during table creation.
    """

    db_connect()
    try:
        mycursor.execute("""
            create table if not exists posts (
                userId    int,
                id        int primary key,
                title     varchar(600),
                body      varchar(600),
                constraint posts_fk foreign key (userId) references users(id) on delete cascade,
                created_by VARCHAR(255) DEFAULT "Bavani Kishore",
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(255) DEFAULT "Bavani Kishore",
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
    except mysql.connector.Error as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in posts: {err}\n")


def comments():
    """
    Create the 'comments' table with a foreign key referencing 'posts'.
    Defines comment-specific fields and timestamps.
    Logs any errors encountered during table creation.
    """

    db_connect()
    try:
        mycursor.execute("""
            create table if not exists comments (
                postId    int,
                id        int primary key,
                name      varchar(600),
                email     varchar(600),
                body      varchar(600),
                constraint comments_fk foreign key (postId) references posts(id) on delete cascade,
                created_by VARCHAR(255) DEFAULT "Bavani Kishore",
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(255) DEFAULT "Bavani Kishore",
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
    except mysql.connector.Error as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in comments: {err}\n")

def albums():
    """
    Create the 'albums' table with a foreign key referencing 'users'.
    Includes album-specific fields and timestamps.
    Logs any errors encountered during table creation.
    """

    db_connect()
    try:
        mycursor.execute("""
            create table if not exists albums (
                userId    int,
                id        int primary key,
                title     varchar(600),
                constraint albums_fk foreign key (userId) references users(id) on delete cascade,
                created_by VARCHAR(255) DEFAULT "Bavani Kishore",
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(255) DEFAULT "Bavani Kishore",
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
    except mysql.connector.Error as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in albums: {err}\n")

def photos():
    """
    Create the 'photos' table with a foreign key referencing 'albums'.
    Defines photo-specific fields and timestamps.
    Logs any errors encountered during table creation.
    """

    db_connect()
    try:
        mycursor.execute("""
            create table if not exists photos (
                albumId       int,
                id            int primary key,
                title         varchar(600),
                url           varchar(600),
                thumbnailUrl  varchar(600),
                constraint photos_fk foreign key (albumId) references albums(id) on delete cascade,
                created_by VARCHAR(255) DEFAULT "Bavani Kishore",
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(255) DEFAULT "Bavani Kishore",
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
    except mysql.connector.Error as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in photos: {err}\n")


def todos():
    """
    Create the 'todos' table with a foreign key referencing 'users'.
    Defines task-specific fields including completion status and timestamps.
    Logs any errors encountered during table creation.
    """

    db_connect()
    try:
        mycursor.execute("""
            create table if not exists todos (
                userId     int not null,
                id         int primary key,
                title      varchar(600),
                completed  boolean,
                constraint todos_fk foreign key (userId) references users(id) on delete cascade,
                created_by VARCHAR(255) DEFAULT "Bavani Kishore",
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(255) DEFAULT "Bavani Kishore",
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)
    except mysql.connector.Error as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in todos: {err}\n")


def inserting_data(path :str, name:str):
    """
    Read data from a CSV file and insert it into the specified database table.
    Uses 'ON DUPLICATE KEY UPDATE' to update existing rows based on primary key conflicts.
    Handles column name cleanup, prepares parameterized queries, and commits transactions.
    Logs any errors during the data insertion process.
    
    """

    db_connect()  # Ensure connection is active
    try:
        df = pd.read_csv(path)
        if df.empty:
            return

        # Replace dot in column names with underscore (for SQL)
        clean_columns = [col.replace('.', '_') for col in df.columns]
        columns_sql = [f"`{col}`" for col in clean_columns]
        placeholders = ', '.join(['%s'] * len(columns_sql))

        # Build the ON DUPLICATE KEY UPDATE clause (excluding 'id')
        update_clause = ', '.join([
            f"{col}=VALUES({col})"
            for col in columns_sql
            if col != '`id`'  # do not try to update the primary key
        ])

        insert_sql = f"""
            INSERT INTO `{name}` ({', '.join(columns_sql)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause};
        """

        # Prepare data
        data_to_insert = []
        for _, row in df.iterrows():
            row_values = []
            for val in row:
                if pd.isna(val):
                    row_values.append(None)
                elif isinstance(val, bool):
                    row_values.append(val)
                else:
                    row_values.append(val)
            data_to_insert.append(tuple(row_values))

        # Execute batch insert/update
        mycursor.executemany(insert_sql, data_to_insert)
        mydb.commit()

    except Exception as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in inserting_data for {name}: {err} \n")

    except Exception as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in inserting_data for {name}: {err} \n")
