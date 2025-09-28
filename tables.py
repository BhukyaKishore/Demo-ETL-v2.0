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
    """Ensure the database exists, then connect and provide a global mycursor."""
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


#inserting data into tables
def inserting_data(path,name):
    db_connect() # Ensure connection is active
    try:
        df=pd.read_csv(path)
        if df.empty:
            return
        columns = [f"`{col.replace('.', '_')}`" for col in df.columns]
        placeholders = ', '.join(['%s'] * len(columns))
        # Modified: Use INSERT IGNORE to handle duplicate primary keys
        insert_sql = f"INSERT IGNORE INTO `{name}` ({', '.join(columns)}) VALUES ({placeholders});"

        data_to_insert = []
        for index, row in df.iterrows():
            row_values = []
            for col in df.columns:
                val = row[col]
                if pd.isna(val): # Handle NaN values
                    row_values.append(None)
                elif isinstance(val, bool):
                    row_values.append(val)
                else:
                    row_values.append(val)
            data_to_insert.append(tuple(row_values))

        mycursor.executemany(insert_sql, data_to_insert)
        mydb.commit()

    except Exception as err:
        with open("error.txt", "a") as fs:
            fs.write(f"{datetime.datetime.now()} Error in inserting_data for {name}: {err} \n")


users()
posts()
comments()
albums()
photos()
todos()