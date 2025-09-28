
## 🚀 Project Overview

This project is a robust Python-based system designed for efficient data ingestion and comprehensive quality control. It automates the process of extracting raw data from CSV files, applying a series of predefined data quality checks and transformations, and subsequently loading the cleaned and validated data into a MySQL relational database. The primary objective is to ensure data integrity, consistency, and reliability before persistent storage, which is crucial for downstream data analysis, reporting, and application functionality.

## 📂 Project Structure

The system is modular, comprising the following key components:

-   `app.py`: The central orchestration script that manages the entire data pipeline, from reading raw data to applying quality checks and loading into the database.
-   `dq.py`: A dedicated module containing a library of reusable data quality validation and transformation functions.
-   `config.json`: A crucial configuration file that externalizes all dynamic parameters, including database connection details, paths for source CSV files, and destination paths for cleaned data.
-   `tables.py` (implicitly used by `app.py`): This module (not directly provided in this context but inferred from `app.py`) is responsible for handling MySQL database connections, ensuring database existence, creating necessary tables, and performing bulk data insertion.

## ✨ Features

-   **Automated ETL Pipeline**: Streamlined process for Extracting, Transforming, and Loading data.
-   **Comprehensive Data Quality**: Implements various checks (e.g., primary key, foreign key, data type, format validation) to ensure data integrity.
-   **Configurable**: Easy-to-modify `config.json` for database credentials, file paths, and other settings.
-   **Error Logging**: Detailed logging of all data quality issues and database errors to `error.txt`.
-   **Modular Design**: Separation of concerns for easy maintenance and extension.

## 🛠️ Setup and Installation

Follow these steps to get the project up and running on your local machine.

### Prerequisites

Before you begin, ensure you have the following installed:

-   **Python 3.x**: Download and install from [python.org](https://www.python.org/).
-   **MySQL Database Server**: A running MySQL instance. You can download it from [mysql.com](https://www.mysql.com/downloads/).

### Clone the Repository

```bash
git clone <https://github.com/BhukyaKishore/Demo-ETL-v2.0.git>
cd <Demo-ETL-v2.0>
```

### Install Dependencies

Install the required Python libraries using `pip`:

```bash
pip install pandas mysql-connector-python
```

### Configuration

1.  **`config.json`**: Create a `config.json` file in the root directory of your project. This file will hold your database credentials and file paths. An example structure is provided below. **Replace placeholder values with your actual database details and ensure file paths are correct.**

    ```json
    {
      "database": {
        "host": "localhost",
        "user": "root",
        "password": "your_mysql_password",
        "db":"your_database_name"
      },
      "srcpath":{
        "albums":"./src/albums.csv",
        "comments":"./src/comments.csv",
        "photos":"./src/photos.csv",
        "posts":"./src/posts.csv",
        "todos":"./src/todos.csv",
        "users":"./src/users.csv"
      },
      "tables":{
        "albums":"albums",
        "comments":"comments",
        "photos":"photos",
        "posts":"posts",
        "todos":"todos",
        "users":"users"
      },
      "distpath":{
        "albums":"./dist/albums.csv",
        "comments":"./dist/comments.csv",
        "photos":"./dist/photos.csv",
        "posts":"./dist/posts.csv",
        "todos":"./dist/todos.csv",
        "users":"./dist/users.csv"
      },
      "names": "[\"users\",\"posts\",\"comments\",\"albums\",\"posts\",\"todos\"]",
      "paths": "[\"./dist/users.csv\",\"./dist/posts.csv\",\"./dist/comments.csv\",\"./dist/albums.csv\",\"./dist/posts.csv\",\"./dist/todos.csv\"]"
    }
    ```

2.  **Source Data Files**: Create a `src/` directory in your project root. Place your raw CSV data files (e.g., `users.csv`, `posts.csv`, `comments.csv`, `albums.csv`, `photos.csv`, `todos.csv`) into this `src/` directory, matching the paths specified in your `config.json`.

## 🚀 How to Run

Once configured, execute the main application script from your terminal:

```bash
python3 app.py
```

Upon execution, the `app.py` script will:

-   Establish a connection to your MySQL database (creating the database if it doesn't exist).
-   Create all necessary tables (`users`, `posts`, `comments`, `albums`, `photos`, `todos`) in the database.
-   Read data from the specified source CSV files.
-   Apply data quality checks and transformations using the rules defined in `dq.py`.
-   Save the cleaned data to new CSV files within a `dist/` directory (which will be created if it doesn't exist).
-   Insert the cleaned data from the `dist/` directory into the corresponding tables in your MySQL database.

## 🐞 Error Logging

Any errors encountered during database operations or data quality checks (e.g., invalid data, connection issues) will be logged to an `error.txt` file in the project root directory. This file is crucial for debugging and monitoring the data ingestion process.


## Thankyou ❤️

