''''
Author : Bhavani Kishore
Date : 26/09/2025

description:
dq.py handles data quality checks before loading data into the database.
It validates columns for nulls, data types, value ranges, and foreign key integrity.
Invalid rows are logged and dropped to ensure clean, consistent data.
'''

#importing modules
import pandas as pd
import datetime

try:
    def fk_id_check(df, col:str, table:str, parent_df, parent_key:str):
        # Check: non-null, of type int, and value between 0 and 10 inclusive
        is_valid = df[col].notnull()
        df = df[is_valid].copy()
        # Step 2: Log invalid rows from basic check
        invalid_rows = df.loc[~is_valid].index.tolist()
        with open('error.txt', 'a') as fs:
            for k in invalid_rows:
                fs.write(f'invalid {col} in {table} table in id {df.loc[k, "id"]} row dropped (invalid type or value out of range)\n')

        # Step 3: Check foreign key existence in parent table
        valid_fk_values = set(parent_df[parent_key])
        is_fk_present = df[col].isin(valid_fk_values)

        # Log missing FK references
        with open('error.txt', 'a') as fs:
            for k in df[~is_fk_present].index:
                fs.write(f'{col} in {table} table in id {df.loc[k, "id"]} not found in parent table row dropped\n')

        # Step 4: Keep only rows with valid FK references
        df = df[is_fk_present].copy()
        # Drop invalid rows
        return df


    def primary_key_check_num(df, col:str, table:str):
        # Condition: must be number (int or float) and not null
        is_number = df[col].apply(lambda x: isinstance(x, (int, float)))
        not_null = df[col].notnull()
        # Check uniqueness (duplicated values are invalid)
        is_unique = ~df[col].duplicated(keep=False)
        # Combine conditions
        is_valid = is_number & not_null & is_unique
        # Get invalid row indexes
        invalid_index = df[~is_valid].index.tolist()

        # Log invalid rows
        with open('error.txt', 'a') as fs:
            for k in invalid_index:
                fs.write(f'invalid {col}s in {table} table so row is dropped\n')

        # Keep only valid rows
        df = df[is_valid].copy()
        return df


    def title_check_to_untitled(df,col :str, table :str):
        #checking for null values
        valid=df[col].notnull()
        invalid_index=df[~valid].index.tolist()

        with open('error.txt','a') as fs:
            for k in invalid_index:
                fs.write(f' blank {col} is changed to untitled task in table {table} with id {df.loc[k,"id"]} \n')
                df.loc[k,col]='untitled'
        return df

    def title_check_to_drop(df,col :str, table :str):
        #checking for null values
        valid=df[col].notnull()
        invalid_index=df[~valid].index.tolist()

        with open('error.txt','a') as fs:
            for k in invalid_index:
                fs.write(f' blank {col} is changed to untitled task in table {table} with id {df.loc[k,"id"]} \n')
        df = df[valid].copy()    
        return df

    def bool_check(df, col:str, table:str):
        bools = df[col].astype(str)
        is_valid = bools.isin(['True', 'False'])
        invalid_index = df[~is_valid].index.tolist()
        with open('error.txt', 'a') as fs:
            for k in invalid_index:
                fs.write(f'invalid boolean in {table} table at id {df.loc[k,"id"]} (value={df.loc[k, col]}) so row is dropped\n')
        return df[is_valid].copy()    


    def name_check_to_anonymous(df,col :str, table :str):
        #checking for null values
        valid=df[col].notnull()
        invalid_index=df[~valid].index.tolist()
        with open('error.txt','a') as fs:
            for k in invalid_index:
                fs.write(f' blank {col} is changed to anonymous in table {table} with id {df.loc[k,"id"]} \n')
                df.loc[k,col]='anonymous'
        return df


    def email_check_drop(df,col :str, table :str):
        pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        is_valid=df[col].astype(str).str.match(pattern,na=False)
        invalid_index=df[~is_valid].index.tolist()
        with open ('error.txt','a') as fs:
            for k in invalid_index:
                fs.write(f'invalide email in {table} table in id {df.loc[k,"id"]} so droped from table\n')
            df.drop(k,axis='index')
        return df

    def email_check_blank(df,col :str, table :str):
        pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        is_valid=df[col].astype(str).str.match(pattern,na=False)
        invalid_index=df[~is_valid].index.tolist()
        with open ('error.txt','a') as fs:
            for k in invalid_index:
                fs.write(f'invalide email in {table} table in id {df.loc[k,"id"]} made it blank \n')
                df.loc[k,col]=''
        return df



    def comment_body_blank_drop(df, col:str, table:str):
        valid = df[col].notnull()
        invalid_index = df[~valid].index.tolist()
        with open('error.txt', 'a') as fs:
            for k in invalid_index:
                fs.write(f'null body in {table} table at id {df.loc[k, "id"]} row is dropped\n')
        df = df[valid].copy()
        return df


    def url_check_drop(df,col :str, table :str):
        pattern=r'https?://(?:www\.)?\S+|www\.\S+'
        is_valid=df[col].astype(str).str.match(pattern,na=False)
        invalid_index=df[~is_valid].index.tolist()
        with open ('error.txt','a') as fs:
            for k in invalid_index:
                fs.write(f'invalide url in {table} table in id {df.loc[k,"id"]} so droped from table\n')
                df.drop(k,axis='index')
        return df


    def url_check_null(df,col :str, table :str):
        pattern=r'https?://(?:www\.)?\S+|www\.\S+'
        is_valid=df[col].astype(str).str.match(pattern,na=False)
        invalid_index=df[~is_valid].index.tolist()
        with open ('error.txt','a') as fs:
            for k in invalid_index:
                fs.write(f"invalide thumbnailUrl in {table} table in id {df.loc[k,'id']} so replace it with a common url \n ")
                df.loc[k,col]='https://surl.li/yfoimd'
        return df


    def username_check_fill(df,col :str, table :str):
        valid=df[col].notnull()
        invalid=df[~valid].index.tolist()
        with open ('error.txt','a') as fs:
            for k in invalid:
                fs.write(f"invalide usernames in {table} table in id {df.loc[k,'id']} so replace it with a {df.loc[k,'name']+str(df.loc[k,'name'])} \n ")
                df.loc[k,col]=df.loc[k,'name']+str(df.loc[k,'name'])
        return df


    def phone_check(df, col :str, table :str):
        """
        Validate phone numbers to ensure 10-digit numeric format; replace invalid ones with blank.
        """

        pattern='^[0-9]{10}$'
        is_valid=df[col].astype(str).str.match(pattern,na=False)
        invalid_index=df[~is_valid].index.tolist()
        with open ('error.txt','a') as fs:
            for k in invalid_index:
                fs.write(f"invalide phone number in {table} table in id {df.loc[k,'id']} so made it null \n ")
                df.loc[k,col]=''
        return df


except Exception as err:
            with open("error.txt", "a") as fs:
                fs.write(f"{datetime.datetime.now()} Error in data quality rules Implementation: {err}\n")

