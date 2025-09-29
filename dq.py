''''
Implementing data quality rules
Author : BhavaniKishore
Date : 26/09/2025
'''

#importing modules
import pandas as pd


def fk_id_check(df, col, table):
    # Check: non-null, of type int, and value between 0 and 10 inclusive
    is_valid = df[col].notnull()
    invalid_index = df[~is_valid].index.tolist()

    with open('error.txt', 'a') as fs:
        for k in invalid_index:
            fs.write(f'invalid userId in {table} table in id {df.loc[k,'id']} so row is dropped \n')

    # Drop invalid rows
    df = df[is_valid].copy()
    return df



def primary_key_check_num(df, col, table):
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


# df=pd.read_csv('temp.csv')
# primary_key_check_num(df,'id','todos')

def title_check_to_untitled(df,col,table):
    #checking for null values
    valid=df[col].notnull()
    invalid_index=df[~valid].index.tolist()

    with open('error.txt','a') as fs:
        for k in invalid_index:
            fs.write(f' blank {col} is changed to untitled task in table {table} with id {df.loc[k,'id']} \n')
            df.loc[k,col]='untitled'
    return df

def title_check_to_drop(df,col,table):
    #checking for null values
    valid=df[col].notnull()
    invalid_index=df[~valid].index.tolist()

    with open('error.txt','a') as fs:
        for k in invalid_index:
            fs.write(f' blank {col} is changed to untitled task in table {table} with id {df.loc[k,'id']} \n')
    df = df[valid].copy()    
    return df

def bool_check(df, col, table):
    bools = df[col].astype(str)
    is_valid = bools.isin(['True', 'False'])
    invalid_index = df[~is_valid].index.tolist()
    with open('error.txt', 'a') as fs:
        for k in invalid_index:
            fs.write(f'invalid boolean in {table} table at id {df.loc[k,'id']} (value={df.loc[k, col]}) so row is dropped\n')
    return df[is_valid].copy()    

# df=pd.read_csv('temp.csv')
# bool_check(df,'completed','todo')

def name_check_to_anonymous(df,col,table):
    #checking for null values
    valid=df[col].notnull()
    invalid_index=df[~valid].index.tolist()
    with open('error.txt','a') as fs:
        for k in invalid_index:
            fs.write(f' blank {col} is changed to anonymous in table {table} with id {df.loc[k,'id']} \n')
            df.loc[k,col]='anonymous'
    return df


def email_check_drop(df,col,table):
    pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    is_valid=df[col].astype(str).str.match(pattern,na=False)
    invalid_index=df[~is_valid].index.tolist()
    with open ('error.txt','a') as fs:
        for k in invalid_index:
            fs.write(f'invalide email in {table} table in id {df.loc[k,'id']} so droped from table\n')
        df.drop(k,axis='index')
    return df

def email_check_blank(df,col,table):
    pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    is_valid=df[col].astype(str).str.match(pattern,na=False)
    invalid_index=df[~is_valid].index.tolist()
    with open ('error.txt','a') as fs:
        for k in invalid_index:
            fs.write(f'invalide email in {table} table in id {df.loc[k,'id']} made it blank \n')
            df.loc[k,col]=''
    return df

# df=pd.read_csv('./src/comments.csv')
# email_check_blank(df,'email','comments')
# email_check_drop(df,'email','comments')


def comment_body_blank_drop(df, col, table):
    valid = df[col].notnull()
    invalid_index = df[~valid].index.tolist()
    with open('error.txt', 'a') as fs:
        for k in invalid_index:
            fs.write(f'null body in {table} table at id {df.loc[k, "id"]} row is dropped\n')
    df = df[valid].copy()
    return df


# df=pd.read_csv('temp.csv')
# comment_body_blank_drop(df,'body','comments')

def url_check_drop(df,col,table):
    pattern=r'https?://(?:www\.)?\S+|www\.\S+'
    is_valid=df[col].astype(str).str.match(pattern,na=False)
    invalid_index=df[~is_valid].index.tolist()
    with open ('error.txt','a') as fs:
        for k in invalid_index:
            fs.write(f'invalide url in {table} table in id {df.loc[k,'id']} so droped from table\n')
            df.drop(k,axis='index')
    return df

# df=pd.read_csv('temp.csv')
# url_check_drop(df,'url','photos')

def url_check_null(df,col,table):
    pattern=r'https?://(?:www\.)?\S+|www\.\S+'
    is_valid=df[col].astype(str).str.match(pattern,na=False)
    invalid_index=df[~is_valid].index.tolist()
    with open ('error.txt','a') as fs:
        for k in invalid_index:
            fs.write(f"invalide thumbnailUrl in {table} table in id {df.loc[k,'id']} so replace it with a common url \n ")
            df.loc[k,col]='https://surl.li/yfoimd'
    return df

# df=pd.read_csv('temp.csv')
# url_check_null(df,'thumbnailUrl','photos')



def username_check_fill(df,col,table):
    valid=df[col].notnull()
    invalid=df[~valid].index.tolist()
    with open ('error.txt','a') as fs:
        for k in invalid:
            fs.write(f"invalide usernames in {table} table in id {df.loc[k,'id']} so replace it with a {df.loc[k,'name']+str(df.loc[k,'name'])} \n ")
            df.loc[k,col]=df.loc[k,'name']+str(df.loc[k,'name'])
    return df

# df=pd.read_csv('temp.csv')
# username_check_fill(df,'username','users')



def phone_check(df,col,table):
    pattern='^[0-9]{10}$'
    is_valid=df[col].astype(str).str.match(pattern,na=False)
    invalid_index=df[~is_valid].index.tolist()
    with open ('error.txt','a') as fs:
        for k in invalid_index:
            fs.write(f"invalide phone number in {table} table in id {df.loc[k,'id']} so made it null \n ")
            df.loc[k,col]=''
    return df

# df=pd.read_csv('temp.csv')
# phone_check(df,'phone','users')

