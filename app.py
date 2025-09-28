import tables as t
import dq
import pandas as pd
import json
import datetime
import time


# load config
with open('config.json', 'r') as file:
    config = json.load(file)

#creating tables
t.users()
t.posts()
t.comments()
t.albums()
t.photos()
t.todos()

#users table checkes
# todo_df=pd.read_csv(config["srcpath"]["users"])
# table=config["tables"]["users"]


#posts table checkes
posts_df=pd.read_csv(config["srcpath"]["posts"])
table=config["tables"]["posts"]

#checks on posts_df
posts_df=dq.primary_key_check_num(posts_df,"id",table)
posts_df=dq.fk_id_check(posts_df,"userId",table)
posts_df=dq.title_check_to_untitled(posts_df,"title",table)
posts_df=dq.comment_body_blank_drop(posts_df,"body",table)
posts_df.to_csv(config["distpath"][table],index=False)

#comments table checkes
comments_df=pd.read_csv(config["srcpath"]["comments"])
table=config["tables"]["comments"]

comments_df=dq.primary_key_check_num(comments_df,"id",table)
comments_df=dq.fk_id_check(comments_df,"postId",table)
comments_df=dq.name_check_to_anonymous(comments_df,"name",table)
comments_df=dq.email_check_blank(comments_df,"email",table)
comments_df=dq.comment_body_blank_drop(comments_df,"body",table)
comments_df.to_csv(config["distpath"][table],index=False)


#albums table checkes
albums_df=pd.read_csv(config["srcpath"]["albums"])
table=config["tables"]["albums"]

albums_df=dq.primary_key_check_num(albums_df,"id",table)
albums_df=dq.fk_id_check(albums_df,"userId",table)
albums_df=dq.title_check_to_untitled(albums_df,"title",table)
albums_df.to_csv(config["distpath"][table],index=False)



#photos table checkes
photos_df=pd.read_csv(config["srcpath"]["photos"])
table=config["tables"]["photos"]

photos_df=dq.primary_key_check_num(photos_df,"id",table)
photos_df=dq.fk_id_check(photos_df,"albumId",table)
photos_df=dq.title_check_to_untitled(photos_df,"title",table)
photos_df=dq.url_check_drop(photos_df,'url',table)
photos_df=dq.url_check_null(photos_df,'thumbnailUrl',table)
photos_df.to_csv(config["distpath"][table],index=False)


#todo table checkes
todo_df=pd.read_csv(config["srcpath"]["todos"])
table=config["tables"]["todos"]

#checks on todo_df
todo_df=dq.fk_id_check(todo_df,"userId",table)
todo_df=dq.primary_key_check_num(todo_df,"id",table)
todo_df=dq.title_check_to_drop(todo_df,"title",table)
todo_df=dq.bool_check(todo_df,"completed",table)
todo_df.to_csv(config["distpath"][table],index=False)

#inserting values into database
try:
    names=['users','posts','comments','albums','posts','todos']
    paths=['./dist/users.csv','./dist/posts.csv','./dist/comments.csv','./dist/albums.csv','./dist/posts.csv','./dist/todos.csv']
    for i in range(len(names)):
        t.inserting_data(paths[i],names[i])
    # while(True):
    #     time.sleep(60*60) #execute for every hour 
except Exception as err:
            with open("error.txt", "a") as fs:
                fs.write(f"{datetime.datetime.now()} Error in db_connect: {err}\n")