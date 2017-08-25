#! /usr/bin/env python
import psycopg2
#from db import db_session_context

def connect(DB_NAME="news"):
    """Connect to the PostgreSQL database and returns a database connection."""
    try:
        db = psycopg2.connect("dbname={}".format(DB_NAME))
        c = db.cursor()
        return db, c
    except:
        print("Error in connecting to database")


def view_find_three_articles():
    # What are the most popular three articles of all time?
    try:
        db, c = connect()
        query = "select articles.title, count(log.status) as count\
        from articles, log\
        where log.status like '%200%'\
        and log.path like '%' || articles.slug || '%'\
        group by articles.title\
        order by count desc limit 3"
        c.execute(query)
        db.commit()
        db.close()
    except:
        print("Error in creating popular_articles")


def view_find_popular_authors():
    #Who are the most popular article authors of all time?
    db, c = connect()
    query = "select authors.name, count(log.status) as count\
        from articles, log, authors\
        where log.status like '%200%'\
        and articles.author = authors.id\
        and log.path like '%' || articles.slug || '%'\
        group by authors.name\
        order by count desc"
        c.execute(query)
        db.commit()
        db.close()
    except:
        print("Error in creating popular_authors")


def view_find_errors():
    # On which days did more than 1% of requests lead to errors?
    #with db_session_context(DB_NAME) as db:
    db, c = connect()
    query = "create or replace view log_status as select Date,Total,Error,\
        (Error::float*100)/Total::float as Percent from\
        (select time::timestamp::date as Date, count(status) as Total,\
        sum(case when status = '404 NOT FOUND' then 1 else 0 end) as Error\
        from log group by time::timestamp::date) as result\
        where (Error::float*100)/Total::float > 1.0 order by Percent desc"
        c.execute(query)
        db.commit()
        db.close()
    except:
        print("Error in creating log_status")
    

def find_three_articles():
    """Prints most popular three articles of all time"""
    db, c = connect()
    query = "select * from popular_articles limit 3"
    c.execute(query)
    result = c.fetchall()
    db.close()
    print "\nPopular Articles:\n"
    for i in range(0, len(result), 1):
        print "\"" + result[i][0] + "\" - " + str(result[i][1]) + " views"

def find_popular_authors():
    """Prints most popular article authors of all time"""
    db, c = connect()
    query = "select * from popular_authors"
    c.execute(query)
    result = c.fetchall()
    db.close()
    print "\nPopular Authors:\n"
    for i in range(0, len(result), 1):
        print "\"" + result[i][0] + "\" - " + str(result[i][1]) + " views"

def find_errors():
    """Print days on which more than 1% of requests lead to errors"""
    db, c = connect()
    query = "select * from log_status"
    c.execute(query)
    result = c.fetchall()
    db.close()
    print "\nDays with more than 1% of errors:\n"
    for i in range(0, len(result), 1):
        print str(result[i][0])+ " - "+str(round(result[i][3], 2))+"% errors"
#call all the three functions 
if __name__ == '__main__':
    find_three_articles()
    find_popular_authors()
    find_errors()
    """
    #to see the views
    view_find_three_articles()
    view_find_popular_authors()
    view_find_errors()
    """

          
	
