#!/usr/bin/env python3
from __future__ import print_function
import psycopg2 as pg


def printData(data, sender):
    k = 0
    for i in data:
        k += 1
        for j in range(2):
            if j == 1:
                print(i[j], end=" " + sender)
                print()
            else:
                print("  ", k, ".", i[j], end=" --> ")
    print()


try:
    con = pg.connect(database='news')
except psycopg2.Error as e:
    print ("Unable to connect to the database")
cur = con.cursor()
print("1. What are the three most popular articles of all time?")
cur.execute("""select title,count(*) from log, articles
                 where articles.slug = substring(path, 10)
                 group by substring(path,10), title
                 order by count(*) desc limit 3; """)
data = cur.fetchall()
sender = 'views'
printData(data, sender)

print("2. Who are the most popular authors of all time?")
cur.execute("""select name, count(*)  from authors,articles,log
                where articles.slug=substring(log.path,10)
                and authors.id=articles.author
                group by name order by count(*) desc;""")
data = cur.fetchall()
printData(data, sender)

print("3. The day with more than 1% of the requests that led to an error?")
cur.execute("""with result as (select date(time) as day,
                 round(100.0 * sum(case log.status when '404 NOT FOUND'
                 then 1 else 0 end)/count(log.status),3)
                 as error from log group by date(time) order by error desc)
                 select day,error from result where error >1;""")
data = cur.fetchall()
sender = "%"
printData(data, sender)
con.close()
