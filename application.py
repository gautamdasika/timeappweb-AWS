import logging, memcache
import logging.handlers,os,traceback,itertools
import boto,time,sys,random,urllib
from boto.s3.connection import S3Connection
import csv,datetime
from wsgiref.simple_server import make_server
from flask import Flask,render_template,session,request,redirect,url_for
from boto.s3.key import Key
from random import randint
xThousand = 1000
startRange = 20
endRange = 30
collectedIDs = []
result=[]
import MySQLdb
from time import time

application = Flask(__name__)

memc = memcache.Client(['****************.amazonaws.com:11211'], debug=1)

db = MySQLdb.connect(host="**********************.rds.amazonaws.com",
                     user="gautam18",
                     passwd="gautam18",
                     db="ebdb",
                     port=3306)
cur = db.cursor()



conn = S3Connection('***********', '*****************')
bucket = conn.get_bucket('mybucket9843')

application.secret_key = '********************'

@application.route('/')
def index():
    if 'username' in session:
        username = session['username']
        return render_template("index.html",username=username)
    return render_template("index.html")


@application.route('/query',methods=['POST'])
def query():
    if 'username' not in session:
        username=request.form['username']
        session['username']=username
    cur.execute("Show tables")
    tables = ""
    for table in cur.fetchall():
        tables = tables + table[0] + ";"
        # return "".join(table)
        # print tables
    return render_template("query.html", tables=tables)
'''
    ufile=request.files['ufile']
    f = open(ufile.filename,'w')
    contents = ufile.read()
    f.write(contents)
    #mysqlimport()
    sql="LOAD DATA LOCAL INFILE '" + ufile.filename + "' INTO TABLE " + ufile.filename[:-4] + " FIELDS TERMINATED BY ','  ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES"
    cur.execute(sql)
    db.commit()

    cur.execute("Show tables")
    tables=""
    for table in cur.fetchall():
        tables = tables+table[0]+";"
        #return "".join(table)
        #print tables
    return render_template("query.html",tables=tables)
'''
@application.route('/db',methods=['POST'])
def db():
    file1=request.files['uffile']
    a=con()
    cur=a.cursor()
    with open(file1.filename) as f:
        reader = csv.reader(f)
        row1 = next(reader)

    fi=file1.filename[:-4]

    sqldrop="Drop table if exists " +fi
    cur.execute(sqldrop)
    starttime=datetime.datetime.now()
    query="Create table " + fi + "("
    for i in range (0,len(row1)):
        query+= row1[i] + " varchar(30),"
    query+=" id int PRIMARY KEY AUTO_INCREMENT,c_id int)"
    cur.execute(query)
        #print "query fired"
    #s="ALTER TABLE HPI_master MODIFY COLUMN id int AUTO_INCREMENT"
    #cur.execute(s)
    with open (file1.filename,'w') as file2:
        file2.write(file1.read())
        #print file2
    global name
    name=file1.filename
    try:
        sql="LOAD DATA LOCAL INFILE " + file1.filename + " INTO TABLE " + file1.filename[:-4] + " FIELDS TERMINATED BY ','  ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES set c_id = FLOOR(RAND() * (20))"
    #print sql
    except:
        print traceback.print_exc()
    print sql
    cur.execute(sql)
    header=True
    global t_name
    #print row1
    for data in row1:
        if(header==True):
            abc=data
            header=False
        break
        #print abc
    t_name=abc
    a.commit()
    endtime=datetime.datetime.now()
    time=endtime-starttime
    cur.close()
    a.close()
    return str(time)

@application.route('/results',methods=['GET','POST'])
def results():
    query = request.form['query']
    cur.execute(query)
    result = cur.fetchall()
    t1 = '<table style="border: 1px solid black; padding:5px; border-spacing:5px">'
    tdata=""
    for row in result:
        s='</td><td>'.join(str(i) for i in row)
        data="<td>"+s+"</td>"
        tdata=tdata+"<tr>"+data+"</tr>"
    tlast="</table>"
    table=t1+tdata+tlast
    return table


def random():
    tablename = request.form['table']


def uptos3():
    conn = S3Connection('***************', '***********************')
    bucket = conn.get_bucket('mybucket9843')
    k = Key(bucket)
    k.key = 'asg3/UNPrecip.csv'
    start_time=time.time()
    k.set_contents_from_filename('C:/Users/Gautam Dasika/Downloads/UNPrecip.csv')
    k.set_acl('public-read')
    delay=time.time()-start_time
    print delay
def downfroms3():
    key = bucket.get_key('asg3/earthquakes.csv')
    key.get_contents_to_filename('C:/Users/Gautam Dasika/earthquakes.csv')

def mysqlimport():
    op = 'mysqlimport -h **********************.rds.amazonaws.com --fields-terminated-by=, --verbose --local -u gautam18 -p ebdb earthquakes.csv'
    print op
    os.system(op)
    print "done"
    db.commit()

def mysqlinsert():
    urllink = "https://s3.amazonaws.com/mybucket9843/asg3/UNPrecip.csv"
    file = urllib.urlopen(urllink)
    csvfile = csv.reader(file)

    for row in itertools.islice(csvfile, 1, None):
        sql = "INSERT INTO ebdb.UNPrecip VALUES('{0}','{1}',{2},'{3}',{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15});".format(
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12],
            row[13], row[14], row[15])
        print sql
        cur.execute(sql)
        db.commit()

    cur.execute("""CREATE TABLE """)
def loaddata(filename,table_name):
   #with surrogate load_data = """LOAD DATA LOCAL INFILE """+filename+""" INTO TABLE """+table_name+""" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'  LINES TERMINATED BY '\n' IGNORE 1 LINES set rand_col=floor(RAND()*100)"""
    load_data = "LOAD DATA LOCAL INFILE '"+filename+"' INTO TABLE "+table_name+" FIELDS TERMINATED BY ',' ENCLOSED BY '\"'  LINES TERMINATED BY '\r\n' IGNORE 1 LINES"
    print load_data
    cur.execute(load_data)
    db.commit()

#col numbers from zero
def collectids(table_name,col_num):
    global collectedIDs
    global listLength
    selectIDs = "select * from "+table_name
    cur.execute(selectIDs)
    rows = cur.fetchall()
    for col in rows:
        collectedIDs.append(col[col_num])
        listLength = collectedIDs.__len__()
    print collectedIDs

@application.route('/random',methods=['GET','POST'])
def random():
    tname=request.form['tname']
    collectids("earthquakes",11)
    delay=randomquery(20,"earthquakes",20,30,"id")
    return str(delay)


def randomquery(repeat,table_name,start,end,column_name):

    global result
    start_time = time.time()
    query="SELECT * from "+table_name+" WHERE "+column_name+"="
    for num in range(1,repeat):
        randomnum=randint(start,end)
        try:
            executable=query+"'"+collectedIDs[randomnum]+"'"
            print "query is"
            print executable
            cur.execute(executable)
            result.append(cur.fetchall())
            print "result is"
            print result
        except:
            print traceback.print_exc()
    delay = time.time()-start_time
    return str(delay)


@application.route('/r1',methods=['GET','POST'])
def r1():
    f1=request.form['f1']
    f2=request.form['f2']
    q0="SELECT * FROM UNPrecip where "+f1+"="+f2
    print q0
    cur.execute(q0)
    result=cur.fetchall()
    return str(result)


#@application.route('/r1',methods=['GET','POST'])
def r1():
    f1=request.form['f1']
    f2=request.form['f2']
    q0="SELECT *,COUNT(*) FROM UNPrecip where "
    a, b, c = f2.split(" ")
    if (b == 'E'):
        b="="

    else:
        b="<="
    q1=q0+a+b+c
    q2=q1+" and 'Country or Territory' = '"+f1+"'"
    print q2

    starttime=time()
    for i in range(1,250):
        try:
            cacheval = memc.get(q2)
            if not cacheval:
                # cursor = conn.cursor()
                cur.execute(q2)
                rows = cur.fetchall()
                for row in rows:
                    val = row[0]
                    memc.set(key, rows, 60)
                    print "Updated memcached with MySQL data"
            else:
                print "Loaded data from memcached"
                for row in cacheval:
                    # print "%s" % (row[0])
                    continue
        except:
            print traceback.print_exc()
            print "gone to error block"
            continue
    result=cur.fetchall()
    dely=time()-starttime

    print result
    return str(result)+str(dely)

@application.route('/cache',methods=['GET','POST'])
def querycache():
    # def querycache(startRange,endRange):
    #print "Executing ", xThousand
    collectids("earthquakes",11)
    start_time = time.time()  # record start time

    for num in range(1, 20):  # numbers from 1 - xThousand
        randomNumber = randint(startRange, endRange)
        id = collectedIDs[randomNumber]
        dynamicQuery = """SELECT * FROM earthquakes where id = '""" + id + """';"""
        key = id
	print key
        try:
            cacheval = memc.get(key)
            if not cacheval:
                # cursor = conn.cursor()
                cur.execute(dynamicQuery)
                rows = cur.fetchall()
                for row in rows:
                    val = row[0]
                    memc.set(key, rows, 60)
                    print "Updated memcached with MySQL data"
            else:
                print "Loaded data from memcached"
                for row in cacheval:
                    #print "%s" % (row[0])
                    continue
        except:
            print traceback.print_exc()
            print "gone to error block"
            continue
    end_time = time.time()  # record end time
    total_time = end_time - start_time  # calculate total time taken
    #print "start_time ", start_time
    #print "end time ", end_time
    #print "total time ", total_time
    return str(total_time)

if __name__ == "__main__":
    application.run( debug=True)