'''
   Read yesterday's dump of tweets from HDFS, parse the relevant fields and insert it into the tweets table
   Maintained by Srivatsan Ramanujam <sramanujam@gopivotal.com>, Ronert Obst<robst@gopivotal.com>
   Note:
   =====
   You should have saved your password in ~/.pgpass which should be of the following:
   -----------------
   hostname:port:database:username:password
   -----------------
'''

import subprocess
import logging 
import logging.handlers
import datetime

def execute(parse_sql_file, host, database, user, logfile):
    '''
	    Read the parse_sql_file, substitute the DATE, MONTH, DAY values to 
		yesterday's date and invoke it on HAWQ.
		Inputs:
		=======
		    parse_sql_file: (string) source sql file
			host: (string) hostname on which HAWQ master is running
			database: (string) database name on HAWQ
			user: (string) HAWQ database user
            logfile: (string) logging file to write results of the job
		Outputs:
		========
		Returns whatever the psql command will return upon executing the query.
	''' 
    #Logger
    decalogger = logging.getLogger('decahose_cron_logger')
    decalogger.setLevel(logging.INFO)
    #Formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    #Handler
    rothandler = logging.handlers.RotatingFileHandler(logfile, maxBytes=1024*1024*1024, backupCount=5)  
    rothandler.setFormatter(formatter)  
    decalogger.addHandler(rothandler)   
    
    #We'll load yesterday's data 
    yesterday = datetime.date.fromordinal(datetime.date.today().toordinal()-1)
    year = yesterday.year
    month = yesterday.month
    day = yesterday.day
    
    #Prepare SQL command
    sql = open(parse_sql_file,'r').read().format(
             YEAR = year,
             MONTH = '{month:02d}'.format(month=month),
             DAY = '{day:02d}'.format(day=day)
          )
          
    cmd = ['psql', '-h', host, '-d', database, '-U', user, '-c', sql]
    
    proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    output, error = proc.communicate()
    decalogger.info(u'OUTPUT: {0}'.format(output))
    decalogger.info(u'ERROR: {0}'.format(error))
    
if(__name__ == '__main__'):
    from sys import argv
    if(len(argv)!=6):
        print 'Usage: python decahose_load_cron.py <PARSE_SQL_FILE>  <HOST>  <DATABASE>  <USER> <LOG_FILE>'
    else:
        execute(argv[1],argv[2],argv[3],argv[4],argv[5])