'''
	Original code to connect to a GNIP Stream, by Dr. Skippy.
    Major overhauling by Srivatsan Ramanujam <sramanujam@gopivotal.com>
    July-2014
'''
import urllib2,socket
import base64
import zlib,httplib
import threading
from threading import Lock
import ujson as json
import sys
import ssl
import os
from datetime import datetime
import time

# Tune CHUNKSIZE as needed.  The CHUNKSIZE is the size of compressed data read
# For high volume streams, use large chunk sizes, for low volume streams, decrease
# CHUNKSIZE.  Minimum practical is about 1K.
CHUNKSIZE = 100*1024
GNIPKEEPALIVE = 30  # seconds
NEWLINE = '\r\n'

#Should be in the user's home directory.
USER_CREDENTIALS_FILE = os.path.join(os.path.expanduser('~'),'.gnip_credentials.secret')

#SampleTrack Hose (1% Stream)
SAMPLEHOSE_URL = 'https://stream.gnip.com:443/accounts/GreenplumInternal1/publishers/twitter/streams/sample1track/Prod.json'
#Decahose Powertrack (10% Stream)
DECAHOSE_URL = 'https://stream.gnip.com:443/accounts/PivotalLabs/publishers/twitter/streams/decahose/prod.json'
HOSE_URL = DECAHOSE_URL
POST_URL = ''
print_lock = Lock()
err_lock = Lock()

class procEntry(threading.Thread):
    def __init__(self, buf):
        self.buf = buf
        self.conn = httplib.HTTPConnection(POST_URL)
        self.header = {'Content-Type':'application/json; charset=UTF-8'}
        threading.Thread.__init__(self)

    def run(self):
        tweets = []
        for rec in [x.strip() for x in self.buf.split(NEWLINE) if x.strip() <> '']:
            try:
                rec_escaped = json.loads(rec)
                rec_escaped_converted = json.dumps(rec_escaped)
                tweets.append(rec_escaped_converted)
            except ValueError, e:
                with err_lock:
                     sys.stderr.write(u'Error processing JSON: {error} {obj}\n'.format(error=e,obj=rec))
            
        #Now write out all the tweets in a single http post 
        try:
            #Submit the resulting json to the HTTP POST URL
            self.conn.request('POST', u'', b'\n'.join(tweets), self.header)
            response = self.conn.getresponse().read()
        except urllib2.URLError, e:
            with err_lock:
                sys.stderr.write('URL Error on http post to %s: %s\n'%(str(POST_URL),str(e)))
        except Exception, e:
            with err_lock:
                sys.stderr.write(u'HTTP POST exception:{0}'.format(e))

def fetchUserCredentials():
    '''
	The user credentials file should contain just one line which is of the form base64.encodestring('%s:%s' % (UN, PW))
        where UN and PW are your user name and password.
	Make sure the file is only readable by you.
    '''
    creds = open(USER_CREDENTIALS_FILE,'r').read()
    return creds

def fetchHeaders(creds):
    '''
	Return the Headers
    '''
    HEADERS = { 'Accept': 'application/json',
            'Connection': 'Keep-Alive',
            'Accept-Encoding' : 'gzip',
            'Authorization' : 'Basic %s' % creds  }
    return HEADERS

def getStream():
    req = urllib2.Request(HOSE_URL, headers=fetchHeaders(fetchUserCredentials()))
    response = urllib2.urlopen(req, timeout=1+GNIPKEEPALIVE)
    decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
    remainder = ''
    while True:
        tmp = ''
        try:
            tmp = decompressor.decompress(response.read(CHUNKSIZE))
        except httplib.IncompleteRead, e:
            with err_lock:
               sys.stderr.write("Incomplete Read: %s"%(str(e))) 
               tmp=''              
        if tmp == '':
            return
        [records, remainder] = ''.join([remainder, tmp]).rsplit(NEWLINE,1)
        procEntry(records).start()

def testSetup():
    '''
       Check if the user credentials file is present at  $HOMEDIR/.gnip_credentials.secret 
    '''
    if not os.path.exists(USER_CREDENTIALS_FILE):
       print 'Please enter your GNIP credentials encoded in base64 in :{0}'.format(USER_CREDENTIALS_FILE)
       print 'It should be of the form base64.encodestring("%s:%s" % (UN, PW)) - where UN and PW are your user name and password.'
       return False
    return True
    
def main(post_url):
    '''
       Entry point
    '''
    max_attempts = 10
    attempt = 0
    delay = 1
    global POST_URL 
    POST_URL = post_url
    if(testSetup()):
        # Note: this automatically reconnects to the stream upon being disconnected
        while attempt < max_attempts:
            with print_lock:
                sys.stdout.write('\nAttempt:{0}'.format(attempt))
            try:
                getStream()
                attempt = 0
                delay = 1
                with err_lock:
                    attempt += 1
                    delay *= 2
                    sys.stderr.write("Forced disconnect. Retrying in {0} seconds".format(delay))
                    time.sleep(delay)
            except ssl.SSLError, e:
                with err_lock:
                    sys.stderr.write("Connection failed: %s\n"%(str(e)))
            except urllib2.HTTPError, e:
                with err_lock:
                    sys.stderr.write("HTTP Error: %s\n"%(str(e)))
            except urllib2.URLError, e:
                with err_lock:
                    sys.stderr.write("URL Error: %s\n"%(str(e)))
            except socket.error, e:
                with err_lock:
                    sys.stderr.write("Socket Error: %s\n"%(str(e)))
            except IOError, e:
	        with err_lock:
            	    sys.stderr.write("IOError: %s\n"%(str(e)))
            except SystemExit, e:
                with err_lock:
                    sys.stderr.write("It hit the fan now: %s\n"%(str(e)))
                sys.exit(e)

if __name__ == "__main__":
    from sys import argv
    if(len(argv) !=2):
        print 'Usage: python decahose.py <http post URL>'
        print 'Example: python decahose.py localhost:9009'
    else:
        main(argv[1])

