'''

import.io client library

Dependencies: Python 2.7

@author: import.io
'''

import threading, logging, uuid, json, urllib, urllib2, cookielib, gzip, Queue
from cookielib import CookieJar, DefaultCookiePolicy
from _pyio import BytesIO
import sys

logging.basicConfig()
logger = logging.getLogger(__name__)

class Query:
    '''
    Represents a single query into the import.io platform
    '''

    def __init__(self, callback, query):
        self.query = query
        self.jobsSpawned = 0
        self.jobsStarted = 0
        self.jobsCompleted = 0
        self._finished = False
        self._callback = callback
    
    def _onMessage(self, data):
        
        msgType = data["type"]
        if msgType == u"SPAWN":
            self.jobsSpawned+=1
        elif msgType == u"INIT" or msgType == u"START":
            self.jobsStarted+=1
        elif msgType == u"STOP":
            self.jobsCompleted+=1
            
        self._finished = self.jobsStarted is self.jobsCompleted and self.jobsSpawned + 1 is self.jobsStarted and self.jobsStarted > 0;
        
        # if there is an error or the user is not authorised correctly then allow isFinished to return true by setting jobs to -1
        if msgType == u"ERROR" or msgType == u"UNAUTH" or msgType == u"CANCEL":
            self._finished = True;
        
        self._callback(self, data)
        
    def finished(self):
        '''
        Returns boolean - true if the query has terminated
        '''
        return self._finished

class ImportIO:
    '''
    The main IO client class.
    '''
    
    def __init__(self, host="https://query.import.io", proxies={}, userId=None, apiKey=None):
        self.host = host
        self.proxies = proxies
        self.cookies = {}
        self.msgId = 1
        self.clientId = None
        self.url = "%s/query/comet/" % host
        self.messagingChannel = u"/messaging"
        self.queries = {}
        self.userId = userId
        self.apiKey = apiKey
        self.cj = cookielib.CookieJar()
        self.opener = urllib2.build_opener(urllib2.ProxyHandler(self.proxies), urllib2.HTTPCookieProcessor(self.cj))
        self.queue = Queue.Queue()
        self.isConnected = False
    
    def login(self, username, password, host="https://api.import.io"):
        r = self.opener.open("%s/auth/login" % host, urllib.urlencode( {'username': username, 'password': password} ) ) 

        if r.code is not 200:
            raise Exception("Could not log in, code %s" % r.code)
    
    def request(self, channel, path="", data={}, throw=True):
        
        # add in the common values
        data["channel"] = channel
        data["connectionType"] = "long-polling"
        data["id"] = self.msgId
        self.msgId += 1
        
        if self.clientId is not None:
            data["clientId"] = self.clientId
            
        url = "%s%s" % (self.url, path)
        
        if self.apiKey is not None:
            url = "%s?&%s" % (url, urllib.urlencode({ "_user" : self.userId, "_apikey" : self.apiKey }) )
        
        request = urllib2.Request(url)
        request.add_data(json.dumps([data]))
        request.add_header("Content-Type", "application/json;charset=UTF-8")
        request.add_header('Accept-encoding', 'gzip')
        response = self.opener.open(request)
        if response.code != 200 :
            raise Exception("Connect failed, status %s" % response.code)
        
        if response.info().get('Content-Encoding') == 'gzip':
            # need to buffer in memory - facepalm: http://bugs.python.org/issue914340
            response.json = json.load(gzip.GzipFile(fileobj=BytesIO(response.read())))
        else:
            response.json = json.load(response)
        
        for msg in response.json:
            if "successful" in msg and msg["successful"] is not True :
                msg = "Unsuccessful request: %s", msg
                if throw:
                    raise Exception(msg)
                else:
                    logger.warn(msg)
            try:
                if msg["channel"] != self.messagingChannel : continue
            except:
                print "Encountered error in importio: %s\n"%(sys.exc_info()[0])
                break
            self.queue.put(msg["data"])
        
        return response
    
    def handshake(self):
        handshake = self.request("/meta/handshake", path="handshake", data={"version":"1.0","minimumVersion":"0.9","supportedConnectionTypes":["long-polling"],"advice":{"timeout":60000,"interval":0}})
        self.clientId = handshake.json[0]["clientId"]
    
    def connect(self):
        if self.isConnected:
            return;

        self.handshake()
        
        self.request("/meta/subscribe", data={"subscription":self.messagingChannel})

        self.isConnected = True
        t = threading.Thread(target=self.poll, args=())
        t.daemon = True
        t.start()
        
        t2 = threading.Thread(target=self.pollQueue, args=())
        t2.daemon = True
        t2.start()
        

    def disconnect(self):
        self.request("/meta/disconnect", throw=True)
        self.isConnected = False

    def pollQueue(self):
        while self.isConnected:
            try:
                self.processMessage(self.queue.get())
            except:
                logger.error("Error", exc_info=True)

    def poll(self):
        while self.isConnected:
            self.request("/meta/connect", path="connect", throw=False)
        
    def processMessage(self, data):
        try:
            reqId = data["requestId"]
            query = self.queries[reqId]
            query._onMessage(data)
            if query.finished(): del self.queries[reqId]
        except:
            logger.error("Error", exc_info=True)
        
    def query(self, query, callback):
        query["requestId"] = str(uuid.uuid4())
        self.queries[query["requestId"]] = Query(callback, query)
        self.request("/service/query", data={ "data":query })
