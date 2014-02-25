# import io script to interface with importio extractors

import os, sys, re, time, gzip
import urllib, urllib2
import logging, importio, threading, json


# We define a latch class as python doesn't have a counting latch built in
class _Latch(object):
    def __init__(self, count=1):
        self.count = count
        self.lock = threading.Condition()

    def countDown(self):
        with self.lock:
            self.count -= 1
            if self.count <= 0:
                self.lock.notifyAll()

    def await(self):
        with self.lock:
            starttime = int(time.time())
            while self.count > 0:
                self.lock.wait()




def callback(query, message):
    if message["type"] == "MESSAGE":
        if message["data"] != {}:
            f = open("tempfile.json", "a")
            f.write(json.dumps(message["data"],indent = 4))
            f.close()
    if query.finished(): latch.countDown()



#logging.basicConfig(level=logging.INFO)



class DataGatherer(object):

    commaPattern = re.compile(r",", re.MULTILINE | re.DOTALL)
    
    def __init__(self, userId="", apiKey=""):
        self.client = None
        self.userId = userId
        self.apiKey = apiKey
        self.connGuid = ""
        self.input = "webpage/url"
        self.queryUrl = "" # Keep in mind that self.connGuid and self.queryUrl will be dependent on each other
        self.outputData = None
        self.siteLinks = []
        self.callBack = callback
        self.siteLinks = []
        if userId and apiKey:
            self.client = self.buildClient(userId, apiKey)
        return(None)


    def buildClient(self, uId, apiK):
        self.userId = uId
        self.apiKey = apiK
        self.client = importio.ImportIO(host="https://query.import.io", userId=self.userId, apiKey=self.apiKey)
        try:
            self.client.connect()
        except:
            print "Could not establish connection - %s\n"%sys.exc_info()[0].__str__()
            return(None)
        return(self.client)


    def executeQuery(self, targetUrl, connectorGuid):
        self.connGuid = connectorGuid
        self.queryUrl = targetUrl
        try:
            self.client.query({"input" : {self.input : self.queryUrl}, "connectorGuids" : [self.connGuid]}, self.callBack)
        except:
            print "Could not execute query correctly - %s\n"%(sys.exc_info()[0])
            f = open("tempfile.json", "a")
            f.write(json.dumps({},indent = 4))
            f.close()


    def extractLinks(self, jsonString):
        try:
            self.outputData = json.loads(jsonString)
        except:
            print "The JSON string received could not be loaded correctly - %s\nTrying again by formatting it.\n"%(sys.exc_info()[0])
            couplePattern = re.compile(r"}{", re.MULTILINE | re.DOTALL)
            coupleMatch = re.search(couplePattern, jsonString)
            if coupleMatch:
                jsonStringElements = jsonString.split("}{")
                jsonString = jsonStringElements[jsonStringElements.__len__() - 1]
                jsonString = "{" + jsonString
            # Now, check the content of 'jsonString' for completeness. It should end with a '}' character.
            endBracePattern = re.compile(r"}$", re.MULTILINE | re.DOTALL)
            endBraceMatch = endBracePattern.search(jsonString)
            if not endBraceMatch:
                self.siteLinks = []
                return(self.siteLinks)
            self.outputData = json.loads(jsonString)
        dataList = []
        if self.outputData.has_key("results"):
            dataList = self.outputData["results"]
        elif self.outputData.has_key("errorType") and self.outputData["errorType"] == "UnauthorizedException" or self.outputData["errorType"] == "InputException":
            return ["",] # This case is for those cases where the outputData has an 'UnauthorizedException'.
        else:
            return([])
        self.siteLinks = [ tmpdata["biz_link"] for tmpdata in dataList ]
        return(self.siteLinks)


    def extractInfo(self, jsonString):
        try:
            outputData = json.loads(jsonString)
        except:
            print "The JSON string received could not be loaded correctly - %s\nTrying again by formatting it.\n"%(sys.exc_info()[0])
            couplePattern = re.compile(r"}{", re.MULTILINE | re.DOTALL)
            coupleMatch = re.search(couplePattern, jsonString)
            if coupleMatch:
                jsonStringElements = jsonString.split("}{")
                jsonString = jsonStringElements[jsonStringElements.__len__() - 1]
                jsonString = "{" + jsonString
            # Now, check the content of 'jsonString' for completeness. It should end with a '}' character.
            endBracePattern = re.compile(r"}$", re.MULTILINE | re.DOTALL)
            endBraceMatch = endBracePattern.search(jsonString)
            if not endBraceMatch:
                self.siteLinks = []
                return(self.siteLinks)
            outputData = json.loads(jsonString)
        infoDict = {}
        dataList = []
        if outputData.has_key("results"):
            dataList = outputData["results"]
        elif outputData.has_key("errorType") and outputData["errorType"] == "UnauthorizedException" or outputData["errorType"] == "InputException":
            return {} # This case is for those cases where the outputData has an 'UnauthorizedException'.
        else:
            return({})
        for data in dataList:
            for dk in data.keys():
                dkval = data[dk]
                dkval = re.sub(self.__class__.commaPattern, "__comma__", dkval)
                del data[dk] # Remove this element. We will add a clean and unicode free key and value in the next line.
                dk = dk.decode('unicode_escape').encode("ascii", "ignore")
                data[dk] = dkval.encode("ascii", "ignore")
            if not data.has_key("category"):
                data["category"] = ""
            elif not data.has_key("neighborhood"):
                data["neighborhood"] = ""
            elif not data.has_key("biz_name"):
                data["biz_name"] = ""
            elif not data.has_key("biz_link/_text"):
                data["biz_link/_text"] = ""
            elif not data.has_key("address"):
                data["address"] = ""
            else:
                pass
            try:
                infoDict[data["biz_link"]] = [data["category"], data["biz_name"], data["biz_link/_text"], data["address"]]
            except KeyError:
                try:
                    errormsg = (sys.exc_info()[0]).__str__()
                except:
                    print "Could not get the error message - %s\n"%(sys.exc_info()[0])
                    continue
                if re.search(re.compile(r"category"), errormsg):
                    infoDict[data["biz_link"]] = ["", data["biz_name"], data["biz_link/_text"], data["address"]]
                elif re.search(re.compile(r"biz_name"), errormsg):
                    infoDict[data["biz_link"]] = [data["category"], "", data["biz_link/_text"], data["address"]]
                elif re.search(re.compile(r"biz_link/_text"), errormsg):
                    infoDict[data["biz_link"]] = [data["category"], data["biz_name"], "", data["address"]]
                elif re.search(re.compile(r"address"), errormsg):
                    infoDict[data["biz_link"]] = [data["category"], data["biz_name"], data["biz_link/_text"], ""]
                else:
                    pass
        return infoDict
    


    def disconnect(self):
        self.client.disconnect()



if __name__ == "__main__":
    if sys.argv.__len__() < 2:
        print "Usage: python %s <file_containing_a_list_of_links_to_crawl>"%sys.argv[1]
        sys.exit()
    infile = sys.argv[1]
    outfile = "collectedData.json"
    if sys.argv.__len__() == 3:
        outfile = sys.argv[2]
    fin = open(infile)
    targetUrls = fin.readlines()
    fin.close()
    fout = open(outfile, "w")
    # Write the header row
    fout.write("\"Num\", \"WidgetName\", \"Source\", \"ResultNumber\", \"PageURL\", \"BusinessName\", \"Website\", \"WebsiteSource\", \"WebsiteTitle\", \"WebsiteText\", \"Address\"\n")
    numCtr = 1
    for url in targetUrls: # Stage 1 starts here
        infoDict = {} # Dictionary to hold data extracted from stage 1 and stage 2.
        url = url.rstrip("\n")
        if url == "":
            continue
        latch = _Latch(1)
        bot = None
        try:
            bot = DataGatherer("7dca778d-123c-40e8-91a2-0536b5a4531e", "I0Cw8PdgcsJvfy1NIeqVQ7idQEjdxxM8KDvyU48kc9p3HJ1G4ZsMtrCBUGgHuUR2VoAp6P/w8rl2uhvcsL0b/g==")
        except:
            print "Could not create bot object for URL '%s' - %s\n"%(url, sys.exc_info()[0])
        if not bot.client:
            continue
        # Remove earlier "tempfile.json" if it exists
        if os.path.isfile("tempfile.json"):
            os.remove("tempfile.json")
        while not os.path.isfile("tempfile.json") and not os.path.exists("tempfile.json"):
            try:
                bot.executeQuery(url, "3bcdeb1c-17cd-4d59-8114-6504548bff31")
            except:
                print "Encountered exception: %s\n"%(sys.exc_info()[0])
                continue
        latch.await()
        f = open("tempfile.json")
        data = f.read()
        f.close()
        try:
            os.remove("tempfile.json")
        except:
            print "Failed to remove tempfile.json - %s. Trying again after 3 seconds.\n"%(sys.exc_info()[0])
            time.sleep(3)
            os.remove("tempfile.json")
        siteLinks = bot.extractLinks(data)
        tmpDict = {}
        tmpDict =  bot.extractInfo(data)
        for urlkey in tmpDict.keys():
            infoDict[urlkey] = tmpDict[urlkey]
        # Crawl all subsequent pages
        start = 10
        while 1: # Loop to handle pagination
            latch = _Latch(1)
            pageUrl = url + "&start=" + start.__str__()
            print "Requesting '%s'\n"%pageUrl
            while not os.path.isfile("tempfile.json") and not os.path.exists("tempfile.json"):
                try:
                    bot.executeQuery(pageUrl, "3bcdeb1c-17cd-4d59-8114-6504548bff31")
                except:
                    print "Encountered exception while fetching '%s': %s\n"%(pageUrl, sys.exc_info()[0])
                    continue
            latch.await()
            f = open("tempfile.json")
            data = f.read()
            f.close()
            try:
                os.remove("tempfile.json")
            except:
                print "Failed to remove tempfile.json - %s. Trying again after 3 seconds.\n"%(sys.exc_info()[0])
                time.sleep(3)
                os.remove("tempfile.json")
            pageLinks = bot.extractLinks(data)
            if pageLinks.__len__() == 0:
                break
            elif pageLinks.__len__() == 1 and pageLinks[0] == "": # Case where we get 'InputException' or 'UnauthorizedException'.
                print "Repeating request for '%s'\n"%pageUrl
                continue # We need to repeat the request for this page
            tmpDict = {}
            tmpDict =  bot.extractInfo(data)
            for urlkey in tmpDict.keys():
                infoDict[urlkey] = tmpDict[urlkey]
            siteLinks.extend(pageLinks)
            start += 10 # Stage 1 ends here.
        linkCtr = 0
        while linkCtr < siteLinks.__len__(): # Stage 2 starts here.
            link = siteLinks[linkCtr]
            latch = _Latch(1)
            while not os.path.isfile("tempfile.json"):
                try:
                    bot.executeQuery(link, "e7391a0d-e1d8-499d-a400-8c88852b904a")
                except:
                    print "Encountered exception: %s\n"%sys.exc_info()[0]
                    continue
            latch.await()
            f = open("tempfile.json")
            data = f.read()
            f.close()
            try:
                jsonDict = json.loads(data)
            except:
                print "Could not load data - %s\n"%(sys.exc_info()[0])
                try:
                    os.remove("tempfile.json")
                except:
                    print "Failed to remove tempfile.json - %s. Trying again after 3 seconds.\n"%(sys.exc_info()[0])
                    time.sleep(3)
                    os.remove("tempfile.json")
                continue # Try fetching this again.
            linkCtr += 1
            try:
                os.remove("tempfile.json")
            except:
                print "Failed to remove tempfile.json - %s. Trying again after 3 seconds.\n"%(sys.exc_info()[0])
                time.sleep(3)
                os.remove("tempfile.json")
            if infoDict.has_key(link):
                try:
                    infoDict[link].append(jsonDict["pageUrl"])
                except:
                    infoDict[link].append("")
            print link # Stage 2 ends here.
        try:
            bot.disconnect()
        except:
            print "Error disconnecting bot: %s\n"%sys.exc_info()[0]
        # Dump the collected data in the output file in CSV format.
        resNumCtr = 1
        for urlkey in infoDict.keys():
            widgetName = "Yelp Profile Crawler"
            source = "Yelp Profile Crawler"
            resultNumber = resNumCtr
            businessName = infoDict[urlkey][1]
            businessName = businessName.decode('unicode_escape').encode("ascii", "ignore")
            mainSitePattern = re.compile(r"^https?:\/\/[^\/]+\/")
            websiteSource = re.sub(mainSitePattern, "/", urlkey)
            websiteSource = websiteSource.decode('unicode_escape').encode("ascii", "ignore")
            websiteText = infoDict[urlkey][2]
            address = infoDict[urlkey][3]
            fout.write("\"" + numCtr.__str__() + "\", \"" + widgetName + "\", \"" + source + "\", \"" + resultNumber.__str__() + "\", \"" + urlkey + "\", \"" + businessName + "\", \"" + urlkey + "\", \"" + websiteSource + "\", \"\", \"" + websiteText + "\", \"" + address + "\"\n")
            numCtr += 1
            resNumCtr += 1
    # We are done.
    fout.close()
    print "Done.\n"
    
    
