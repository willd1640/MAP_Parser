
import pprint,sys
import numpy as np
from BTrees.OOBTree import OOBTree
from tables import *


class MDUpdateAction(object):
    NEW = '0'
    CHANGE = '1'
    DELETE = '2'
    OVERLAY = '5'

class MDEntryType(object):
    BID = '0'
    OFFER = '1'
    TRADE = '2'


class Message(object):
    APPLVERID               = '1128'
    BODYLENGTH              = '9'
    LASTMSGSEQNUMPROCESSED  = '369'
    MDSECURITYTRADINGSTATUS = '1682'
    MSGSEQNUM               = '34'
    MSGTYPE                 = '35'
    POSSDUPFLAG             = '43'
    SENDERCOMPID            = '49'
    SENDINGTIME             = '52'
    AGGRESSORSIDE       = '5797'
    FIXINGBRACKET       = '5790'
    MATCHEVENTINDICATOR = '5799'
    MDENTRYPX           = '270'
    MDENTRYSIZE         = '271'
    MDENTRYTIME         = '273'
    MDENTRYTYPE         = '269'
    MDPRICELEVEL        = '1023'
    MDQUOTETYPE         = '1070'
    MDUPDATEACTION      = '279'
    NETCHGPREVDAY       = '451'
    NOMDENTRIES         = '268'
    NUMBEROFORDERS      = '346'
    OPENCLOSESETTLEFLAG = '286'
    QUOTECONDITION      = '276'
    RPTSEQ              = '83'
    SECURITYDESC        = '107'
    SECURITYID          = '48'
    SECURITYIDSOURCE    = '22'
    SETTLDATE           = '64'
    TICKDIRECTION       = '274'
    TRADECONDITION      = '277'
    TRADEDATE           = '75'
    TRADEVOLUME         = '1020'
    TRADINGSESSIONID    = '336'
    DELIMITER           = '\x01'

    def __init__ (self,msg):
        self.pp = pprint.PrettyPrinter(depth=4)
        self.header = {}
        self.repeatinggroups = []
        self.message = msg


    def isIncremental(self):
        s = self.message.split(Message.DELIMITER)
        s.pop(-1)
        p = dict( z.split('=') for z in s )
        return p.get('35') == 'X'

    def parse(self):
        header,repeatgroups = self.message.split('268=')
        self.__parseHeader(header)
        self.__parseRepeatingGroups(repeatgroups)
        #print header

    def __parseHeader (self,header):
        w=header.split(Message.DELIMITER)
        w.pop(-1)
        for elem in w:
            k,v=  elem.split('=')
            self.header[k] = v
        #self.pp.pprint(self.header)

    def __parseRepeatingGroups (self,rg):
        groups = rg.split('279=')
        groups.pop(0)
        #print len(groups)
        for g in groups:
            g= '279=' + g
            w=g.split(Message.DELIMITER)
            w.pop(-1)
            mydict={}
            for elem in w:
                k,v=  elem.split('=')
                mydict[k] = v
            self.repeatinggroups.append(mydict)
        #self.pp.pprint(self.repeatinggroups)

    def getSENDINGTIME(self):
        return self.header[Message.SENDINGTIME]

    def getMDENTRIES (self):
        return self.repeatinggroups

    def getNOMDENTRIES (self):
        return len(self.repeatinggroups)

    def getSorted (self):
        return sorted(self.repeatinggroups,key=lambda x: x[Message.MDENTRYPX])

    def getNew (self):
        """
        return indexes of new orders
        """
        idx=[]
        for i,ha in enumerate(self.repeatinggroups):
            #print ha
            if ha[Message.MDUPDATEACTION] == MDUpdateAction.NEW:
                idx.append(i)
        return idx

    def getChange (self):
        """
        return indexes of new orders
        """
        idx=[]
        for i,ha in enumerate(self.repeatinggroups):
            #print ha
            if ha[Message.MDUPDATEACTION] == MDUpdateAction.CHANGE:
                idx.append(i)
        return idx

    def getDelete (self):
        """
        return indexes of deleted orders
        """
        idx=[]
        for i,ha in enumerate(self.repeatinggroups):
            #print ha
            if ha[Message.MDUPDATEACTION] == MDUpdateAction.DELETE:
                idx.append(i)
        return idx




class Level(object):
    def __init__(self,kwargs):
        self.level = int(kwargs[Message.MDPRICELEVEL])
        self.price = kwargs[Message.MDENTRYPX]
        self.size  = kwargs[Message.MDENTRYSIZE]
        self.side  = kwargs[Message.MDENTRYTYPE]

    def __cmp__(self, other):
        if other.__class__ is Level:
            if self.side == MDEntryType.BID:
                return cmp(self.level, other.level)
            else:
                return cmp(other.level,self.level)
        else:
            return cmp(self.level, other.level)




class HDFreader(object):
    def __init__ (self,filename, depth):
        self.filename = filename
        self.depth = depth
        self.h5file = openFile(self.filename)

    def reader(self):
        self.rbook = self.h5file.root.orderbook.book

    def shutdown (self):
        try:
            self.h5file.close()
        except:
            pass

    def __del__ (self):
        self.shutdown()



class HDFutils(object):
    def __init__ (self,filename, depth):
        self.filename = filename
        self.depth = depth

    def create (self):
        self.h5file = openFile(self.filename, mode = "w", title = "Test file")
        class OBook(IsDescription):
            ticker    = StringCol(16)                     # 16-character String
            timestamp = Int64Col()                        # Signed 64-bit integer
            bids      = Float64Col(shape=(self.depth,))   # integer
            bidsize   = Int32Col(shape=(self.depth,))     # integer
            asks      = Float64Col(shape=(self.depth,))   # integer
            asksize   = Int32Col(shape=(self.depth,))     # integer
        group = self.h5file.createGroup("/", 'orderbook', 'OrderBookgroup')
        self.table = self.h5file.createTable(group, 'book', OBook, "OrderBook")
        self.book = self.table.row

    def add (self,bidbook,askbook,ticker,timestamp):
        bids = np.zeros(self.depth)
        asks = np.zeros(self.depth)
        bidsize = np.zeros(self.depth)
        asksize = np.zeros(self.depth)

        buys  = [ [bidbook[b].size, bidbook[b].price] for b in sorted(bidbook.keys()) ]
        sells = [ [askbook[a].price, askbook[a].size] for a in sorted(askbook.keys()) ]
        buys  = np.array(buys)
        sells = np.array(sells)

        bids[0:len(buys)] = buys[:,0][:self.depth]
        asks[0:len(sells)] = sells[:,0][:self.depth]
        bidsize[0:len(buys)] = buys[:,1][:self.depth]
        asksize[0:len(sells)] = sells[:,1][:self.depth]

        self.book['ticker'] = ticker
        self.book['timestamp'] = timestamp
        self.book['bids'] = bids #.tolist()
        self.book['asks'] = asks #.tolist()
        self.book['bidsize'] = bidsize #.tolist()
        self.book['asksize'] = asksize #.tolist()
        self.book.append()

    def shutdown (self):
        try:
            self.table.flush()
        except:
            pass
        self.h5file.close()

    def __del__ (self):
        self.shutdown()





class Parser(object):
    def __init__ (self,file,**kwargs):
        self.file = file
        self.fh = open(self.file, "r")
        self.symbol = kwargs['symbol']
        self.symbol = kwargs.get('symbol','ALL')
        self.depth = kwargs['depth']
        self.bidbook = OOBTree()
        self.askbook = OOBTree()
        self.pp = pprint.PrettyPrinter(depth=10)
        self.hdfutils = HDFutils('cme_depth_zbu1_10v9.h5',10)
        self.hdfutils.create()

    def read(self):
        c=0
        for line in self.fh:
            msg = Message(line)
            if not msg.isIncremental(): continue
            msg.parse()
            mdentries= msg.getMDENTRIES()
            for i in msg.getChange():
                if mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.BID:
                    self.bidbook[ mdentries[i][Message.MDPRICELEVEL] ] = Level(mdentries[i])
                    #self.pp.pprint( mdentries[i] )
                elif mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.OFFER:
                    self.askbook[ mdentries[i][Message.MDPRICELEVEL] ]= Level(mdentries[i])
            for i in msg.getDelete():
                if mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.BID:
                    try:
                        del self.bidbook[ mdentries[i][Message.MDPRICELEVEL] ]
                    except:
                        pass
                elif mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.OFFER:
                    try:
                        del self.askbook[ mdentries[i][Message.MDPRICELEVEL] ]
                    except:
                        pass
            for i in msg.getNew():
                if mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.BID:
                    self.bidbook[ mdentries[i][Message.MDPRICELEVEL] ] = Level(mdentries[i])
                elif mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.OFFER:
                    self.askbook[ mdentries[i][Message.MDPRICELEVEL] ]= Level(mdentries[i])
            c+=1
            self.display()
            try:
                self.hdfutils.add(self.bidbook,self.askbook,self.symbol,msg.getSENDINGTIME() )
            except:
                pass
            if c>743:
                break

    def display(self):
        try:
            del self.bidbook[ '10' ]
        except:
            pass
        try:
            del self.askbook[ '10' ]
        except:
            pass
        print '================================================================\\'

        print "@@"
        for b in sorted(self.bidbook.keys()):
            print self.bidbook[b].size, self.bidbook[b].price

        for a in sorted(self.askbook.keys(), reverse=True):
            print self.askbook[a].price , self.askbook[a].size

        print '================================================================/'
        print



    def __del__ (self):
        self.fh.close()




if __name__ =='__main__':
    p = Parser("../data/XCBT_MD_ZB_20110725_20110729",symbol='ZBU1',depth='10')
    p.read()
    p.display()
