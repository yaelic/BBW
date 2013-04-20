__author__ = 'yaelcohen'
import nltk
import ystockquote
from datetime import timedelta
import cx_Oracle
from os import path
import csv

QUERY = 'select * from calls join companies on calls.companyid = companies.companyid'

class StockInfo(object):
    def __init__(self):
        self.conn = cx_Oracle.connect('yaelc/yaeliyaeli@bw.c5lq1n5iprmf.us-west-2.rds.amazonaws.com/bwdb')
        self.curs = self.conn.cursor()

    def getCallsToFill(self):
        self.curs.prepare(QUERY)
        self.curs.execute(QUERY)
        ##cursor = cx_Oracle.Cursor(connection)
        ##cursor.execute(query)
        for i in self.curs.fetchall():
            #print i
            call = {
                'callId' : i[0],
                'callDate' : i[1],
                'weekBCall' : i[1]-timedelta(days=7),
                'weekACall' : i[1]+timedelta(days=7),
                'nextDay' : i[1]+timedelta(days=1),
                'ticker' : i[19]
            }
           # print call
            res = self.calcOutcome(call)
            new_res = self.outputPercent(res)
            if new_res is not None:
                self.writeLine(new_res,res)
        self.conn.close()

    def calcOutcome(self, call):
        #print 'getting outcome'
        try:
            nextWeekRes = ystockquote.get_historical_prices(call['ticker'],call['callDate'].strftime("%Y%m%d"),call['weekACall'].strftime("%Y%m%d"))
        except Exception,e:
            nextWeekRes = None
            print 'Exception in next week on ', call['ticker'], 'call date: ', call['callDate']
        try:
            prevWeekRes = ystockquote.get_historical_prices(call['ticker'],call['weekBCall'].strftime("%Y%m%d"),call['weekACall'].strftime("%Y%m%d"))
        except Exception,e:
            prevWeekRes = None
            print 'Exception in prev week on ', call['ticker'], 'call date: ', call['callDate']
        try:
            SameDayRes = ystockquote.get_historical_prices(call['ticker'],call['callDate'].strftime("%Y%m%d"),call['callDate'].strftime("%Y%m%d"))
        except Exception,e:
            SameDayRes =None
            print 'Exception in sameDay on ', call['ticker'], 'call date: ', call['callDate']
        try:
            NextDayRes = ystockquote.get_historical_prices(call['ticker'],call['nextDay'].strftime("%Y%m%d"),call['nextDay'].strftime("%Y%m%d"))
        except Exception,e:
            NextDayRes=None
            print 'Exception in next day on ', call['ticker'], 'call date: ', call['callDate']
        res = {
            'call' : call,
            'navg' : self.calcWeekAVG(nextWeekRes),
            'pavg' : self.calcWeekAVG(prevWeekRes),
            }
        if (SameDayRes is not None):
            if ( SameDayRes.__len__()>=2):
                res['sameDayOpen'] = SameDayRes[1][1]
                res['sameDayClose'] = SameDayRes[1][4]
            else:
                res['sameDayOpen'] = None
                res['sameDayClose'] = None
        else:
            res['sameDayOpen'] = None
            res['sameDayClose'] = None
        if (NextDayRes is not None):
            if ( NextDayRes.__len__()>=2):
                res['nextDayOpen'] = NextDayRes[1][1]
            else:
                res['nextDayOpen'] = None
        else:
            res['nextDayOpen'] = None

        return res

    def calcWeekAVG(self,res):
        if res is None:
            return None
        index = 0
        count = 0
        sum = 0
        for i in res:
            if index==0 :
                index+=1
                continue
            else:
                sum = sum + float(i[4])
                count += 1
                index += 1
        avg = sum/count
        return avg

    ##conn.commit()
    ##print "Yael is fat", conn.version


    def outputPercent(self,res):
        new_res ={
            'sdoGrowth' : None,
            'sdcGrowth' : None,
            'ndoGrowth' : None
        }
        if res['pavg'] is None or res['navg'] is None:
            return None
        new_res['base'] = float(res['pavg'])
        new_res['100'] = 100
        if res['sameDayOpen'] is not None:
            new_res['sdoGrowth'] = float(res['sameDayOpen'])*100/new_res['base']
        if res['sameDayClose'] is not None:
            new_res['sdcGrowth'] = float(res['sameDayClose'])*100/new_res['base']
        if res['nextDayOpen'] is not None:
            new_res['ndoGrowth'] = float(res['nextDayOpen'])*100/new_res['base']
        new_res['nWeekGrowth'] = float(res['navg'])*100/new_res['base']
        return new_res

    def writeLine(self, new_res,res):
        #name = 'stockStats.csv'
        #file = open(path.join(name), 'wb')
        #writer = csv.DictWriter(file, ['100','sdoGrowth','sdcGrowth','ndoGrowth','nWeekGrowth'],extrasaction='ignore')
        #writer.writerow(new_res)
        #print new_res['100'], " ", new_res['sdoGrowth'], " ", new_res['sdcGrowth'], " ", new_res['ndoGrowth'], " ",  new_res['nWeekGrowth']
        outcome =0
        if new_res['sdcGrowth'] is not None and new_res['ndoGrowth'] is not None:
            if(new_res['ndoGrowth'] - new_res['sdcGrowth'] ) > 4.0:
                outcome=1
            if(new_res['sdcGrowth'] - new_res['ndoGrowth'] ) > 4.0:
                outcome=-1
        query =  "UPDATE CALLS SET prevWeekAvg=%s, sameDayOpen=%s, sameDayClose=%s, nextDayOpen=%s, nextWeekAvg=%s, outcome=%s where callId=%s"  \
                 % (res['pavg'], res['sameDayOpen'],res['sameDayClose'],res['nextDayOpen'],res['navg'], outcome,res['call']['callId'])
        query.replace('None','null')
        print query,';'
        #CONN_STRING = 'yaelc/yaeliyaeli@bw.c5lq1n5iprmf.us-west-2.rds.amazonaws.com/bwdb'
        #conn = cx_Oracle.connect(CONN_STRING, cclass="HOL", purity=cx_Oracle.ATTR_PURITY_SELF)
        #c = conn.cursor()
        #c.prepare(query)
        #c.execute()
        #conn.commit()


# y = ystockquote.get_historical_prices('T','20080201','20080401')


#writer.writerow(row)
s = StockInfo()
s.getCallsToFill()

