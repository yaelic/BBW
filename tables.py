from os import path
import csv
from oracle_db_helper import OracleDbHelper
from pprint import pprint

INDEX_DIC = {
    'paragraph' : 85532,
    'participants' : 6272,
    'sentences' : 275903,
    'speech' : 40583,
    'word' : 5324580
}

class Table(object):
    NAME = 'BASE'
    FIELDS = []

    def __init__(self):
        self._index = INDEX_DIC.get(self.__class__.NAME, 0)

    def append(self, line):
        # Sanity check for line
        if not line:
            raise ValueError("line param is required")

        self._internal_append(line)

    def _internal_append(self, line):
        raise NotImplementedError()

    def get_next_index(self):
        ret_index = self._index
        self._index += 1
        return ret_index

    def commit(self):
        pass

CSV_DIR = 'csv'


class CSVTable(Table):
    def __init__(self):
        super(CSVTable, self).__init__()
        name = '%s.csv' % self.__class__.NAME
        self.file = open(path.join(CSV_DIR, name), 'wb')
        self.writer = csv.DictWriter(self.file, self.__class__.FIELDS,extrasaction='ignore')

    def _internal_append(self, row):
        self.writer.writerow(row)


class OracleTable(Table):
    """
    A silly implementation of table that updates the table

    IMPORTANT: you must call OracleDbHelper.instance().commit() at the end of your script,
    in order for your data to be committed.
    """
    INSERT_SQL = None

    def __init__(self):
        super(OracleTable, self).__init__()

        self.cursor = OracleDbHelper.instance().cursor(self._get_query())
        self._buffer = []

    def _get_query(self):

        if self.__class__.INSERT_SQL:
            return self.__class__.INSERT_SQL

        # SQL Generation time!!
        # generate an insert statement with bind variables:
        # example: INSERT INTO moslhtable VALUES (:field1, :field2,:field3)
        tbl_name = self.__class__.NAME

        fields = map(lambda n: ':' + n, self.__class__.FIELDS)

        return "INSERT INTO %s VALUES(%s)" % (tbl_name, ', '.join(fields))

    def _internal_append(self, line):

        for f in self.__class__.FIELDS:
            if not f in line:
                line[f] = None
        # Push it to the db
        #print line
        self._buffer.append(line)

   ## def commit(self):
   ##     if self._buffer:
   ##         # print "commiting %d items to %s" % (len(self._buffer), self.__class__.NAME)
   ##         self.cursor.executemany(None, self._buffer)
   ##         self._buffer = []


    def commit(self):
        """


        :raise:
        """
        if self._buffer:
    	    try:
                # try to save the buffer to the DB
                self.cursor.executemany(None, self._buffer)
            except Exception,e:
                pprint(self._buffer)
                # raise the exception so the caller could handle it
                raise
            finally:
                # always clear the buffer, so 	the next xml file could be loaded without issues
                self._buffer = []




class DataTable(OracleTable):
    pass


##############################################################################
# TABLES
##############################################################################

class CallsTable(DataTable):
    NAME = 'calls'
    FIELDS = ['callid',
              'dateofcall',
              'quarter',
              'year',
              'starttime',
              'endtime',
              'companyid',
              'companyname',
              'outcome',
              'canstockex',
              'extranum',
              'extrachar',
              'extralongchar',
              'title']
    INSERT_SQL = """
INSERT INTO calls VALUES(:callid, to_date(:dateofcall,'yyyy-mm-dd'), :quarter, :year, :starttime, :endtime, :companyid, :companyname, :outcome, :canstockex, :extranum, :extrachar, :extralongchar, :title)
    """

class CompaniesTable(DataTable):
    NAME = 'companies'
    FIELDS = ['companyname', 'companyid', 'field', 'about']



class ParagraphTable(DataTable):
    NAME = 'paragraph'
    FIELDS = ['parid',
              'speechid',
              'callid',
              'speakerid',
              'data',
              'indexinspeech',
              'indexincall',
              'length',
              'complexity',
              'emotion',
              'positiveratio',
              'numofsentences',
              'vocaltagc',
              'voacltagn',
              'extranum',
              'extrachar']


class ParticpantsTable(DataTable):
    NAME = 'participants'
    FIELDS = [
        'id',
        'name',
        'affiliation',
        'type',
        'companyid',
        'iskeyspeaker',
        'extra',
    ]


class ParticipantsInCallsTable(DataTable):
    NAME = 'participantsincalls'
    FIELDS = [
        'participantid',
        'callid'
    ]


class SentencesTable(DataTable):
    NAME = 'sentences'
    FIELDS = [
        'sentenceid',
        'data',
        'parid',
        'speechid',
        'callid',
        'speakerid',
        'absindexinpar',
        'indexinpar',
        'indexinspeech',
        'lentgh',
        'numofwords',
        'nlptag',
        'emotion',
        'positiveration',
        'complexity',
        'extravar',
        'extranum'
    ]


class SpeechTable(DataTable):
    NAME = 'speech'
    FIELDS = [
        'speechid',
        'callid',
        'speechsegment',
        'speakerid',
        'indexincall',
        'speechpart',
        'length',
        'starttime',
        'endtime',
        'posorneg',
        'emotiontag',
        'extra',
        'type',
        'data'
    ]


class WordTable(DataTable):
    NAME = 'word'
    FIELDS = [
        'wordid',
        'sentenceid',
        'parid',
        'speakerid',
        'word',
        'indexinsentence',
        'absindexinsentence',
        'indexinpar',
        'absindexinpar',
        'indexinspeech',
        'absindexinspeech',
        'length',
        'complexity',
        'emotion',
        'partofspeech',
        'nlp',
        'positive',
        'extranum',
        'extrachar'
    ]


class DataStore():
    _instance = None

    def __init__(self):
        self.calls = CallsTable()
        self.companies = CompaniesTable()
        self.paragraph = ParagraphTable()
        self.participants = ParticpantsTable()
        self.participants_in_calls = ParticipantsInCallsTable()
        self.sentences = SentencesTable()
        self.speech = SpeechTable()
        self.word = WordTable()

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = DataStore()

        return cls._instance


    def commit(self):
        self.calls.commit()
        self.companies.commit()
        self.paragraph.commit()
        self.participants.commit()
        self.participants_in_calls.commit()
        self.sentences.commit()
        self.speech.commit()
        self.word.commit()