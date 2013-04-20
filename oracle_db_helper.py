import cx_Oracle

CONN_STRING = 'yaelc/yaeliyaeli@bw.c5lq1n5iprmf.us-west-2.rds.amazonaws.com/bwdb'


class OracleDbHelper(object):
    """
    This is a helper class to interact and work with Oracle
    """
    _instance = None

    def __init__(self):
        self.conn = cx_Oracle.connect(CONN_STRING, cclass="HOL", purity=cx_Oracle.ATTR_PURITY_SELF)

    def commit(self):
        self.conn.commit()

    def cursor(self, query=None):
        # Create a new cursor
        c = self.conn.cursor()
        # If we got a query, prepare it
        if query:
            c.prepare(query)
            print 'prepared %s' % query

        return c

    @classmethod
    def instance(cls):
        """
        A quick and dirty singleton
        """
        if not cls._instance:
            cls._instance = OracleDbHelper()

        return cls._instance