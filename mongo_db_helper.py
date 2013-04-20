from pymongo import Connection
from data import Transcript

DB_NAME = 'yael_db'


class MongoDbHelper(object):
    """
    This is a helper class to interact and work with mongo
    """

    def __init__(self):
        self.conn = Connection('localhost')
        self.db = self.conn[DB_NAME]


    def save_transcript(self, transcript):
        data = transcript.to_dict()
        # remove the _id, because we're doing an upsert, and
        # mongo doesn't like updating the '_id' key in a set statement

        del data['_id']

        self.db.transcripts.update({'_id': transcript.id},
                                   {'$set': data}, safe=True, upsert=True)

    def all_transcripts(self, query=None):
        return [Transcript(t) for t in self.db.transcripts.find(query)]

    def transcript_by_id(self, id):
        t = self.db.transcripts.find_one({'_id': id})
        if not t:
            return None

        return Transcript(t)
