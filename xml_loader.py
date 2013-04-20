# Use the c version of ElementTree
from xml.etree import cElementTree as ET
from data import Transcript
import re
import cx_Oracle
import nltk


from tables import DataStore

XML_NS = "{http://www.factset.com/callstreet/xmllayout/v0.1}"

WORD_SPLIT_REGEX = re.compile(r',?\s')
SENTENCE_SPLIT_REGEX = re.compile(r'.*[\.\?!]')


class XMLLoader(object):
    def is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def __init__(self, path):
        self.path = path
        self._data_store = DataStore.instance()

    def load(self):
        try:
            print "Loading", self.path
            tree = ET.parse(self.path)
            self.root = tree.getroot()
            self.result = {}
            self._participants = {}
            self.result['_id'] = self.root.get('id')
            self._load_meta(self.result['_id'])
            self._load_sections(self.result['_id'])

            # Commit the data store
            # Important - We still need to call OracleDbHelper.instance().commit() in order to save the data to the DB
            self._data_store.commit()

        except Exception, e:
             print "\n\n", self.path, "didnt work","\n\n",e


    ##############################################################################
    # Meta loading
    ##############################################################################


    def _load_meta(self, callId):
        # store the id

        meta = self.root.find(XML_NS + 'meta')
        self.result['title'] = meta.find(XML_NS + 'title').text
        self.result['date'] = meta.find(XML_NS + 'date').text

        company_ids = []

        # collect all the company ids
        for child in meta.find(XML_NS + 'companies'):
            company_ids.append(child.text)

        self.result['company_ids'] = company_ids
        if company_ids.__len__()>1:
            print "###### more than one company! ###########"

        c={
            'callid' : callId,
            'dateofcall' : self.result['date'],
            'title' : self.result['title'],
            'companyid': company_ids[0],
            'quarter' : self.result['title'].split(' ')[0],
            'year' : self.result['title'].split(' ')[1]
        }

        if self.is_number(c['year']) == False :
            c['year'] = ''

        self._data_store.calls.append(c)
        self._load_participants(meta,company_ids[0])


    def _load_participants(self, meta, companyId):
        result = []

        for el in meta.find(XML_NS + 'participants'):
            internalId = el.get('id')
            p = {
                'name': el.text.encode('utf-8'),
                'id': self._data_store.participants.get_next_index(),
                'type': el.get('type'),
                'companyid':companyId
            }
            if el.get('affiliation') is not None:
                p['affiliation'] = el.get('affiliation').encode('utf-8')

            self._data_store.participants.append(p)
            # store in our cache, which we'll use later to fetch from when we'll go over the speeches
            self._participants[internalId] = p

            result.append(p)

        self.result['participants'] = result

    ##############################################################################
    # Section loading
    ##############################################################################

    def _load_sections(self, callID):

        ### Chosen tokenizer
        SentenceTokenizer = nltk.tokenize.PunktSentenceTokenizer()
        wordTokenizer = nltk.tokenize.WordPunctTokenizer()
        body = self.root.find(XML_NS + 'body')
        speechIndex = 0
        parIndexInCall = 0
        for section in body:
            name = section.get('name')
            for speaker in section:
                ## loading the speeches
                speech = self._load_speech(speaker, name, speechIndex,callID)
                parIndexInSpeech = 0
                senIndexInSpeech = 0
                wordIndexInSpeech = 0
                absWordIndexInSpeech = 0
                for par in speaker.find(XML_NS + 'plist'):
                    absoluteIndexinPar = 0
                    relativeIndexinPar = 0
                    wordIndexInPar = 0
                    absWordIndexInPar = 0
                    ## loading the paragraph
                    p =self._load_paragraph(par,speech, callID,parIndexInSpeech,parIndexInCall)
                    sentences =SentenceTokenizer.tokenize(p['data'])
                    for s in sentences:
                        indexInSen = 0
                        absIndexInSen = 0
                        ## loading sentences
                        sen = self._load_sentence(s, p, absoluteIndexinPar, relativeIndexinPar, senIndexInSpeech)
                        pos = nltk.word_tokenize(s)
                        words = nltk.pos_tag(pos)
                        for w in words:
                            #print w
                            word = {}
                            word['wordid'] = self._data_store.word.get_next_index()
                            word['sentenceid'] = sen['sentenceid']
                            word['parid'] = sen['parid']
                            word['speakerid'] = sen['speakerid']
                            word['word'] = w[0]
                            word['indexinsentence'] = indexInSen
                            word['absindexinsentence'] = absIndexInSen
                            word['indexinpar'] = wordIndexInPar
                            word['absindexinpar'] = absWordIndexInPar
                            word['indexinspeech'] = wordIndexInSpeech
                            word['absindexinspeech'] = absWordIndexInSpeech
                            word['length'] = w[0].__len__()
                            word['complexity'] = 0
                            word['emotion'] = 0
                            word['partofspeech'] = w[1]
                            word['nlp'] = 0
                            word['positive'] = 0
                            self._data_store.word.append(word)
                            absIndexInSen += w[0].__len__()+1
                            indexInSen += 1
                            absWordIndexInPar += w[0].__len__()+1
                            wordIndexInPar +=1
                            absWordIndexInSpeech += w[0].__len__()+1
                            wordIndexInSpeech += 1
                        absoluteIndexinPar += sen['lentgh']
                        relativeIndexinPar += 1
                        senIndexInSpeech += 1
                    parIndexInSpeech += 1
                    parIndexInCall += 1
                speechIndex += 1


        #self.result['sections'] = list([self._load_section(el) for el in body])

    def _load_section(self, el, D):
        return {
            'name': el.get('name'),
            'children': [self._load_speech(child) for child in el]
        }

    def _load_speech(self, el, name, index, callID):
        speechId = self._data_store.speech.get_next_index()
        speech = {}
        speech['speechsegment'] = name
        speech['speechid'] = speechId
        speech['callid'] = callID
        speech['indexincall'] = index
        speech['type'] = el.get('type')
        speakerId = self._participants.get(el.get('id'))
        if speakerId is not None:
            speech['speakerid'] = speakerId['id']
        else:
            speech['speakerid'] = None
        self._data_store.speech.append(speech)
            ##'children': [self._load_paragraph(child) for child in el.find(XML_NS + 'plist')]
        return speech


    def _load_paragraph(self, par,speech, callID,parIndexInSpeech,parIndexInCall):
        p={}
        if par.text is not None:
            p['data'] = par.text.encode('utf-8')
            p['length'] = par.text.encode('utf-8').__len__()
        else:
            p['data'] = ""
            p['length'] = 0
        p['parid'] = self._data_store.paragraph.get_next_index()
        p['speechid'] = speech['speechid']
        p['callid'] = callID
        p['speakerid'] = speech['speakerid']
        p['indexinspeech'] = parIndexInSpeech
        p['indexincall'] = parIndexInCall
        self._data_store.paragraph.append(p)
        return p

    def _load_sentence(self, text, par, absoluteIndexinPar, relativeIndexinPar, senIndexInSpeech):
        sen={}
        sen['sentenceid'] = self._data_store.sentences.get_next_index()
        sen['data'] = text
        sen['parid'] = par['parid']
        sen['speechid'] = par['speechid']
        sen['callid']= par['callid']
        sen['speakerid']= par['speakerid']
        sen['absindexinpar'] = absoluteIndexinPar
        sen['indexinpar']= relativeIndexinPar
        sen['lentgh'] = text.__len__()
        sen['indexinspeech'] = senIndexInSpeech
        self._data_store.sentences.append(sen)
        return sen

    def _load_word(self, w):
        return {
            'content': w
        }



