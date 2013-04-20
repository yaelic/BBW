class Model(object):
    def to_dict(self):
        raise NotImplementedError


class Collection(list):
    """
    We create this wrapper around list for better handling of Items
    """

    def __init__(self, parent, item_class, data=None):
        """

        """

        # Handle the case of an empty data
        if not data:
            data = []

        # Store the parent, for easy access by the items
        self.parent = parent
        self.item_class = item_class

        # for each item in the data, create an instance of the item class, using the data and
        # a reference to the collection
        idx = 0
        for item in data:
            self.append(item_class(item, self, idx))
            idx += 1


    def all_children(self, of_type=None):
        """
        This is a spacial helper iterator, used to iterate all the children of the collection and items
        """

        for i in self:
            if not of_type:
                yield i
            elif isinstance(i, of_type):
                yield i

            # if i is a collection item iterate over the children
            if isinstance(i, CollectionItem):
                for c in i.children.all_children(of_type):
                    yield c


    def to_dict(self):
        return [i.to_dict() for i in self]


class Item(Model):
    CHILD_TYPE = None

    def __init__(self, data, collection, idx):
        self.idx = idx
        self.collection = collection

        self.tags = data.get('tags', [])

    def parent(self, of_type=None):
        """
        Returns the parent object of an item. an optional type can be passed, and the method
        will try to return the parent with the type given

        Example:
            word.parent(Speach) -> return the speach this word belongs to


        """
        p = self.collection.parent

        if not of_type:
            return p

        while p:
            if isinstance(p, of_type):
                return p

            if hasattr(p, 'parent') and callable(p.parent):
                p = p.parent
            else:
                return None

        return None


    def next(self):
        return self.collection[self.idx + 1]

    def prev(self):
        # if we're the first item in the list
        if self.idx == 0:
            return None

        return self.collection[self.idx - 1]


class CollectionItem(Item):
    CHILD_TYPE = None

    def __init__(self, data, collection, idx):
        super(CollectionItem, self).__init__(data, collection, idx)

        # create a collection for the children, by the specified type
        self.children = Collection(self, self.__class__.CHILD_TYPE, data.get('children', []))

    def all_children(self, of_type=None):
        return self.children.all_children(of_type)


class Transcript(Model):
    def __init__(self, data):

        self.id = data.get('_id')
        self.title = data.get('title')
        self.date = data.get('date')
        self.tags = data.get('tags', [])
        self.company_ids = data.get('company_ids')

        self.participants = [Participant(p) for p in data.get('participants', [])]

        self.sections = Collection(self, Section, data.get('sections'))

    def to_dict(self):
        return {
            '_id': self.id,
            'title': self.title,
            'data': self.date,
            'tags': self.tags,
            'company_ids': self.company_ids,
            'participants': [p.to_dict() for p in self.participants],
            'sections': self.sections.to_dict()
        }


class Participant(Model):
    def __init__(self, data):
        self.name = data.get('name', '')
        self.id = data.get('id')
        self.type = data.get('type')

    def to_dict(self):
        return {
            'name': self.name,
            'id': self.id,
            'type': self.type
        }


class Word(Item):
    def __init__(self, data, collection, idx):
        """
            Initialize a new word, using a dict 'd'

            Params:
                - data     a dict with the content for the word

        """
        super(Word, self).__init__(data, collection, idx)
        self.content = data.get('content', '')
        self.tags = data.get('tags', [])

    def to_dict(self):
        """
        Convert the word to a dict
        """
        return {
            'content': self.content,
            'tags': self.tags
        }


class Sentence(CollectionItem):
    CHILD_TYPE = Word

    def __init__(self, data, collection, idx):
        """
        Initialize a sentence, using a dict 'd'

        Params:
            - d     a dict with the content for the word

        """
        super(Sentence, self).__init__(data, collection, idx)
        self.content = data.get('content', '')

    def to_dict(self):
        return {
            'content': self.content,
            'tags': self.tags,
            'children': self.children.to_dict()
        }


class Paragraph(CollectionItem):
    CHILD_TYPE = Sentence

    def __init__(self, data, collection, idx):
        super(Paragraph, self).__init__(data, collection, idx)

        self.content = data.get('content', '')


    def to_dict(self):
        return {
            'content': self.content,
            'tags': self.tags,
            'children': self.children.to_dict()
        }


class Speech(CollectionItem):
    CHILD_TYPE = Paragraph

    def __init__(self, data, collection, idx):
        super(Speech, self).__init__(data, collection, idx)

        self.speaker = data.get('speaker', None)
        self.type = data.get('type', None)

    def to_dict(self):
        return {
            'tags': self.tags,
            'children': self.children.to_dict(),
            'speaker': self.speaker,
            'type': self.type
        }


class Section(CollectionItem):
    CHILD_TYPE = Speech

    def __init__(self, data, collection, idx):
        super(Section, self).__init__(data, collection, idx)

        self.name = data.get('name')

    def to_dict(self):
        return {
            'name': self.name,
            'children': self.children.to_dict(),
            'tags': self.tags
        }




