# coding: utf-8

# In[ ]:

from pyArango.connection import Connection


class DB:
    def __init__(self, selected_db=None, username=None, password=None):
        self.username = username
        self.password = password
        self.connection = None
        self.selected_db = selected_db

    @property
    def username(self):
        return self.__username

    @username.setter
    def username(self, username):
        self.__username = username

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, password):
        self.__password = password

    @property
    def selected_db(self):
        if self.__selected_db is None:
            raise Exception('DB not selected')
        return self.__selected_db

    @selected_db.setter
    def selected_db(self, selected_db):
        self.__selected_db = selected_db

    @property
    def connection(self):
        return self.__connection

    @connection.setter
    def connection(self, connection):
        self.__connection = Connection(arangoURL='http://35.198.104.86:8529', username=self.username, password=self.password)

    def connect():
        self.connection = self.connection

    def gcdb(self):
        return self.connection[self.selected_db]

    def findOrCreateCollection(self, collection):
        db = self.gcdb()
        try:
            col = db[collection]
            return col
        except Exception as e:
            db.createCollection(name=collection)
            return db[collection]



