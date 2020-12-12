import mysql.connector

from .database_info import DatabaseInfo
from .init_locker import InitLocker


class Py2SQL(metaclass=InitLocker):

    __database_connection = None

    @staticmethod
    def db_connect(db: DatabaseInfo):
        if isinstance(db, DatabaseInfo):
            Py2SQL.__database_connection = mysql.connector.connect(
                host=db.host,
                user=db.user,
                password=db.password,
                database=db.database
            )
        else:
            raise ValueError('db have to be DatabaseInfo class')

    @staticmethod
    def db_disconnect():
        pass

    @staticmethod
    def db_engine():
        pass

    @staticmethod
    def db_name():
        pass

    @staticmethod
    def db_size():
        pass

    @staticmethod
    def db_tables():
        pass

    @staticmethod
    def db_table_structure(table):
        pass

    @staticmethod
    def db_table_size(table):
        pass

    @staticmethod
    def find_object(table, py_object):
        pass

    @staticmethod
    def find_objects_by(table, *attributes):
        pass

    @staticmethod
    def find_class(py_class):
        pass

    @staticmethod
    def find_classes_by(*attributes):
        pass

    @staticmethod
    def create_object(table, id):
        pass

    @staticmethod
    def create_objects(table, fid, lid):
        pass

    @staticmethod
    def create_class(table, module):
        pass

    @staticmethod
    def create_hierarchy(table, package):
        pass
