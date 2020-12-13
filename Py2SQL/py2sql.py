"""
Has realization of Py2SQL class
"""

from typing import List, Tuple

import mysql.connector

from .database_info import DatabaseInfo
from .init_locker import InitLocker


class Py2SQL(metaclass=InitLocker):
    """
    A set of specialized methods for work
    with the corresponding relational database and various variants of object-relational display
    """

    __database_connection = None

    @staticmethod
    def db_connect(db: DatabaseInfo) -> None:
        """
        Establishes a connection to database

        :param db: parameters to connect to database
        """

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
    def db_disconnect() -> None:
        """
        Terminates the connection to database
        """

        Py2SQL.__check_connection()
        Py2SQL.__database_connection.disconnect()
        Py2SQL.__database_connection = None

    @staticmethod
    def db_engine() -> str:
        """
        Return database name and version
        Format: "Name: Database_Name, Version: Database_Version"

        :return: database name and version
        """

        Py2SQL.__check_connection()

        name = Py2SQL.__select_single_query('SELECT DATABASE()')
        version = Py2SQL.__select_single_query('SELECT VERSION()')

        return 'Name: {0}, Version: {1}'.format(name, version)

    @staticmethod
    def db_name() -> str:
        """
        Return database name

        :return: database name
        """

        Py2SQL.__check_connection()

        return Py2SQL.__select_single_query('SELECT DATABASE()')

    @staticmethod
    def db_size() -> float:
        """
        Return database size in Mb

        :return: database size in Mb
        """

        Py2SQL.__check_connection()

        size = Py2SQL.__select_single_query('SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 3) '
                                            'FROM information_schema.tables '
                                            'WHERE table_schema = \'{0}\' '
                                            'GROUP BY table_schema;'.format(Py2SQL.db_name()))
        return float(size)

    @staticmethod
    def db_tables() -> List[str]:
        """
        Return table name list

        :return: list of table names
        """

        Py2SQL.__check_connection()

        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute('SHOW TABLES')
        data = cursor.fetchall()
        return [x[0] for x in data]

    @staticmethod
    def db_table_structure(table: str) -> List[Tuple[int, str, str]]:
        """
        Return list of tuple like (id, name, type),
        where id - number of table attribute, name - attribute name, type - attribute type

        :param table: table name in current database
        :return: list of tuple like (id, name, type),
        where id - number of table attribute, name - attribute name, type - attribute type
        """

        Py2SQL.__check_connection()

        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute('DESCRIBE {0};'.format(table))
        data = cursor.fetchall()
        return [(i, x[0], x[1]) for i, x in enumerate(data)]

    @staticmethod
    def db_table_size(table: str) -> float:
        """
        Return table size in Mb

        :param table: table name in current database
        :return: table size in Mb
        """

        Py2SQL.__check_connection()

        size = Py2SQL.__select_single_query('SELECT ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 3)'
                                            'FROM information_schema.TABLES '
                                            'WHERE TABLE_SCHEMA = "{0}" '
                                            'AND TABLE_NAME = "{1}" '
                                            'ORDER BY (DATA_LENGTH + INDEX_LENGTH);'.format(Py2SQL.db_name(), table))
        return float(size)

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

    @staticmethod
    def __check_connection():
        if Py2SQL.__database_connection is None:
            raise ValueError('Database isn\'t connected')

    @staticmethod
    def __select_single_query(query):
        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute(query)
        data = cursor.fetchone()
        return data[0]
