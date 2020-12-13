from typing import List, Tuple, Any

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
        Py2SQL.__check_connection()
        Py2SQL.__database_connection.disconnect()
        Py2SQL.__database_connection = None

    @staticmethod
    def db_engine():
        Py2SQL.__check_connection()

        name = Py2SQL.__select_single_query('SELECT DATABASE()')
        version = Py2SQL.__select_single_query('SELECT VERSION()')

        return 'Name: {0}, Version: {1}'.format(name, version)

    @staticmethod
    def db_name():
        Py2SQL.__check_connection()

        return Py2SQL.__select_single_query('SELECT DATABASE()')

    @staticmethod
    def db_size():
        Py2SQL.__check_connection()

        return Py2SQL.__select_single_query('SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 3) '
                                            'FROM information_schema.tables '
                                            'WHERE table_schema = \'{0}\' '
                                            'GROUP BY table_schema;'.format(Py2SQL.db_name()))

    @staticmethod
    def db_tables():
        Py2SQL.__check_connection()

        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute('SHOW TABLES')
        data = cursor.fetchall()
        return [x[0] for x in data]

    @staticmethod
    def db_table_structure(table):
        Py2SQL.__check_connection()

        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute('DESCRIBE {0};'.format(table))
        data = cursor.fetchall()
        return [(i, x[0], x[1]) for i, x in enumerate(data)]

    @staticmethod
    def db_table_size(table):
        Py2SQL.__check_connection()

        return Py2SQL.__select_single_query('SELECT ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 3)'
                                            'FROM information_schema.TABLES '
                                            'WHERE TABLE_SCHEMA = "{0}" '
                                            'AND TABLE_NAME = "{1}" '
                                            'ORDER BY (DATA_LENGTH + INDEX_LENGTH);'.format(Py2SQL.db_name(), table))

    @staticmethod
    def find_object(table, py_object):
        pass

    @staticmethod
    def find_objects_by(table: str, *attributes: Tuple[str, Any]) -> List[List[Tuple[str, str, str]]]:
        """
        Returns an ordered list of database table table entries
        that contain the attributes specified in the sequence attributes

        :param table: table name
        :param attributes: pairs (name, value)
        :return: list of table rows, table row is list of tuples: (attribute, type, value)
        """

        Py2SQL.__check_connection()

        table_structure = Py2SQL.db_table_structure(table)
        table_structure_names = [x[1] for x in table_structure]

        for attribute in attributes:
            if attribute[0] not in table_structure_names:
                raise ValueError('table hasn\'t {0} attribute'.format(attribute[0]))

        where_part = ''
        for attribute in attributes:
            where_part += 'T.{0} = "{1}" AND '.format(attribute[0], attribute[1])
        if len(where_part) != 0:
            where_part = where_part[: -4]

        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute('SELECT * '
                       'FROM {0} AS T '
                       'WHERE {1};'.format(table, where_part))
        all_data = cursor.fetchall()
        
        result = []
        for data in all_data:
            row = []
            for i, field in enumerate(table_structure):
                row.append((field[1], field[2], data[i]))
            result.append(row)

        return result

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
