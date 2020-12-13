import mysql.connector
import os
import pyclbr

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
        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute("SHOW COLUMNS FROM " + table + ";")

        column_names = [column[0] for column in cursor]
        table_camel = Py2SQL.__to_camel_case(table)
        Py2SQL.__create_class(table_camel, column_names, module)

        exec(f'from {module} import {table_camel}', globals())

    @staticmethod
    def __to_camel_case(s):
        new_s = ''
        is_first = True
        for i in range(len(s)):
            if s[i] != '_' and s[i] != '-' and s[i] != ' ':
                if is_first:
                    new_s += (s[i]).upper()
                    is_first = False
                elif i > 0 and not (s[i-1] != '_' and s[i-1] != '-' and s[i-1] != ' '):
                    new_s += s[i].upper()
                else:
                    new_s += s[i]
        return new_s

    @staticmethod
    def __create_class(table, column_names, module):
        all_classes = pyclbr.readmodule(module)
        if table in all_classes:
            return

        file = open(module + '.py', 'a+')

        if os.stat(module + '.py').st_size > 0:
            file.write('\n\n')

        file.write(f'class {table}:\n')
        file.write('\tdef __init__(self')
        for col in column_names:
            file.write(', ' + col)
        file.write('):\n')
        if len(column_names) == 0:
            file.write('\t\tpass\n')
        else:
            for col in column_names:
                file.write('\t\tself.' + col + ' = ' + col + '\n')
        file.close()

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
