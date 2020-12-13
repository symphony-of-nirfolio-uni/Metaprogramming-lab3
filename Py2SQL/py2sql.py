"""
Has the implementation of Py2SQL class
"""

from typing import List, Tuple, Any

import mysql.connector
import os
import pyclbr
import shutil

from importlib import reload

from .database_info import DatabaseInfo
from ._init_locker import InitLocker


class Py2SQL(metaclass=InitLocker):
    """
    A set of specialized methods for work
    with the corresponding relational database and various variants of object-relational mapping
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
        Returns database name and version
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
        Returns database name

        :return: database name
        """

        Py2SQL.__check_connection()

        return Py2SQL.__select_single_query('SELECT DATABASE()')

    @staticmethod
    def db_size() -> float:
        """
        Returns database size in Mb

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
        Returns table name list

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
        Returns list of tuples like (id, name, type),
        where id - number of table attribute, name - attribute name, type - attribute type

        :param table: table name in current database
        :return: list of tuples like (id, name, type),
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
        Returns table size in Mb

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
        Py2SQL.__check_connection()

        cursor = Py2SQL.__database_connection.cursor()
        try:
            cursor.execute("SELECT * "
                           "FROM " + table +
                           " WHERE id = " + str(id) + ";")
        except:
            print("Field id doesn't exist in this table")
            return

        value = cursor.fetchone()
        if value is None:
            return None

        table_camel = Py2SQL.__to_camel_case(table)

        if table_camel not in globals():
            Py2SQL.create_class(table, Py2SQL.__to_snake_case(table))

        return globals()[table_camel](*value)

    @staticmethod
    def create_objects(table, fid, lid):
        pass

    @staticmethod
    def create_class(table, module):
        Py2SQL.__check_connection()

        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute("SHOW COLUMNS FROM " + table + ";")

        column_names = [column[0] for column in cursor]
        table_camel = Py2SQL.__to_camel_case(table)
        Py2SQL.__create_class(table_camel, column_names, module)

        reload(__import__(module))
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
    def __to_snake_case(s):
        new_s = ''
        for i in range(len(s)):
            if (s[i]).isupper() and i != 0 and s[i - 1] != '_' and \
                    ((s[i - 1]).islower() or i + 1 < len(s) and (s[i + 1]).islower()):
                new_s += '_'
            if s[i] != ' ' and s[i] != '.':
                new_s += (s[i]).lower()
        return new_s

    @staticmethod
    def __create_class(table, column_names, module):
        file = open(module + '.py', 'a+')

        all_classes = pyclbr.readmodule(module)
        if table in all_classes:
            return

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
        Py2SQL.__check_connection()

        def write_to_init(t_snake, t_camel):
            with open(os.path.join(package, '__init__.py'), 'a+') as init_file:
                init_file.write(f'from .{t_snake} import {t_camel}\n')

            try:
                reload(__import__(package + '.__init__'))
            except ModuleNotFoundError:
                shutil.rmtree(package)
                print('Error occurred 1 time in 100 due to reload import func, please retry again')
                raise

        name = Py2SQL.db_name()

        used_table = []
        table_names = [table]

        if os.path.exists(package):
            shutil.rmtree(package)
        os.mkdir(package)

        ok = True
        while ok:
            current_table = table_names[0]
            used_table.append(current_table)

            table_camel = Py2SQL.__to_camel_case(current_table)
            table_snake = Py2SQL.__to_snake_case(current_table)

            cursor = Py2SQL.__database_connection.cursor()
            cursor.execute("SHOW COLUMNS FROM " + current_table + ";")
            column_names = [column[0] for column in cursor]
            Py2SQL.__create_class(table_camel, column_names, table_snake)

            os.replace(table_snake + ".py", os.path.join(package, table_snake + ".py"))

            write_to_init(table_snake, table_camel)

            exec(f'from {package} import {table_camel}', globals())

            table_names.extend(Py2SQL.__get_reference_to(name, current_table))
            table_names.extend(Py2SQL.__get_reference_from(name, current_table))

            while len(table_names) > 0 and table_names[0] in used_table:
                if len(table_names) > 1:
                    table_names = table_names[1:]
                else:
                    table_names = []

            if len(table_names) == 0:
                ok = False

    @staticmethod
    def __get_reference_to(db_name, table):
        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute("SELECT referenced_table_name "
                       "FROM information_schema.KEY_COLUMN_USAGE "
                       "WHERE table_schema = '" + db_name +
                       "' and table_name = '" + table +
                       "' and NOT referenced_table_name IS NULL;")

        return [column[0] for column in cursor]

    @staticmethod
    def __get_reference_from(db_name, table):
        cursor = Py2SQL.__database_connection.cursor()
        cursor.execute("SELECT table_name "
                       "FROM information_schema.KEY_COLUMN_USAGE "
                       "WHERE table_schema = '" + db_name + "' and referenced_table_name = '" + table + "' ;")

        return [column[0] for column in cursor]

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
