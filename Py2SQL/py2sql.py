from .init_locker import InitLocker


class Py2SQL(metaclass=InitLocker):

    @staticmethod
    def db_connect(db):
        pass

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
