"""
Has the implementation of DatabaseInfo class
"""


class DatabaseInfo:
    """
    Contains info for connection to database
    """

    def __init__(self, host: str = str(), user: str = str(), password: str = str(), database: str = str()) -> None:
        """
        Init function

        :param host: host of MySQL database
        :param user: user of MySQL database
        :param password: password for user
        :param database: database name to connect
        """

        self.host = host
        self.user = user
        self.password = password
        self.database = database
