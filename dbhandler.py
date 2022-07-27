import mysql.connector
from mysql.connector import Error
# regular expression
import re

starpat = re.compile('select +\*', re.IGNORECASE)
describepat = re.compile('describe', re.IGNORECASE)
showpat = re.compile('show', re.IGNORECASE)


class DBHandler:
    def __init__(self):
        self.connection = mysql.connector.connect(host='localhost',
                                                  database='arbor',
                                                  user='root',
                                                  password='password'
                                                  )

        self.__curosr = self.connection.cursor(dictionary=True)

    def __close(self):
        try:
            c = self.__curosr
            c.close()
        except Error as e:
            print("Error while connecting to MySQL", e)

    def close(self):
        self.__close()

    def executeQuery(self, query, args=None, size=None):
        c = self.__curosr
        try:
            c.execute(query)
        except Error as e:
            print("Error while connecting to MySQL", e)
        '''describe = describepat.search(query)
        show = showpat.search(query)
        print('describe\n', describe)
        print('show\n', show)'''
        return(c.fetchall())
