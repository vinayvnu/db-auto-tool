from dbhandler import DBHandler
import datetime


class TableData(DBHandler):
    def __init__(self):
        self.isInsertDone = False
        self.isSelectDone = False
        self.isDeleteDone = False
        self.TABLENAME = ''
        self.REFCOLNAMELIB = {}
        self.COLNAMELIB = {}
        self.EXTRACTALLCOLLIB = {}

    def loadAllData(self, df):
        for i in df.columns.values.tolist():

            if i == "TABLENAME":
                self.TABLENAME = df._get_value(1, i)

            if i.split('_')[0] == "REF":
                if df.loc[0, [i]].isnull().bool():
                    pass
                else:
                    self.REFCOLNAMELIB[df._get_value(
                        0, i)] = df._get_value(1, i)

            if i[:3] == "COL":
                if df.loc[0, [i]].isnull().bool():
                    pass
                else:
                    self.COLNAMELIB[df._get_value(0, i)] = df._get_value(1, i)

    def printAll(self):
        print(self.TABLENAME)
        print(self.REFCOLNAME)
        print(self.COLNAME.keys())
        print(", ".join(self.COLNAME.keys()))
        print(", ".join(self.COLNAME.values()))

    def createInsertQuery(self):
        # specify the colname are of which data type
        try:
            DBHandler.__init__(self)
            querydata = self.executeQuery(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE   TABLE_SCHEMA='arbor' AND TABLE_NAME='component_definition_ref';"
            )
            for i in querydata:
                self.EXTRACTALLCOLLIB[i.values()] = ''

            sql = self.createSqlQuery(self.TABLENAME, self.REFCOLNAMELIB)
            print('sql1', sql)
            sql = self.makeInsertQuery(self.executeQuery(sql), self.TABLENAME)
            print('sql2', sql)

        except Exception as e:
            print('--------------------------------ERROR-----------------------')
            print('issue in execution. error:', e)
        finally:
            print('closing connection')
            self.close()

    def createSqlQuery(self, TABLENAME, COLNAMELIB):
        sql = 'select * from '+TABLENAME
        firstexecution = True

        for i in COLNAMELIB:
            if firstexecution:
                sql = sql + ' where ' + i + '=' + \
                    self.addQuotesIf(TABLENAME, i, COLNAMELIB[i])
                firstexecution = False
            else:
                sql = sql + ' and ' + i + '=' + \
                    self.addQuotesIf(TABLENAME, i, COLNAMELIB[i])
            sql = sql+';'
        return sql

    def addQuotesIf(self, TABLENAME, column_name, column_value):
        sql = 'desc ' + TABLENAME + ';'
        querydata = self.executeQuery(sql)

        for queryIter in querydata:
            # print('here4')
            if column_name.lower() == queryIter['Field']:
                # print('here5')
                if 'VARCHAR' in queryIter['Type'].decode().upper():
                    # print('here6:', column_value
                    column_value = "'" + column_value + "'"
                elif 'DATE' in queryIter['Type'].decode().upper():
                    # print('here7')
                    if column_value is None:
                        column_value = 'null'
                    else:
                        column_value = "'" + \
                            column_value.strftime('%Y-%m-%d') + "'"
        return column_value

    def checkAndMakeMultiple(self, sql, COLUMNNAMES, INSERTVALUES):
        value_to_be_changed = list(self.COLNAMELIB.items())[0][1].lower()
        range_of_value = list(self.COLNAMELIB.items())[0][1].lower().split('-')

        loop_limit = int(range_of_value[1]) - int(range_of_value[0])
        print(f'loop_limit: {loop_limit}')
        print(INSERTVALUES.split(value_to_be_changed)[1])
        final_queries = []

        for loopv in range(loop_limit):
            print(loopv)
            print(str(int(range_of_value[0]) + loopv))
            print(INSERTVALUES.split(value_to_be_changed)[1])
            final_queries.append(sql + COLUMNNAMES + ') VALUES (' +
                                 str(int(range_of_value[0]) + loopv) +
                                 INSERTVALUES.split(value_to_be_changed)[1])
        return final_queries

    def makeInsertQuery(self, querydata, TABLENAME):
        sql = 'INSERT INTO ' + TABLENAME + ' ('
        COLUMNNAMES = ''
        INSERTVALUES = ''

        # print(querydata)
        for querydataIter in querydata:
            # print(querydataIter)
            for i in querydataIter.keys():
                # print('here1')
                if i.upper() in self.COLNAMELIB:
                    querydataIter[i] = self.COLNAMELIB[i.upper()]
                # print('here2')
                COLUMNNAMES = COLUMNNAMES + i + ', '
                # print('here2.1: ', INSERTVALUES)
                if querydataIter[i] is None:
                    INSERTVALUES = INSERTVALUES + 'null' + ', '
                else:
                    INSERTVALUES = INSERTVALUES + \
                        str(self.addQuotesIf(TABLENAME,
                                             i, querydataIter[i])) + ', '
                # print('here3')
        '''print('________________________________________________')
        print(f'self.COLNAMELIB: {self.COLNAMELIB}')
        print('________________________________________________')
        print(f'INSERTVALUES: {INSERTVALUES}')
        print('________________________________________________')'''

        '''return sql + COLUMNNAMES.rstrip(' ').rstrip(',') + ') VALUES (' + INSERTVALUES.rstrip(' ').rstrip(',') + ');'  '''
        return self.checkAndMakeMultiple(sql, COLUMNNAMES.rstrip(
            ' ').rstrip(','), INSERTVALUES.rstrip(' ').rstrip(','))
