import pandas as pd
from insertcreate import InsertCreate
from createinsert import CreateInsert


DBCONNECTION = "MYSQL"


class MainProcess (CreateInsert):
    def __init__(self):
        FILEPATH = '/Users/ratnesh/apps/db-auto-tool/scripts/'
        FILENAME = 'input.csv'
        self.FILEPATH = FILEPATH
        self.FILENAME = FILENAME

    def runProcess(self):
        df = pd.read_csv(self.FILEPATH+self.FILENAME)

        # divide the dataframe for each table
        val = df[df['TABLENAME'] == '~'].index.values
        firstVal = 0
        secondVal = 0
        tdObj = InsertCreate()
        for i in val:
            secondVal = i
            subDF = df.iloc[firstVal:secondVal, :]
            subDF.reset_index(drop=True, inplace=True)
            # print(subDF)
            firstVal = secondVal + 1
            # functions to ready all the values from the data from and load it variables tabledata.
            tdObj.loadAllData(subDF)
            # creating insert query from the data
            tdObj.createInsertQuery()

            # creating select query from the data

            # creating delete query from the data

        # process subdata frames.


if __name__ == '__main__':
    mpObj = MainProcess()
    mpObj.runProcess()
