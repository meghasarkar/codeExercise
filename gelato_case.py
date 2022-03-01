import urllib.request
import json
import pandas as pd
import psycopg2


"""Read the file content from URL provided"""
link = "https://data.cdc.gov/api/views/cjae-szjv/rows.json?accessType=DOWNLOAD"
f = urllib.request.urlopen(link)
contentFile = f.read()

"""Dummy file for test"""
#contentFile = r'Gelato\generated.json'

#don't have to put the column names, but it will make the excel in nicer order
df = pd.read_json(contentFile)

"""To drop the row in dataframe if any values are missing.
thresh=2 means if 2 NA or NAN found in rows, it will be dropped"""
df.dropna(thresh=2)

"""Get column names from the dataframe"""
columnName = list(df.columns.values)
print(columnName)

"""Deciding data types for SQL table is bit tricky. As data types of pandas data-frame and DBMS are completely different.
For collecting the data types in to a list we can write a small piece of code like"""
def getColumnDtypes(dataTypes):
    dataList = []
    for x in dataTypes:
        if(x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar')
    return dataList
columnDataType = getColumnDtypes(df.dtypes)
print(columnDataType)


"""Now we have lists of all the column names and column data types . 
We need to write a small piece of code again to complete create table statement"""
createTableStatement = 'CREATE TABLE IF NOT EXISTS gelatoTest ('
for i in range(len(columnDataType)):
    createTableStatement = createTableStatement + '\n' + columnName[i] + ' ' + columnDataType[i] + ','
createTableStatement = createTableStatement[:-1] + ' );'


"""Connect to database server to run create table statement"""
connect2DB = psycopg2.connect(dbname='testdb', host='https://10.10.10.10', port='5432', user='mydbuser',
                              password='testdb')
cur2DB = connect2DB.cursor()
cur2DB.execute(createTableStatement)
connect2DB.commit()

"""Load the data from the dataframe values to the table"""
for i,row in df.iterrows():
    sql = "INSERT INTO `gelatoTest` (`" +columnName + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))
connect2DB.commit()






