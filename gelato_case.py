import urllib.request
import json
import pandas as pd
import psycopg2

"""Read the file content from URL provided"""
# link = "https://data.cdc.gov/api/views/cjae-szjv/rows.json?accessType=DOWNLOAD"
# f = urllib.request.urlopen(link)
# contentFile = f.read()

"""Dummy file for test"""
contentFile = r'C:\Users\megha\Documents\Gelato\rows.json'

with open(contentFile) as json_file:
    data_test = json.load(json_file)

# To normalize the values in data feild of JSON and storinf to a df
df_data2table = pd.json_normalize(data_test, record_path=['data'])

# To extract the column name for the table from the meta.view.column in json
df = pd.json_normalize(data_test, record_path=['meta', 'view', 'columns'])
columnName = df["name"].tolist()
print(columnName)


def getColumnDtypes(dataTypes):
    dataList = []
    for x in dataTypes:
        if x == 'int64':
            dataList.append('int')
        elif x == 'float64':
            dataList.append('float')
        elif x == 'bool':
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
for i, row in df.iterrows():
    sql = "INSERT INTO `gelatoTest` (`" + columnName + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
    cur2DB.execute(sql, tuple(row))
connect2DB.commit()
