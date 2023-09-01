import json
from replication import connectLocal

# Getting Table Names, all collumns from each table and creating a JSON
def gettigTableNames():
    try:
        conn = connectLocal()
        cursor = conn.cursor()
        cursor.execute("""SELECT table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE' AND table_schema = 'public';
    """)
        table_names = cursor.fetchall()
        tableNamesList = [name[0] for name in table_names]
    except Exception as E:
        print(E)
    return tableNamesList

def getColumnNames(tablename):
    try:
        conn = connectLocal()
        cursor = conn.cursor()
        cursor.execute(f"""SELECT column_name,data_type
            FROM information_schema.columns
            WHERE table_name = '{tablename}'
            ORDER BY ordinal_position;""")
        columnsNames = cursor.fetchall()
        columnsNamesList = [{"column":columnName[0], "data_type":columnName[1]} for columnName in columnsNames]
    except Exception as E:
        print(E)
    return columnsNamesList

def creatingJson():
    try:
        tableNameList = gettigTableNames()
        jsonObject = {"tables": []}
        for tableName in tableNameList:
            columnNamesList = getColumnNames(tableName)
            tableInfo = {
                "table": tableName,
                "columns": [col_name for col_name in columnNamesList]
            }
            jsonObject["tables"].append(tableInfo)
    except Exception as E:
        print(E)
    return jsonObject

#Saving Json

def savingJson():
    try:
        jsonObject = creatingJson()
        with open('database.json','w') as file:
            json.dump(jsonObject,file,indent=4)
    except Exception as E:
        print(E)

#Reading and Separating json

def readingJson(pathJson):
    try:
        with open(pathJson,'r') as file:
            data = json.load(file)
            return data
    except Exception as E:
        print(E)
    
def separatingData():
    try:
        path = 'database.json'
        jsonObject = readingJson(path)
        tableList = []
        for table in jsonObject["tables"]:
            tableList.append(table["table"])
            columnList = []
            for column in table["columns"]:
                columnName = column["column"]
                columnDatatype = column["data_type"]
                columnList.append(columnName)
                columnList.append(columnDatatype)

            tableList.append(columnList)
    except Exception as E:
        print(E)
    return tableList
