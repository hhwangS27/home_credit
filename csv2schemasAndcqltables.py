import os, os.path
import re
import csv

def desc2dict(inputp):
    #the output is like {tableName+clmnName:{schmT:,cqlT:}}
    targetc = re.compile(r"^(.+).csv$")
    with open(inputp, encoding='mac_roman') as fin:
        f_csv = csv.DictReader(fin)
        targetc = re.compile(r"^(.+).csv$")
        descdict = {}
        for row in f_csv:
            if row['Table']=="application_{train|test}.csv":
                #for train
                tableName = "application_train"
                clmnName = row["Row"].replace(' ', '')
                schmT = row["DataFrame Type"].replace(' ', '')
                cqlT = row["CQL Type"].replace(' ', '')
                descdict[tableName+clmnName]={"schmT":schmT,"cqlT":cqlT}

                #for test
                tableName = "application_test"
                descdict[tableName+clmnName]={"schmT":schmT,"cqlT":cqlT}

            else:
                tmp = targetc.match(row['Table'])
                tableName = tmp.group(1)
                clmnName = row["Row"].replace(' ', '')
                schmT = row["DataFrame Type"].replace(' ', '')
                cqlT = row["CQL Type"].replace(' ', '')
                descdict[tableName+clmnName]={"schmT":schmT,"cqlT":cqlT}

    return descdict

def get_tableName_columns(inputp):
    targetc = re.compile(r"^(.+).csv$")
    tableName_columns = {}
    for (dpath, dnames, fnames) in os.walk(inputp):
        for fn in fnames:
            fn_exc_suf = targetc.match(fn)
            if fn_exc_suf:
                fn_exc_suf = fn_exc_suf.group(1) # file name excluding of suffix
                with open(dpath+fn, encoding='mac_roman') as fin:
                    f_csv = csv.reader(fin)
                    columns = next(f_csv)
                    tableName_columns.update({fn_exc_suf:columns})
    return tableName_columns

def outputSchemas(tableName_columns, desc, outputp):
    output = 'schemas = {'

    for tableName, columns in tableName_columns.items():
        output += "'{}':\ntypes.StructType([\n".format(tableName)
        for c in columns:
            if c.startswith('SK_ID_'):
                nullable = "False"
            else:
                nullable = "True"

            output += "                     types.StructField('{}', types.{}(), {}),\n" \
                                                .format(c, desc[tableName+c]["schmT"], nullable)

        output += "                ]),\n"

    output += '}'

    print(output)
    with open(outputp, 'w') as f:
        f.write(output)


"""CREATE TABLE IF NOT EXISTS {}(
                id UUID,
                host TEXT,
                datetime TIMESTAMP,
                path TEXT,
                bytes_ INT,
                PRIMARY KEY (host, id));"""

def outputCQLTables(tableName_columns, desc, outputp):
    output = ""
    for tableName, columns in tableName_columns.items():
        output += "CREATE TABLE IF NOT EXISTS {}(\n"
        primary_key = []
        for c in columns:
            if c.startswith('SK_ID_'):
                primary_key.append(c)

            output += "{} {},\n" \
                         .format(c, desc[tableName+c]["cqlT"])
        output += "PRIMARY KEY ({}));\n\n".format(','.join(primary_key))

    print(output)
    with open(outputp, 'w') as f:
        f.write(output)

def main():
    desc = desc2dict('HomeCredit_columns_description.csv') #desc is like {tableName+clmnName:{schmT:,cqlT:}}
    tableName_columns = get_tableName_columns("20LineCSVs/")#tables_... is like {tableName:[c1, c2, ..]}

    print(desc)
    outputSchemas(tableName_columns, desc, "schemas.py")

    outputCQLTables(tableName_columns, desc, "cqltables.txt")


if __name__ == "__main__":
    main()

