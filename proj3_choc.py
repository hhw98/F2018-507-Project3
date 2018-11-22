import sqlite3
import csv
import json
import sys
import numpy as np

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
'''
with open (COUNTRIESJSON,encoding='utf-8') as f:
    CSV_contents = f.read()
    CSV_List = json.loads(CSV_contents)
    dumped_json = json.dumps(CSV_List,indent = 4,separators=(',', ':'))
    fw = open(COUNTRIESJSON,"w")
    fw.write(dumped_json)
    #for ele in CSV_List:
        #print(ele['alpha2Code'])
'''
conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

statement = '''
    DROP TABLE IF EXISTS 'Countries';
'''
cur.execute(statement)
statement = '''
    CREATE TABLE 'Countries'(
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Alpha2' TEXT NOT NULL,
        'Alpha3' TEXT NOT NULL,
        'EnglishName' TEXT NOT NULL,
        'Region' TEXT NOT NULL,
        'Subregion' TEXT NOT NULL,
        'Population' INTEGER NOT NULL,
        'Area' Real
    );
'''
cur.execute(statement)

statement = '''
    DROP TABLE IF EXISTS 'Bars';
'''
cur.execute(statement)

statement = '''
    CREATE TABLE 'Bars'(
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Company' TEXT NOT NULL,
        'SpecificBeanBarName' TEXT NOT NULL,
        'REF' TEXT NOT NULL,
        'ReviewData' TEXT NOT NULL,
        'CocoaPercent' Real NOT NULL,
        'CompanyLocationId' INTEGER ,
        'Rating' Real NOT NULL,
        'BeanType' TEXT,
        'BroadBeanOriginId' INTEGER
    );
'''
cur.execute(statement)


with open (COUNTRIESJSON,encoding='utf-8') as f:
    CSV_contents = f.read()
    CSV_List = json.loads(CSV_contents)
    for ele in CSV_List:
        A = []
        statement = 'INSERT INTO "Countries" (Alpha2,Alpha3,EnglishName,Region,Subregion,Population,Area)'
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?)'
        A = [ele["alpha2Code"],ele["alpha3Code"],ele["name"],ele["region"],ele["subregion"],ele["population"],ele["area"]]
        #print(ele["name"])
        cur.execute(statement,A)

with open ('flavors_of_cacao_cleaned.csv',encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        #print(row)
        company = row[0]
        SpecificBeanBarName = row[1]
        REF = row[2]
        ReviewDate = row[3]
        CocoaPercent = row[4].strip('%')
        if row[5]=='Unknown':
            CompanyLocationId = 'Unknown'
        else:
            CompanyLocation = (row[5],)
            statement = 'SELECT Id FROM Countries WHERE EnglishName = ?'
            cur.execute(statement,CompanyLocation)
            #print(cur)
            for r in cur:
                CompanyLocationId = r[0]
        Rating = row[6]
        BeanType = row[7]
        if row[8] == 'Unknown':
            #print('Unknown')
            BroadBeanOriginId = 'Unknown'
        else:
            BroadBeanOrigin = (row[8],)
            statement = 'SELECT Id FROM Countries WHERE EnglishName = ?'
            cur.execute(statement, BroadBeanOrigin)
            for r in cur:
                BroadBeanOriginId = r[0]
        B = []
        statement = 'INSERT INTO "Bars" (Company,SpecificBeanBarName,REF,ReviewData,CocoaPercent,CompanyLocationId,Rating,BeanType,BroadBeanOriginId)'
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
        A = [company,SpecificBeanBarName,REF,ReviewDate,CocoaPercent,CompanyLocationId,Rating,BeanType,BroadBeanOriginId]
        cur.execute(statement,A)

conn.commit()
conn.close()


# Part 2: Implement logic to process user commands
def process_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    for r in cur:
        print(r)
    r = command.split()
    if r[0] == 'bars':
        #print(r[1:])
        statement = ''
        Sequence = 'DESC '
        num = 'LIMIT 10 '
        order = 'ORDER BY Rating '
        for ele in r[1:]:
            item = ele.split('=')
            #print(item)
            if item[0] == 'sellcountry':
                #print('sellcountry')
                sellcountry = item[1]
                statement = 'WHERE sell.Alpha2 = "'+sellcountry+'" '
                #statement = 'WHERE sell.EnglishName = ? '
                #print(statement)
            elif item[0] == 'sourcecountry':
                sourcecountry = item[1]
                statement = 'WHERE source.Alpha2 = "'+sourcecountry+'" '
            elif item[0] == 'sellregion':
                sellregion = item[1]
                statement = 'WHERE sell.Region = "'+sellregion+'" '
            elif item[0] == 'sourceregion':
                sourceregion = item[1]
                statement = 'WHERE source.Region = "'+sourceregion+'" '
            elif item[0] == 'top':
                Sequence = 'DESC '
                try:
                    num = 'LIMIT {} '.format(item[1])
                except:
                    num = 'LIMIT 10 '

            elif item[0] == 'bottom':
                Sequence = 'ASC '
                try:
                    num = 'LIMIT {} '.format(item[1])
                except:
                    num = 'LIMIT 10 '

            elif item[0] == 'ratings':
                order = 'ORDER BY Rating '
            elif item[0] == 'cocoa':
                order = 'ORDER BY CocoaPercent '
                #print('Hello')
            else:
                print('Command not recognized: {}'.format(command))
                conn.close()
                return None
        #print(statement)
        #print(order)
        s = 'SELECT SpecificBeanBarName, Company, sell.EnglishName, ROUND(Rating,1), CocoaPercent, source.EnglishName FROM Bars '
        s += 'LEFT JOIN countries AS sell ON Bars.CompanyLocationId = sell.Id '
        s += 'LEFT JOIN countries AS source ON Bars.BroadBeanOriginId = source.Id '
        s += statement
        s += order
        s += Sequence
        s += num
        #print(s)
        cur.execute(s)

    elif r[0] == 'companies':
        statement = ''
        Sequence = 'DESC '
        num = 'LIMIT 10 '
        agg = 'ROUND(AVG(Rating),1)'
        order = 'ORDER BY AVG(Rating) '
        for ele in r[1:]:
            item = ele.split('=')
            #print(item)
            if item[0] == 'country':
                #print('sellcountry')
                country = item[1]
                statement = 'AND Alpha2 = "'+country+'" '
                #statement = 'WHERE sell.EnglishName = ? '
                #print(statement)
            elif item[0] == 'region':
                region = item[1]
                statement = 'AND Region = "'+region+'" '
            elif item[0] == 'top':
                Sequence = 'DESC '
                try:
                    num = 'LIMIT {} '.format(item[1])
                except:
                    num = 'LIMIT 10 '

            elif item[0] == 'bottom':
                Sequence = 'ASC '
                try:
                    num = 'LIMIT {} '.format(item[1])
                except:
                    num = 'LIMIT 10 '

            elif item[0] == 'ratings':
                agg = 'ROUND(AVG(Rating),1)'
                order = 'ORDER BY AVG(Rating) '
            elif item[0] == 'cocoa':
                agg = 'ROUND(AVG(CocoaPercent),1)'
                order = 'ORDER BY AVG(CocoaPercent) '
            elif item[0] == 'bars_sold':
                agg = 'COUNT(SpecificBeanBarName)'
                order = 'ORDER BY COUNT(SpecificBeanBarName) '
                #print('Hello')

            else:
                print('Command not recognized: {}'.format(command))
                conn.close()
                return None
        #print(statement)
        #print(order)
        s = 'SELECT Company, EnglishName, {} FROM Bars '.format(agg)
        s += 'LEFT JOIN countries ON Bars.CompanyLocationId = countries.Id '
        s += 'GROUP BY Company '
        s += 'HAVING COUNT(SpecificBeanBarName) > 4 '
        s += statement
        s += order
        s += Sequence
        s += num
        #print(s)
        cur.execute(s)
        #for r in cur:
            #print(r)
    elif r[0] == 'countries':
        statement = 'LEFT JOIN Countries AS seller ON Bars.CompanyLocationId = seller.Id '
        Sequence = 'DESC '
        num = 'LIMIT 10 '
        agg = 'ROUND(AVG(Rating),1)'
        order = 'ORDER BY AVG(Rating) '
        region_s =''
        for ele in r[1:]:
            item = ele.split('=')
            #print(item)
            if item[0] == 'region':
                #print('sellcountry')
                region = item[1]
                region_s = 'AND Region = "'+region+'" '
                #statement = 'WHERE sell.EnglishName = ? '
                #print(statement)
            elif item[0] == 'sellers':
                statement = 'LEFT JOIN Countries AS seller ON Bars.CompanyLocationId = seller.Id '
            elif item[0] == 'sources':
                statement = 'LEFT JOIN Countries AS source ON Bars.BroadBeanOriginId = source.Id '
            elif item[0] == 'top':
                Sequence = 'DESC '
                try:
                    num = 'LIMIT {} '.format(item[1])
                except:
                    num = 'LIMIT 10 '

            elif item[0] == 'bottom':
                Sequence = 'ASC '
                try:
                    num = 'LIMIT {} '.format(item[1])
                except:
                    num = 'LIMIT 10 '

            elif item[0] == 'ratings':
                agg = 'ROUND(AVG(Rating),1)'
                order = 'ORDER BY AVG(Rating) '
            elif item[0] == 'cocoa':
                agg = 'ROUND(AVG(CocoaPercent),1)'
                order = 'ORDER BY AVG(CocoaPercent) '
            elif item[0] == 'bars_sold':
                agg = 'COUNT(SpecificBeanBarName)'
                order = 'ORDER BY COUNT(SpecificBeanBarName) '
                #print('Hello')

            else:
                print('Command not recognized: {}'.format(command))
                conn.close()
                return None
        #print(statement)
        #print(order)
        s = 'SELECT EnglishName, Region, {} FROM Bars '.format(agg)
        s += statement
        s += 'GROUP BY EnglishName '
        s += 'HAVING COUNT(SpecificBeanBarName) > 4 '
        s += region_s
        s += order
        s += Sequence
        s += num
        #print(s)
        cur.execute(s)
        #for r in cur:
            #print(r)
    elif r[0] == 'regions':
        statement = 'INNER JOIN Countries AS seller ON Bars.CompanyLocationId = seller.Id '
        Sequence = 'DESC '
        num = 'LIMIT 10 '
        agg = 'ROUND(AVG(Rating),1)'
        order = 'ORDER BY AVG(Rating) '
        for ele in r[1:]:
            item = ele.split('=')
            if item[0] == 'sellers':
                statement = 'INNER JOIN Countries AS seller ON Bars.CompanyLocationId = seller.Id '
            elif item[0] == 'sources':
                statement = 'INNER JOIN Countries AS source ON Bars.BroadBeanOriginId = source.Id '
            elif item[0] == 'top':
                Sequence = 'DESC '
                num = 'LIMIT {} '.format(item[1])

            elif item[0] == 'bottom':
                Sequence = 'ASC '
                num = 'LIMIT {} '.format(item[1])

            elif item[0] == 'ratings':
                agg = 'ROUND(AVG(Rating),1)'
                order = 'ORDER BY AVG(Rating) '
            elif item[0] == 'cocoa':
                agg = 'ROUND(AVG(CocoaPercent),1)'
                order = 'ORDER BY AVG(CocoaPercent) '
            elif item[0] == 'bars_sold':
                agg = 'COUNT(SpecificBeanBarName)'
                order = 'ORDER BY COUNT(SpecificBeanBarName) '
                #print('Hello')

            else:
                print('Command not recognized: {}'.format(command))
                conn.close()
                return None
        #print(statement)
        #print(order)
        s = 'SELECT Region, {} FROM Bars '.format(agg)
        s += statement
        s += 'GROUP BY Region '
        s += 'HAVING COUNT(SpecificBeanBarName) > 4 '
        s += order
        s += Sequence
        s += num
        #print(s)
        cur.execute(s)
    else:
        print('Command not recognized: {}'.format(command))
    #return []
    result = cur.fetchall()
    conn.close()
    return result

def load_help_TEXT():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!

def interactive_prompt():
    help_TEXT = load_help_TEXT()
    response = ''
    while response != 'exit':
        response = input('\nEnter a command: ')
        #print(response)

        if response == 'help':
            print(help_TEXT)
            continue
        elif len(response) == 0:
            continue
        elif response == 'exit':
            print('bye')
            break
        else:
            cur = process_command(response)
            if cur == None:
                continue
            else:
                delim = '\t'  # tweak to your preference    str('{0: <5}'.format(str(i[:5]))
                for element in cur:
                    s=''
                    #print(type(element))
                    #print(int((element[4])))
                    for i in range(len(element)):
                        #print(type(element[i]))
                        if element[i] == None:
                            s = s+'Unknown'+'\t'
                        elif type(element[i]) is float:
                            percent = int(element[i])
                            if percent>5:
                                s = s+('{}%'.format(int(element[i])))+'\t'
                                #print(element[i])
                            else:
                                s = s+('{}'.format(str(element[i])))+'\t'
                        elif len(str(element[i])) > 12:
                            s = s+('{0: <12}...'.format(str(element[i])[:12]))+'\t'
                        else:
                            s = s+('{0: <15}'.format(str(element[i])))+'\t'
                        #s = s+('{0: <5}... ...'.format(str(i[:5])) if len(str(i)) > 5 else ('{0: <5}... ...'.format(str(i[:5]))) )+'\t'
                    print(s)
                '''
                for i in element：
                    str(i)
                element = list(element)
                print(element)
                #print(delim.join((str(i[:5]) + '...') if len(str(i)) > 5 else str(i) for i in element))
                for i in element:
                    print(i)
                    print(delim.join('{0: <5}... ...'.format(str(i[:5])) if len(str(i)) > 5 else ('{0: <5}... ...'.format(str(i[:5]))) ))
                '''
            #for r in cur:
                #for i in r:
                    #print(i)
                #print('\n')


# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
