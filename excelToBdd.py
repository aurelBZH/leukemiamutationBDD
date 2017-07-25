import xlrd
import sys
from collections import OrderedDict
import simplejson as json
import pymongo
from pymongo import MongoClient
import re
import glob2
import logging
client = MongoClient('localhost', 27017)
db = client.pymongo_test


def checkDates(sheet, cellTmp, cellNumber,cellLine):
    matchc = re.match(r"(.*):\s*(\d{2}\/\d{2}\/\d{4})", cellTmp)
    if re.match(r"(.*):\s*(\d{2}\/\d{2}\/\d{4})", cellTmp):
        val_content = matchc.group(1)
        date = matchc.group(2)
        return [val_content, date]
    elif re.match(r"([a-zA-Zéè]{4}\s[a-zA-Zéè]{2}\s[a-zA-Zéè]{10}\s[a-zA-Zéè]{8}:)", cellTmp):
        matchn = re.match(r"([a-zA-Zéè]{4}\s[a-zA-Zéè]{2}\s[a-zA-Zéè]{10}\s[a-zA-Zéè]{8}:)", cellTmp)
        val_content = matchn.group(1)
        print ("ihihih")

        datec = sheet.cell(0, cellNumber + 2)
        print("try again")
        print(datec.value)

        if re.match(r"(\d{2}\/\d{2}\/\d{4})", str(datec.value)):
            dateMatch = re.match(r"(\d{2}\/\d{2}\/\d{4})", datec.value)
            date = dateMatch.group(1)
            return [val_content,date]
        else:
            raise ValueError("no valuable date ")


def get_file():
    files =glob2.glob("/home/aurelien/test_import_BDD/*.xls")
    if len(files)==0:
        raise Exception("file not found")

    return files
file_list = get_file()


def extract_content(file):
    book = xlrd.open_workbook(file , formatting_info=True)
    print("extcont"+file)
    sheets = book.sheet_names()
    patient= {}
    print ("sheets are:", sheets)
    for index, sh in enumerate(sheets):
        sheet = book.sheet_by_index(index)
        print ("Sheet:", sheet.name)
        print(sheet)
        rows, cols = sheet.nrows, sheet.ncols
        if rows>0 and cols>0:
            print ("Number of rows: %s   Number of cols: %s" % (rows, cols))
            print(sheet.cell(0,0).value)
            patient["name"] = sheet.cell(0,0).value
            patient["barcode"] = sheet.cell(0, 2).value
            patient["samplename"] = sheet.cell(0,3).value
            val_content=""
            date=""
            for cellnumber in range(7, 9):
                celld = sheet.cell(0, cellnumber).value
                print(celld)
                print ("test")
                if checkDates(sheet, celld,cellnumber,0):
                    patient [checkDates(sheet, celld,cellnumber,0)[0]] = checkDates(sheet, celld,cellnumber,0)[1]
                cellt = sheet.cell(1,cellnumber).value
                if checkDates(sheet, cellt, cellnumber,1):
                    patient[checkDates(sheet, cellt,cellnumber,1)[0]] = checkDates(sheet, cellt,cellnumber,1)[1]

            for row in range (rows):
                if re.match(r"Couverture Moy/ Amp",str(sheet.cell(row,0).value)):
                    for rownumber in range(row, row+8):
                        for col in range(cols):
                            if type(sheet.cell(rownumber,col).value) == str:
                                tab = sheet.cell(rownumber, col).value.split("\n")
                                if len(tab)==3:
                                    patient[tab[0]]=[tab[1],tab[2]]
                if re.match(r"Résultats JSI", str(sheet.cell(row, 0).value)):

                    for rownumber2 in range(row+2, rows):
                        mutationValueList = []
                        for col in range(1,cols):
                            xfx = sheet.cell_xf_index(rownumber2, col)
                            xf = book.xf_list[xfx]
                            bgx = xf.background.pattern_colour_index
                            if bgx!=64:
                                mutationValueList.append(bgx)
                            mutationValueList.append(sheet.cell(rownumber2,col).value)
                            print(sheet.cell(rownumber2, 0).value)
                            xfx = sheet.cell_xf_index(rownumber2, col)
                            xf = book.xf_list[xfx]
                            bgx = xf.background.pattern_colour_index
                        patient[sheet.cell(rownumber2, 0).value]=mutationValueList
            print(patient["name"])
            print(patient)
            return patient
patients=[]
print(get_file())
for file in get_file():
    print("boucle "+file)
    patients.append(extract_content(file))
        
posts = db.posts
result = posts.insert_many(patients)
print("the end ")
#print('One post: {0}'.format(result.inserted_id))

test_post = posts.find_one({"samplename": "L1706221"})
print(test_post)