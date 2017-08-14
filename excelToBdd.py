import xlrd
from pymongo import MongoClient
import re
import glob2
import logging
from log import *

client = MongoClient('localhost', 27017)
db = client.testdb
client.drop_database(db)

def checkDates(sheet: xlrd.sheet.Sheet, cellTmp: int, cellNumber: int,cellLine: int) ->list:

    """
    :param sheet:
    :param cellTmp:
    :param cellNumber:
    :param cellLine:
    :return:
    """
    matchc = re.match(r"(.*):\s*(\d{2}\/\d{2}\/\d{4})", cellTmp)
    if re.match(r"(.*):\s*(\d{2}\/\d{2}\/\d{4})", cellTmp):
        val_content = matchc.group(1)
        date = matchc.group(2)
        return [val_content, date]
    elif re.match(r"([a-zA-Zéè]{4}\s[a-zA-Zéè]{2}\s[a-zA-Zéè]{10}\s[a-zA-Zéè]{8}:)", cellTmp):
        matchn = re.match(r"([a-zA-Zéè]{4}\s[a-zA-Zéè]{2}\s[a-zA-Zéè]{10}\s[a-zA-Zéè]{8}:)", cellTmp)
        val_content = matchn.group(1)

        datec = sheet.cell(cellLine, cellNumber + 2)

        if xlrd.xldate_as_tuple(datec.value, 0):
            date = xlrd.xldate_as_tuple(datec.value, 0)
            str_date=str(date[2])+"/"+str(date[1])+"/"+str(date[0])
            return [val_content,str_date]
        else:
            raise ValueError("no normal date ")


def get_file()->list:
    """

    :return:
    """
    files =glob2.glob("/home/aurelien/test_import_BDD/*.xls")
    if len(files)==0:
        raise Exception("file not found")

    return files
file_list = get_file()


def extract_content(file: str)->dict:
    """
    :param file:
    :return:
    """
    book = xlrd.open_workbook(file , formatting_info=True)
    color_map= book.colour_map
    sheets = book.sheet_names()
    patient= {}
    for index, sh in enumerate(sheets):
        sheet = book.sheet_by_index(index)
        rows, cols = sheet.nrows, sheet.ncols
        if rows>0 and cols>0:
            patient["name"] = sheet.cell(0,0).value
            if sheet.cell(0, 2).value!='':
                patient["barcode"] = sheet.cell(0, 2).value
                patient["samplename"] = sheet.cell(0, 3).value
            elif  sheet.cell(0, 3).value!='':
                patient["barcode"] = sheet.cell(0, 3).value
                patient["samplename"] = sheet.cell(0, 4).value
            else :
                logging.info("problem in %s"%(file))

            val_content=""
            date=""
            for cellnumber in range(7, 9):
                celld = sheet.cell(0, cellnumber).value
                if checkDates(sheet, celld,cellnumber,0):
                    patient [checkDates(sheet, celld,cellnumber,0)[0]] = checkDates(sheet, celld,cellnumber,0)[1]
                cellt = sheet.cell(1,cellnumber).value
                if checkDates(sheet, cellt, cellnumber,1):
                    patient[checkDates(sheet, cellt,cellnumber,1)[0]] = checkDates(sheet, cellt,cellnumber,1)[1]
            genlist=[]

            for row in range (rows):
                if re.match(r"Couverture Moy/ Amp",str(sheet.cell(row,0).value)):
                    for rownumber in range(row, row+8):
                        for col in range(0, cols):
                            if type(sheet.cell(rownumber,col).value) == str and sheet.cell(rownumber,col).value!="":
                                tab = sheet.cell(rownumber, col).value
                                genlist.append(tab)
                                patient["genelist"]=genlist

                if re.match(r"Résultats JSI", str(sheet.cell(row, 0).value)):
                    end_mutation_col=0
                    for rownumber2 in range(row+2, rows):
                        for col in range(1,cols):
                            if re.match(r"position", str(sheet.cell(rownumber2-1, col).value)):
                                end_mutation_col = col
                        mutationValueList = []
                        for col in range(1,end_mutation_col+1):
                            if sheet.cell(rownumber2,col).value!=" " and sheet.cell(rownumber2,col).value!="":
                                mutationValueList.append(sheet.cell(rownumber2,col).value)
                        xfx = sheet.cell_xf_index(rownumber2, 1)
                        xf = book.xf_list[xfx]
                        bgx = xf.background.pattern_colour_index
                        rgb_value = color_map[bgx]
                         #list(rgb_value)pymongo_test
                        mutationValueList.append(rgb_value)



                        logging.debug(mutationValueList)
                        mutation_id = sheet.cell(rownumber2, 0).value +'_'+ sheet.cell(rownumber2, 1).value+'_'+sheet.cell(rownumber2, 9).value
                        mut_id = mutation_id.replace('.', r"\U+002E")
                        logging.debug(mut_id)
                        patient[mut_id]=mutationValueList

            logging.info(patient)
            return patient

if __name__ == '__main__':
    logging.info("beginning of analysis")
    logging.info("beginning of data extraction")
    patients=[]
    for file in get_file():
        patients.append(extract_content(file))
        logging.info("extracting %s file"%file)
        
    posts = db.posts
    logging.info("adding in the database")
    result = posts.insert_many(patients)

    test_post = posts.find_one({"samplename": "L1706268"})
    test_post2 = posts.find_one({"samplename": "L1706263"})
    test_post3 = posts.find_one({"samplename": "L1706269"})
    print(test_post)
    print (test_post2)
    print(test_post3)
    logging.info("end of the analysis")