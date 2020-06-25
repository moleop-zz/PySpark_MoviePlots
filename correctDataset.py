#!usr/bin/env python

import csv
import re

with open('movieplots_fixed.csv', 'w') as outfile:
    spamwriter = csv.writer(outfile, delimiter=',', quotechar='"')
    with open('movieplots.csv') as infile:
        spamreader = csv.reader(infile, delimiter=',', quotechar='"')
        for row in spamreader:
            for col in range(7):
                row[col] = row[col].replace('\n','; ')
                row[col] = row[col].replace(',',';')
            tmp = " ".join(row[7].split())
            tmp = tmp.replace(',','')
            tmp = tmp.replace('.','')
            tmp = tmp.replace('?','')
            tmp = tmp.replace('!','')
            tmp = tmp.replace('"','')
            tmp = tmp.replace(';','')
            tmp = tmp.replace(':','')
            tmp = tmp.replace(' - ',' ')
            tmp = tmp.replace('\'s','')
            tmp = tmp.replace('/',' ')
            tmp = tmp.replace('(','')
            tmp = tmp.replace(')','')
            tmp = tmp.replace('{','')
            tmp = tmp.replace('}','')
            tmp = tmp.replace('‍—‌',' ')
            tmp = tmp.replace('—',' ')

            tmp = tmp.replace('\\u',' ')
            tmp = tmp.replace('\\t',' ')
            tmp = tmp.replace('\\a',' ')

            row[7] = re.sub('\[.*?\]','',tmp)
            spamwriter.writerow(row)
