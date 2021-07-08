#!/usr/bin/python

from py_scripts import what_load
from py_scripts import ETL_main
from py_scripts import rep_constr
import os
import shutil

path = './'
path_sql_etl = './sql_scripts/ETL_scripts.sql'
path_sql_rep = './sql_scripts/report.sql'

to_check = what_load.find_files(path)

for files in to_check:
    print('start transaction. files to load:',files[1])
    what_load.check_files(files)
    ETL_main.ETL(path_sql_etl,files[1],files[0])
    print('end of trunsaction')
    
    rep_constr.constract_report(path_sql_rep)
    
    shutil.move(files[1][0],'archive/' + files[1][0] + '.backup')
    shutil.move(files[1][1],'archive/' + files[1][1] + '.backup')
    shutil.move(files[1][2],'archive/' + files[1][2] + '.backup')
    


