#!/usr/bin/python

from os import listdir
from os.path import isfile, join
from collections import defaultdict
import datetime
import jaydebeapi

def find_files(mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    transact_load = {datetime.datetime.strptime(f[13:-4], '%d%m%Y').date():f for f in onlyfiles if f[:12] == 'transactions'}
    passports_load = {datetime.datetime.strptime(f[19:-5], '%d%m%Y').date():f for f in onlyfiles if f[:8] == 'passport'}
    terminals_load = {datetime.datetime.strptime(f[10:-5], '%d%m%Y').date():f for f in onlyfiles if f[:9] == 'terminals'}
    
    day_load = defaultdict(list)

    for d in (transact_load, passports_load, terminals_load): 
        for key, value in d.items():
            day_load[key].append(value)
    
    to_check = []
    for key in sorted(day_load):
        to_check.append((key, day_load[key]))
   
    if len(to_check) == 0:
        SystemExit('there are no files to upload') 

    return to_check  



def check_files(to_check):

    if len(to_check[1]) != 3:
        raise SystemExit('there are no all 3 files for date ',to_check[0])    

    conn = jaydebeapi.connect( 'oracle.jdbc.driver.OracleDriver',
                               'jdbc:oracle:thin:itde1/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
                               ['itde1', 'bilbobaggins'],
                               '/home/itde1/ojdbc8.jar' )
    curs = conn.cursor()    
    
    curs.execute(''' SELECT LAST_LOAD FROM ITDE1.WEXZ_META_LOAD ''')
    last_upload = datetime.datetime.strptime(curs.fetchall()[0][0], '%Y-%m-%d %H:%M:%S').date()
    
    
    if last_upload + datetime.timedelta(days=1) != to_check[0]:
        raise SystemExit('there is more than 1 day between day of files to load and last upload ')
	
    curs.close()
    conn.close()
    
