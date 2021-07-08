#!/usr/bin/python

import pandas as pd
import jaydebeapi
pd.options.mode.chained_assignment = None
import datetime


#day_data  = [transactions, passports, terminals] 
def ETL(path_sql,day_data,date_of_upload):
    fd = open(path_sql, 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')
    sqlCommands = sqlCommands[:-1]
    
    conn = jaydebeapi.connect( 'oracle.jdbc.driver.OracleDriver',
                               'jdbc:oracle:thin:itde1/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
                               ['itde1', 'bilbobaggins'],
                               '/home/itde1/ojdbc8.jar' )
    conn.jconn.setAutoCommit(False)
    curs = conn.cursor()

    num_stg = 7 #number of stg tables
    for command in sqlCommands[:num_stg]:
        curs.execute(command)

    transactions = pd.read_csv(day_data[0], delimiter=";")
    transactions = transactions[['transaction_id','transaction_date','card_num','oper_type','amount','oper_result','terminal']]

    curs.executemany('''INSERT INTO ITDE1.WEXZ_STG_TRANSACTIONS(TRANS_ID,TRANS_DATE,CARD_NUM,OPER_TYPE,
	                                AMT,OPER_RESULT,TERMINAL)
        VALUES (?,?,?,?,?,?,?)''',
        transactions.values.tolist())
    del transactions


    passports = pd.read_excel(day_data[1])
    passports = passports[['passport','date']]

    curs.execute(''' SELECT LAST_UPDATE FROM ITDE1.WEXZ_META_DATE 
	                        WHERE DBNAME = 'ITDE1' 
							AND TABLENAME = 'WEXZ_DWH_FACT_PSSPRT_BLCKLST' ''')
    #find only new passports 
    result = curs.fetchall()[0][0]
    result = pd.Timestamp(result)
    passports = passports.loc[passports['date'] > result]
    passports['date'] = passports['date'].astype(str)

    curs.executemany('''INSERT INTO ITDE1.WEXZ_STG_PSSPRT_BLCKLST(PASSPORT_NUM,ENTRY_DT)
        VALUES (?,?)''',
        passports.values.tolist())
    del passports


    terminals = pd.read_excel(day_data[2])
    #capture id for deletion check
    id_to_del_term = pd.DataFrame(terminals['terminal_id'])
    id_to_del_term['source_name'] = 'terminals'
    id_to_del_term = id_to_del_term[['source_name','terminal_id']]

    curs.executemany('''INSERT INTO ITDE1.WEXZ_STG_DEL(source_name,ID)
        VALUES (?,?)''',
        id_to_del_term.values.tolist())

    #catture increment
    curs.execute('''SELECT      terminal_id,
            terminal_type,
            terminal_city,
            terminal_address
    FROM        ITDE1.WEXZ_DWH_DIM_TERMINALS_HIST
    WHERE       effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )''')

    result = curs.fetchall()
    cur_term_dwh = pd.DataFrame(result,columns = terminals.columns.tolist())
	
    increment = terminals.loc[pd.merge(terminals,cur_term_dwh, on=terminals.columns.tolist(), how='left', indicator=True)['_merge'] == 'left_only']
    increment['day_of_load'] = day_data[2][10:-5]
    curs.executemany('''INSERT INTO ITDE1.WEXZ_STG_TERMINALS(TERMINAL_ID,TERMINAL_TYPE,TERMINAL_CITY,
	                                TERMINAL_ADDRESS,DAY_OF_LOAD)
        VALUES (?,?,?,?,?)''',
        increment.values.tolist())

    del terminals
    del increment
    del cur_term_dwh

    for command in sqlCommands[num_stg:]:
        curs.execute(command)
    
    update_s = '''UPDATE ITDE1.WEXZ_META_LOAD SET LAST_LOAD = to_date('{}','YYYY-MM-DD HH24:MI:SS') '''.format(date_of_upload.strftime("%Y-%m-%d %H:%M:%S"))

    curs.execute(update_s)

	
    conn.commit()
    curs.close()
    conn.close()


