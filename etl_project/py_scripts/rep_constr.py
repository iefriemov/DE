import jaydebeapi

def constract_report(path_sql):
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


    for command in sqlCommands:
        curs.execute(command)

    conn.commit()
    curs.close()
    conn.close()
