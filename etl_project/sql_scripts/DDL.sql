-- DWH tables   
CREATE TABLE ITDE1.WEXZ_DWH_DIM_ACCOUNTS_HIST(	
		account_num VARCHAR2(20),
		valid_to DATE,
		client VARCHAR2(20),
		effective_from DATE,
		effective_to DATE,
		deleted_flg CHAR( 1 ) 
		);
		
CREATE TABLE ITDE1.WEXZ_DWH_DIM_CARDS_HIST(	
		card_num VARCHAR2(20),
		account_num VARCHAR2(20),
		effective_from DATE,
		effective_to DATE,
		deleted_flg CHAR( 1 ) 
		);
		
CREATE TABLE ITDE1.WEXZ_DWH_DIM_CLIENTS_HIST(	
		client_id VARCHAR2(20),
		last_name VARCHAR2(100),
		first_name VARCHAR2(100),
		patronymic VARCHAR2(100),
		date_of_birth DATE,
		passport_num VARCHAR2(15),
		passport_valid_to DATE,
		phone VARCHAR2(20),
		effective_from DATE,
		effective_to DATE,
		deleted_flg CHAR( 1 ) 
		);


CREATE TABLE ITDE1.WEXZ_DWH_FACT_TRANSACTIONS(	
		trans_id VARCHAR2(20),
		trans_date DATE,
		card_num VARCHAR2(20),
		oper_type VARCHAR2(20),
		amt DECIMAL,
		oper_result VARCHAR2(20),
		terminal VARCHAR2(20)
		); 
		
		
CREATE TABLE ITDE1.WEXZ_DWH_FACT_PSSPRT_BLCKLST(	
		passport_num VARCHAR2(15),
		entry_dt DATE
		); 
		
CREATE TABLE ITDE1.WEXZ_DWH_DIM_TERMINALS_HIST(	
		terminal_id VARCHAR2(10),
		terminal_type VARCHAR2(10),
		terminal_city VARCHAR2(100),
		terminal_address VARCHAR2(200),
		effective_from DATE,
		effective_to DATE,
		deleted_flg CHAR( 1 ) 
		);
		
-- META		
drop table ITDE1.WEXZ_META_DATE;
create table ITDE1.WEXZ_META_DATE(
	dbname varchar2(30),
	tablename varchar2(30),
	last_update date
	);

insert into ITDE1.WEXZ_META_DATE( dbname, tablename, last_update ) values ( 'ITDE1', 'WEXZ_DWH_DIM_ACCOUNTS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );
insert into ITDE1.WEXZ_META_DATE( dbname, tablename, last_update ) values ( 'ITDE1', 'WEXZ_DWH_DIM_CARDS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );
insert into ITDE1.WEXZ_META_DATE( dbname, tablename, last_update ) values ( 'ITDE1', 'WEXZ_DWH_DIM_CLIENTS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );
insert into ITDE1.WEXZ_META_DATE( dbname, tablename, last_update ) values ( 'ITDE1', 'WEXZ_DWH_FACT_PSSPRT_BLCKLST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );
commit;

drop table ITDE1.WEXZ_META_LOAD;
create table ITDE1.WEXZ_META_LOAD(
	LAST_LOAD date
	);
insert into ITDE1.WEXZ_META_LOAD( LAST_LOAD ) values (  to_date( '2021-2-28', 'YYYY-MM-DD' ) );
commit;

-- STAGING TABLES

drop table ITDE1.WEXZ_STG_DEL;
create table ITDE1.WEXZ_STG_DEL(
	dbname varchar2(30),
	tablename varchar2(30),
	id varchar2(30)
	);
	
-------------------------------------------------
CREATE TABLE ITDE1.WEXZ_STG_ACCOUNTS(	
		ACCOUNT VARCHAR2(20),
		VALID_TO DATE,
		CLIENT VARCHAR2(20),
		CREATE_DT DATE,
		UPDATE_DT DATE
		);
		
		


CREATE TABLE ITDE1.WEXZ_STG_CARDS(	
		CARD_NUM VARCHAR2(20),
		ACCOUNT VARCHAR2(20),
		CREATE_DT DATE,
		UPDATE_DT DATE
		);


CREATE TABLE ITDE1.WEXZ_STG_CLIENTS(
		CLIENT_ID VARCHAR2(20),
		LAST_NAME VARCHAR2(100),
		FIRST_NAME VARCHAR2(100),
		PATRONYMIC VARCHAR2(100),
		DATE_OF_BIRTH DATE,
		PASSPORT_NUM VARCHAR2(15),
		PASSPORT_VALID_TO DATE,
		PHONE VARCHAR2(20),
		CREATE_DT DATE,
		UPDATE_DT DATE
);	



CREATE TABLE ITDE1.WEXZ_STG_TRANSACTIONS(	
		trans_id VARCHAR2(20),
		trans_date VARCHAR2(20),
		card_num VARCHAR2(20),
		oper_type VARCHAR2(20),
		amt VARCHAR2(20),
		oper_result VARCHAR2(20),
		terminal VARCHAR2(20)
		); 
		
		
CREATE TABLE ITDE1.WEXZ_STG_PSSPRT_BLCKLST(	
		passport_num VARCHAR2(15),
		entry_dt VARCHAR2(20)
		); 
		
CREATE TABLE ITDE1.WEXZ_STG_TERMINALS(	
		terminal_id VARCHAR2(10),
		terminal_type VARCHAR2(10),
		terminal_city VARCHAR2(100),
		terminal_address VARCHAR2(200),
		DAY_OF_LOAD VARCHAR2(10)
		);
