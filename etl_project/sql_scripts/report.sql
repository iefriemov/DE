-- first case

insert into itde1.WEXZ_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
(select	distinct	trans_date, 
			client.passport_num,
			CONCAT(CONCAT(last_name,CONCAT(' ',first_name)),CONCAT(' ',patronymic)) fio,
			phone,
			1 event_type,
 			(select max(to_date( DAY_OF_LOAD, 'DDMMYYYY' )) from itde1.WEXZ_stg_terminals) rep_dt
from itde1.WEXZ_dwh_fact_transactions trans
left join itde1.WEXZ_dwh_dim_cards_hist cards
	on trans.card_num = trim(cards.card_num) and cards.deleted_flg = 'N'
	and cards.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
left join itde1.WEXZ_dwh_dim_accounts_hist acc
	on cards.account_num = acc.account_num and acc.deleted_flg = 'N'
	and acc.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
left join itde1.WEXZ_dwh_dim_clients_hist client
	on acc.client = client.client_id and client.deleted_flg = 'N'
	and client.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
left join itde1.WEXZ_dwh_fact_pssprt_blcklst pas
    on client.passport_num = pas.passport_num
where trunc(trans_date) = (select max(to_date(DAY_OF_LOAD, 'DDMMYYYY')) from itde1.WEXZ_stg_terminals)
and trans_date > passport_valid_to or entry_dt is not null);

-- second case

insert into itde1.WEXZ_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select distinct		trans_date, 
			client.passport_num,
			CONCAT(CONCAT(last_name,CONCAT(' ',first_name)),CONCAT(' ',patronymic)) fio,
			phone, 
			2 event_type,
 			(select max(to_date(DAY_OF_LOAD, 'DDMMYYYY')) from itde1.WEXZ_stg_terminals) rep_dt
from itde1.WEXZ_dwh_fact_transactions trans
left join itde1.WEXZ_dwh_dim_cards_hist cards
	on trans.card_num = trim(cards.card_num) and cards.deleted_flg = 'N'
	and cards.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
left join itde1.WEXZ_dwh_dim_accounts_hist acc
	on cards.account_num = acc.account_num and acc.deleted_flg = 'N'
	and acc.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
left join itde1.WEXZ_dwh_dim_clients_hist client
    on acc.client = client.client_id and client.deleted_flg = 'N'
	and client.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
where trunc(trans_date) = (select max(to_date( DAY_OF_LOAD, 'DDMMYYYY' )) from itde1.WEXZ_stg_terminals)
and trans_date > valid_to;

-- third case


insert into itde1.WEXZ_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select	trans_date, 
	passport_num, 
	fio, 
	phone, 
	3 event_type, 
	rdt 
from	(select trans_date, 
		passport_num, 
		card_num, 
		fio, 
		phone, 
		rdt,
		row_number() over (partition by card_num order by trans_date) rn
	from 	(select	* 
		from	(select distinct trans_date,
				client.passport_num, 
				tra.card_num,
				CONCAT(CONCAT(last_name,CONCAT(' ',first_name)),CONCAT(' ',patronymic)) fio,
				phone,
				(select max(to_date( DAY_OF_LOAD, 'DDMMYYYY' )) from itde1.WEXZ_stg_terminals) rdt,
				lead(trans_date, 1, to_date('2999-12-31', 'YYYY-MM-DD')) over (partition by tra.card_num order by trans_date) - trans_date passed,
				lead(terminal_city, 1, terminal_city) over (partition by tra.card_num order by trans_date) c_city,
				terminal_city
			from	itde1.WEXZ_dwh_fact_transactions tra
			left join itde1.WEXZ_dwh_dim_terminals_hist ter
				on tra.terminal = ter.terminal_id and ter.deleted_flg = 'N'
				and ter.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
			left join itde1.WEXZ_dwh_dim_cards_hist cards
				on tra.card_num = trim(cards.card_num) and cards.deleted_flg = 'N'
				and cards.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
			left join itde1.WEXZ_dwh_dim_accounts_hist acc
				on cards.account_num = acc.account_num and acc.deleted_flg = 'N'
				and acc.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
			left join itde1.WEXZ_dwh_dim_clients_hist client
				on acc.client = client.client_id and client.deleted_flg = 'N'
				and client.effective_to = to_date('2999-12-31', 'YYYY-MM-DD'))
				where passed <= 1/24 and terminal_city <> c_city
				and trunc(trans_date) = (select max(to_date(DAY_OF_LOAD, 'DDMMYYYY')) 
			from itde1.WEXZ_stg_terminals)))
where rn = 2;

-- case 4

insert into itde1.WEXZ_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
select	tdt, 
	passport_num, 
	fio, 
	phone, 
        4 event_type,
	rep_dt 
from 	(select distinct	trans_date,
				client.passport_num,
				trans.card_num,
				phone,
				CONCAT(CONCAT(last_name,CONCAT(' ',first_name)),CONCAT(' ',patronymic)) fio,
				(select max(to_date( DAY_OF_LOAD, 'DDMMYYYY' )) from itde1.WEXZ_stg_terminals) rep_dt,
				amt, 
				oper_result res,
				lead(oper_result, 1, null) over (partition by trans.card_num order by trans_date) res1,
				lead(oper_result, 2, null) over (partition by trans.card_num order by trans_date) res2,
				lead(oper_result, 3, null) over (partition by trans.card_num order by trans_date) res3,
				lead(amt, 1, null) over (partition by trans.card_num order by trans_date) amt1,
				lead(amt, 2, null) over (partition by trans.card_num order by trans_date) amt2,
				lead(amt, 3, null) over (partition by trans.card_num order by trans_date) amt3,
				lead(trans_date, 3, to_date('2999-12-31', 'YYYY-MM-DD')) over (partition by trans.card_num order by trans_date) - trans_date passed,
				lead(trans_date, 3, to_date('2999-12-31', 'YYYY-MM-DD')) over (partition by trans.card_num order by trans_date) tdt
    	from itde1.WEXZ_dwh_fact_transactions trans
    	left join itde1.WEXZ_dwh_dim_cards_hist cards
		on trans.card_num = trim(cards.card_num) and cards.deleted_flg = 'N'
        	and cards.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
    	left join itde1.WEXZ_dwh_dim_accounts_hist acc
        	on cards.account_num = acc.account_num and acc.deleted_flg = 'N'
        	and acc.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
    	left join itde1.WEXZ_dwh_dim_clients_hist client
        	on acc.client = client.client_id and client.deleted_flg = 'N'
    		and client.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
)
where passed <= (20/(24*60))
and amt > amt1 and amt1 > amt2 and amt2 > amt3
and res = 'REJECT' and res1 = 'REJECT' and res2 = 'REJECT' and res3 = 'SUCCESS'
and trunc(trans_date) = (select max(to_date(DAY_OF_LOAD, 'DDMMYYYY')) from itde1.WEXZ_stg_terminals);
