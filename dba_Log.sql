use tempdb
go

exec sp_spaceused

--Vemos el espacio total y consumido actualmente
dbcc sqlperf(logspace)

--Estadística y estado de los VLFs. 
dbcc loginfo

--Posibles transacciones abiertas bloqueando la reducción del log.
dbcc opentran

--Transacciones activas
select * from sys.dm_tran_active_transactions
select * from sys.dm_tran_session_transactions
select * from sys.dm_tran_database_transactions
select * from sys.dm_tran_active_snapshot_database_transactions

--En los bloqueos se pueden ver los owner_id que tienen transacciones abiertas si hay alguno. 
select * from sys.dm_tran_locks
select * from sys.dm_exec_sessions
select * from sys.dm_exec_requests
