--Estadísticas desglosadas a nivel de query (si procede)
--FUNCIONARÁ EN SQL 2005 Y 2008
SELECT  TOP 100
        CASE WHEN deqs.statement_start_offset = 0 AND deqs.statement_end_offset = -1
             THEN '-- text  -->' +  CHAR(13) + CHAR(10) + dest.text
             ELSE '-- query -->' + CHAR(13) + CHAR(10)
                  + SUBSTRING(dest.text, deqs.statement_start_offset / 2,
                              ( ( CASE WHEN deqs.statement_end_offset = -1
                                       THEN DATALENGTH(dest.text)
                                       ELSE deqs.statement_end_offset
                                  END ) - deqs.statement_start_offset ) / 2)
        END AS Texto,
        db_name(deqp.dbid) as BaseDeDatos,										--2
        deqp.objectid as Objeto,												--3
        execution_count as Ocurrencias,											--4
        total_worker_time / 1000 as CPU_Total_ms,								--5
        total_worker_time / execution_count / 1000 AS CPU_Promedio_ms,			--6
        total_physical_reads as Lecturas_Fisicas_Total,							--7
        total_physical_reads / execution_count AS Lecturas_Fisicas_Promedio,	--8
        total_logical_reads as Lecturas_Logicas_Total,							--9
        total_logical_reads / execution_count AS Lecturas_Logicas_Promedio,		--10
        total_logical_writes as Escrituras_Logicas_Total,						--11
        total_logical_writes / execution_count AS Escrituras_Logicas_Promedio,	--12
        total_elapsed_time / 1000 as Tiempo_Total_ms,							--13
        total_elapsed_time / execution_count / 1000 AS Tiempo_Promedio_ms,		--14
        last_execution_time,													--15
        deqs.plan_handle, 
        deqs.sql_handle, 
        deqs.statement_start_offset,
        deqs.statement_end_offset,
        deqs.creation_time, 
        deqs.plan_generation_num
        --deqs.query_hash, 
        --deqs.query_plan_hash
FROM    sys.dm_exec_query_stats deqs
        CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) AS dest
        CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) deqp
ORDER BY 9 DESC;

--La siguiente query me devuelve las estadísticas agrupando por sql_handle. 
--Recordar que una misma sql con ligeras variaciones en párametros
--Esto pretende agrupar para todas las subconsultas del mismo sql, pero ojo, 
--puede ser que el mismo sql tenga diferente plan (por recompilación). No pasa
--nada, los agregados que busco me valen, excepto NumeroDeSubConsultas, que no 
--sería real. 
--FUNCIONARÁ EN SQL 2005 Y 2008
SELECT TOP  1000
        MAX(dest.text) AS Texto,
        COUNT(*) AS NumeroDeSubConsultas,										--2
        MAX(execution_count) AS Ocurrencias_Total,								--3
        SUM(total_worker_time) / 1000 AS CPU_Total_ms,							--4
        SUM(total_physical_reads) AS Lecturas_Fisicas_Total,					--5
        SUM(total_logical_reads) AS Lecturas_Logicas_Total,						--6
        SUM(total_logical_writes) AS Escrituras_Logicas_Total,					--7
        SUM(total_elapsed_time) / 1000 AS Tiempo_Total_ms						--8
FROM    sys.dm_exec_query_stats deqs
        CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
GROUP BY deqs.sql_handle
ORDER BY 3 DESC

--Esta query devuelve las estadísticas agrupando por query_hash. 
--Esto me garantiza que el mismo batch u objeto, aunque se haya 
--recompilado varias veces o cambien los valores de algunos parámetros
--se agrupe como uno sólo. 
--LA PUÑETA ES QUE SÓLO ME FUNCIONARÁ EN SQL 2008
SELECT TOP  1000
        MAX(CASE WHEN deqs.statement_start_offset = 0 AND deqs.statement_end_offset = -1
             THEN '-- text  -->' +  CHAR(13) + CHAR(10) + dest.text
             ELSE '-- query -->' + CHAR(13) + CHAR(10)
                  + SUBSTRING(dest.text, deqs.statement_start_offset / 2,
                              ( ( CASE WHEN deqs.statement_end_offset = -1
                                       THEN DATALENGTH(dest.text)
                                       ELSE deqs.statement_end_offset
                                  END ) - deqs.statement_start_offset ) / 2)
        END) AS Texto,
        MAX(execution_count) AS Ocurrencias_Total,								--2
        SUM(total_worker_time) / 1000 AS CPU_Total_ms,							--3
        SUM(total_physical_reads) AS Lecturas_Fisicas_Total,					--4
        SUM(total_logical_reads) AS Lecturas_Logicas_Total,						--5
        SUM(total_logical_writes) AS Escrituras_Logicas_Total,					--6
        SUM(total_elapsed_time) / 1000 AS Tiempo_Total_ms						--7
FROM    sys.dm_exec_query_stats deqs
        CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
GROUP BY deqs.query_hash
ORDER BY 5 DESC

select * from sys.dm_exec_query_stats