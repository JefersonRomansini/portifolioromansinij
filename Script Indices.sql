
/*=========================================*/
/* CONTROLE DE INDICES DE TABELAS DO BANCO */
/*=========================================*/


--DETALHAMENTO DOS INDICES DAS TABELAS
SELECT * FROM sys.dm_db_index_physical_stats  
    (DB_ID(N'GRUPOTEL'), OBJECT_ID(N'VIAGENS_FRETAM'), NULL, NULL , 'DETAILED');  

--DETALHAMENTO DOS INDICES COM ESPECIFICAÇÃO DO NOME DE CADA INDICE:
SELECT 
	a.index_id, 
	name AS 'NOME_INDICE', 
	avg_fragmentation_in_percent,
	page_count  
FROM 
	sys.dm_db_index_physical_stats (DB_ID(N'GRUPOTEL'), OBJECT_ID(N'VIAGENS_FRETAM'), NULL, NULL, NULL) AS a  
    JOIN sys.indexes AS b ON a.object_id = b.object_id AND a.index_id = b.index_id;   
GO  

--Reorganizando um determinado indice de uma tabela.   

ALTER INDEX PK_VIAGENS_FRETAM ON VIAGENS_FRETAM  
REORGANIZE ;   
GO

-- Recriando um determinado indice da tabela
ALTER INDEX PK_VIAGENS_FRETAM ON VIAGENS_FRETAM
REBUILD;
GO

-- Reorganizando todos os indices da tabela  
ALTER INDEX ALL ON VIAGENS_FRETAM  
REORGANIZE ;   
GO 

-- Recriando todos os indices da tabela
ALTER INDEX ALL ON VIAGENS_FRETAM
REBUILD WITH (FILLFACTOR = 80, SORT_IN_TEMPDB = ON,
              STATISTICS_NORECOMPUTE = ON);
GO


-- Saber todas as tabelas que precisam de REBUILD de indices (acima de 30% de fragmentação)
SELECT 
	a.index_id, 
	c.name AS 'NOME TABELA',
	b.name AS 'NOME_INDICE', 
	avg_fragmentation_in_percent,
	page_count  
FROM 
	sys.dm_db_index_physical_stats (DB_ID(N'GRUPOTEL'), OBJECT_ID(N'*'), NULL, NULL, NULL) AS a  
    JOIN sys.indexes AS b ON a.object_id = b.object_id AND a.index_id = b.index_id  
	JOIN SYS.objects AS c ON a.object_id = c.object_id
WHERE avg_fragmentation_in_percent > 30
ORDER BY avg_fragmentation_in_percent DESC
GO 


-- Descobrir todos os índices que estão faltando.


	SELECT  
			sys.objects.name
		, 	(avg_total_user_cost * avg_user_impact) * (user_seeks + user_scans) AS Impact
		,  		'CREATE NONCLUSTERED INDEX ix_IndexName ON ' 
				+ sys.objects.name COLLATE DATABASE_DEFAULT 
				+ ' ( ' 
				+ IsNull(mid.equality_columns, '') 
				+ CASE 
					WHEN mid.inequality_columns IS NULL    THEN '' 
					ELSE 
					  CASE 
						WHEN mid.equality_columns IS NULL  THEN '' 
						ELSE ',' END + mid.inequality_columns END + ' ) ' 
				+ CASE 
					WHEN mid.included_columns IS NULL      THEN '' 
					ELSE 'INCLUDE (' + mid.included_columns + ')' END + ';' 
				AS CreateIndexStatement
		, 	mid.equality_columns
		, 	mid.inequality_columns
		, 	mid.included_columns 
	FROM 
		sys.dm_db_missing_index_group_stats AS migs 
			INNER JOIN sys.dm_db_missing_index_groups AS mig 
				ON migs.group_handle = mig.index_group_handle 
			INNER JOIN sys.dm_db_missing_index_details AS mid 
				ON 	mig.index_handle = mid.index_handle 
					AND mid.database_id = DB_ID() 
			INNER JOIN sys.objects WITH (nolock) 
				ON mid.OBJECT_ID = sys.objects.OBJECT_ID 
	WHERE     
			(migs.group_handle IN	( 
										SELECT     
											TOP (500) group_handle 
										FROM          
											sys.dm_db_missing_index_group_stats WITH (nolock) 
										ORDER BY 
											(avg_total_user_cost * avg_user_impact) * (user_seeks + user_scans) DESC
									)
			)  
		AND OBJECTPROPERTY(sys.objects.OBJECT_ID, 'isusertable')=1 	
	ORDER BY 2 DESC , 3 DESC
	
	
	
-- Índice não utilizado. 

SELECT 
		o.name
	, indexname=i.name
	, i.index_id 
	, reads=user_seeks + user_scans + user_lookups 
	, writes = user_updates 
	, rows = (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.index_id = s.index_id AND s.object_id = p.object_id)
	, CASE
			WHEN s.user_updates < 1 THEN 100
			ELSE 1.00 * (s.user_seeks + s.user_scans + s.user_lookups) / s.user_updates
			END 
		 AS reads_per_write
	, 'DROP INDEX ' + QUOTENAME(i.name) 
	+ ' ON ' + QUOTENAME(c.name) + '.' + QUOTENAME(OBJECT_NAME(s.object_id)) as 'drop statement'
FROM 
	sys.dm_db_index_usage_stats s 
	INNER JOIN sys.indexes i 
		ON 	i.index_id = s.index_id 
			AND s.object_id = i.object_id 
	INNER JOIN sys.objects o 
		on s.object_id = o.object_id
	INNER JOIN sys.schemas c 
		on o.schema_id = c.schema_id
WHERE 
		OBJECTPROPERTY(s.object_id,'IsUserTable') = 1
	AND s.database_id = DB_ID() 
	AND i.type_desc = 'nonclustered'
	AND i.is_primary_key = 0
	AND i.is_unique_constraint = 0
	AND (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.index_id = s.index_id AND s.object_id = p.object_id) > 10000
ORDER BY reads