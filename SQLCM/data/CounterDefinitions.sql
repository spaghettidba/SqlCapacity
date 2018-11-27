MERGE INTO CounterDefinitions AS dest
USING (
	SELECT * 
	FROM (
		VALUES 
			 ('Page life expectancy','',0)
			,('Logins/sec','',1)
			,('User Connections','',0)
			,('Transactions','',0)
			,('Page Splits/sec','',1)
			,('Transactions/sec','_Total',1)
			,('Memory Grants Pending','',0)
			,('Batch Requests/sec','',1)
	) AS v(name, instance, cumulative)
) AS src 
	ON dest.counter_name = src.name 
    AND dest.instance_name = src.instance
WHEN MATCHED THEN UPDATE 
	SET cumulative = src.cumulative
WHEN NOT MATCHED BY TARGET THEN INSERT(counter_name, instance_name, cumulative) 
	VALUES(src.name, src.instance, src.cumulative)
WHEN NOT MATCHED BY SOURCE THEN DELETE;