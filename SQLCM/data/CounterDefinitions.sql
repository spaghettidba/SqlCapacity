MERGE INTO CounterDefinitions AS dest
USING (
	SELECT * 
	FROM (
		VALUES 
			 (01,'Page life expectancy','',0)
			,(02,'Logins/sec','',1)
			,(03,'User Connections','',0)
			,(04,'Transactions','',0)
			,(05,'Page Splits/sec','',1)
			,(06,'Transactions/sec','_Total',1)
			,(07,'Memory Grants Pending','',0)
			,(08,'Batch Requests/sec','',1)
			,(09,'Cpu Usage %','',0)
			,(10,'Other Cpu Usage %','',0)
	) AS v(id, name, instance, cumulative)
) AS src 
	ON dest.counter_name = src.name 
    AND dest.instance_name = src.instance
WHEN MATCHED THEN UPDATE 
	SET cumulative = src.cumulative,
	id = src.id
WHEN NOT MATCHED BY TARGET THEN INSERT(counter_id, counter_name, instance_name, cumulative) 
	VALUES(src.id, src.name, src.instance, src.cumulative)
WHEN NOT MATCHED BY SOURCE THEN DELETE;