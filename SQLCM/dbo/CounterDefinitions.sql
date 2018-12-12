CREATE TABLE [dbo].[CounterDefinitions]
(
	[counter_id] int NOT NULL CONSTRAINT PK_CounterDefinitions PRIMARY KEY CLUSTERED,
	[counter_name] nvarchar(255) NOT NULL,
	[instance_name] nvarchar(255) NOT NULL,
	[cumulative] bit NULL,
	CONSTRAINT UQ_CounterDefinitions UNIQUE(counter_name, instance_name)
)
