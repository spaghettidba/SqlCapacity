CREATE TABLE [dbo].[CounterDefinitions]
(
	[counter_name] nvarchar(255) NOT NULL,
	[instance_name] nvarchar(255) NOT NULL,
	[cumulative] bit NULL,
	CONSTRAINT PK_CounterDefinitions PRIMARY KEY(counter_name, instance_name)
)
