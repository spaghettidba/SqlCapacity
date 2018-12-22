CREATE TABLE [dbo].[WaitStats](
	[interval_id] [int] NOT NULL CONSTRAINT FK_WaitStats_Intervals FOREIGN KEY REFERENCES Intervals(interval_id),
    [wait_id] INT NOT NULL CONSTRAINT FK_WaitStats_WaitDefinitions FOREIGN KEY REFERENCES WaitDefinitions(type_id),
    [wait_sec] [float] NOT NULL,
    [resource_sec] [float] NOT NULL,
    [signal_sec] [float] NOT NULL,
    [wait_count] [bigint] NOT NULL,
	CONSTRAINT PK_WaitStats PRIMARY KEY CLUSTERED(interval_id, [wait_id])
)
