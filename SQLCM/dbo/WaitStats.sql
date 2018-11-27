CREATE TABLE [dbo].[WaitStats](
	[interval_id] [int] NOT NULL CONSTRAINT FK_WaitStats_Intervals FOREIGN KEY REFERENCES Intervals(interval_id),
    [wait_type] [varchar](255) NOT NULL,
    [wait_sec] [float] NOT NULL,
    [resource_sec] [float] NOT NULL,
    [signal_sec] [float] NOT NULL,
    [wait_count] [bigint] NOT NULL,
	CONSTRAINT PK_WaitStats PRIMARY KEY CLUSTERED(interval_id, wait_type)
)
