CREATE TABLE [dbo].[Intervals] (
	[interval_id] [int] NOT NULL CONSTRAINT PK_Intervals PRIMARY KEY CLUSTERED,
	[server_id] [int] NOT NULL,
	[end_time] [datetime] NOT NULL,
	[duration_minutes] [int] NOT NULL
)