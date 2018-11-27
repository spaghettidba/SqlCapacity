CREATE TABLE [dbo].[PerformanceCounters](
	[interval_id] [int] NOT NULL CONSTRAINT FK_PerformanceCounters_Intervals FOREIGN KEY REFERENCES Intervals(interval_id),
    [counter_name] [varchar](255) NOT NULL,
	[counter_instance] [varchar](255) NOT NULL,
    [min_counter_value] [float] NOT NULL,
    [max_counter_value] [float] NOT NULL,
    [avg_counter_value] [float] NOT NULL, 
    CONSTRAINT [PK_PerformanceCounters] PRIMARY KEY ([counter_name], [interval_id])
)