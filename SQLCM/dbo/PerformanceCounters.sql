CREATE TABLE [dbo].[PerformanceCounters](
	[interval_id] [int] NOT NULL CONSTRAINT FK_PerformanceCounters_Intervals FOREIGN KEY REFERENCES Intervals(interval_id),
    [counter_id] [int] NOT NULL CONSTRAINT FK_PerformanceCounters_Counters FOREIGN KEY REFERENCES CounterDefinitions(counter_id),
    [min_counter_value] [float] NOT NULL,
    [max_counter_value] [float] NOT NULL,
    [avg_counter_value] [float] NOT NULL, 
    CONSTRAINT [PK_PerformanceCounters] PRIMARY KEY ([interval_id], [counter_id])
)