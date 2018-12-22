using NLog;
using SQLCMCore.Util;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLCMCore
{
	public class PerformanceCounterSnapshot : ICloneable
	{
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private double _value;

		public CounterDefinition Counter { get; set; }
		public long RawValue { get; set; }
		public double Value {
			get
			{
				return this.Counter.Cumulative ? _value : RawValue;
			}
			set
			{
				if(this.Counter.Cumulative)
				{
					_value = value;
				}
				else
				{
					RawValue = (long)value;
				}
			}
		}
		public bool Rebased { get; set; }
		public Interval Interval { get; set; }

		

		public static List<PerformanceCounterSnapshot> Take(SqlConnection targetConnection, List<CounterDefinition> counters)
		{
			string sql = @"
                WITH ts_now(ts_now) AS (
                    SELECT cpu_ticks/(cpu_ticks/ms_ticks) FROM sys.dm_os_sys_info WITH (NOLOCK)
                ),
                CPU_Usage AS (
                    SELECT TOP(256) SQLProcessUtilization,
                                    OtherProcessUtilization =  100 - SQLProcessUtilization - SystemIdle,
                                    DATEADD(ms, -1 * (ts_now.ts_now - [timestamp]), GETDATE()) AS [Event_Time] 
                    FROM (
                        SELECT record.value('(./Record/@id)[1]', 'int') AS record_id, 
                            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') 
                            AS [SystemIdle], 
                            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') 
                            AS [SQLProcessUtilization], [timestamp] 
                        FROM (
                            SELECT [timestamp], CONVERT(xml, record) AS [record] 
                            FROM sys.dm_os_ring_buffers WITH (NOLOCK)
                            WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
                                AND record LIKE N'%<SystemHealth>%'
                        ) AS x
                    ) AS y 
                    CROSS JOIN ts_now
                )
                SELECT p.*, v.Id, CAST(v.cumulative AS bit) AS cumulative
                FROM (
                    SELECT RTRIM(counter_name) AS counter_name,
	                    RTRIM(instance_name) AS instance_name,
	                    cntr_value
                    FROM sys.dm_os_performance_counters AS p

                    UNION ALL

                    SELECT 
                        CASE WHEN name = 'SQLProcessUtilization' THEN 'Cpu usage %' ELSE 'Other Cpu usage %' END,
                        '',
                        value
                    FROM CPU_Usage
                    UNPIVOT (value FOR name IN ([SQLProcessUtilization], [OtherProcessUtilization])) AS u
                    WHERE [Event_Time] >= DATEADD(minute, -1, GETDATE())
                ) AS p
                INNER JOIN(
	                VALUES {0}
                ) AS v(id, name, instance, cumulative)
	                ON p.counter_name = v.name
	                AND p.instance_name = v.instance
                ORDER BY 1,2,3
                OPTION (RECOMPILE);
            ";


			string counterDefinitions = String.Join(",\r\n",
				from t in counters
				select new
				{
					SQLLine = $"({t.Id}, '{t.Name}', '{t.Instance}', {(t.Cumulative?1:0)} )"
				}.SQLLine);

			sql = String.Format(sql, counterDefinitions);

			List <PerformanceCounterSnapshot> result = new List<PerformanceCounterSnapshot>();

			DateTime intervalEnd = DateTime.Now;

			using (SqlCommand cmd = targetConnection.CreateCommand())
			{
				cmd.CommandText = sql;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(
                            new PerformanceCounterSnapshot()
                            {
                                Counter = new CounterDefinition()
                                {
                                    Id = reader.GetInt32(reader.GetOrdinal("Id")),
                                    Name = reader.GetString(reader.GetOrdinal("counter_name")),
                                    Instance = reader.GetString(reader.GetOrdinal("instance_name")),
                                    Cumulative = reader.GetBoolean(reader.GetOrdinal("cumulative"))
                                },
                                Value = reader.GetInt64(reader.GetOrdinal("cntr_value")),
                                RawValue = reader.GetInt64(reader.GetOrdinal("cntr_value")),
                                Interval = new Interval()
                                {
                                    Server = new Server()
                                    {
                                        Name = targetConnection.DataSource
                                    },
                                    EndDate = intervalEnd
                                }
                            }
                        );

                    }
                }
			}

			return result;
		}


		public static void SaveToDatabase(SqlConnection conn, string TableName, IEnumerable<PerformanceCounterSnapshot> data, SqlTransaction tran, IEnumerable<Interval> intervals)
		{
            using (SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                                            SqlBulkCopyOptions.KeepIdentity |
                                                            SqlBulkCopyOptions.FireTriggers |
                                                            SqlBulkCopyOptions.CheckConstraints |
                                                            SqlBulkCopyOptions.TableLock,
                                                            tran))
            {
                bulkCopy.DestinationTableName = TableName;
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;

                var Table = (
                    from t in data
                    group t by new
                    {
                        server_name = t.Interval.Server.Name,
                        counter_id = t.Counter.Id
                    }
                    into grp
                    select new
                    {
                        interval_id = intervals.First(i => i.Server.Name == grp.Key.server_name).Id,
                        grp.Key.counter_id,
                        min_counter_value = grp.Min(t => t.Value),
                        max_counter_value = grp.Max(t => t.Value),
                        avg_counter_value = grp.Average(t => t.Value)
                    }).ToList();

                using (var dt = DataUtils.ToDataTable(Table))
                {
                    bulkCopy.WriteToServer(dt);
                }
            }

		}

		public object Clone()
		{
			return this.MemberwiseClone();
		}
	}
}
