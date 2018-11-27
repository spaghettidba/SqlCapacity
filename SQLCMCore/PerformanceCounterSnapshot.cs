using SQLCMCore.Util;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLCMCore
{
	public class PerformanceCounterSnapshot
	{
		public CounterDefinition Counter { get; set; }
		public long Value { get; set; }
		public bool Rebased { get; set; }
		public Interval Interval { get; set; }


		public static List<PerformanceCounterSnapshot> LoadLast(SqlConnection conn, string ServerName)
		{
			List<PerformanceCounterSnapshot> result = new List<PerformanceCounterSnapshot>();

			string sql = @"
				SELECT [interval_id]
					,[counter_name]
					,[counter_instance]
					,[max_counter_value]
					,[end_time]
					,[duration_minutes]
					,[server_name]
					,[server_id]
				FROM [PerformanceCounters] AS pc
				CROSS APPLY (
					SELECT TOP(1) i.end_time, i.duration_minutes, s.server_name, s.server_id
					FROM Intervals AS i
					INNER JOIN Servers AS s
						ON i.server_id = s.server_id 
					WHERE server_name = @target
						AND pc.interval_id = i.interval_id
					ORDER BY interval_id DESC
				) AS last_interval
				";

			using (SqlCommand cmd = conn.CreateCommand())
			{
				cmd.CommandText = sql;
				using (SqlDataReader rdr = cmd.ExecuteReader())
				{
					while (rdr.Read())
					{
						PerformanceCounterSnapshot snap = new PerformanceCounterSnapshot()
						{
							Counter = new CounterDefinition()
							{
								Name = rdr.GetString(rdr.GetOrdinal("counter_name")),
								Instance = rdr.GetString(rdr.GetOrdinal("counter_instance"))
							},
							Value = rdr.GetInt64(rdr.GetOrdinal("max_counter_value")),
							Interval = new Interval()
							{
								Id = rdr.GetInt32(rdr.GetOrdinal("interval_id")),
								EndDate = rdr.GetDateTime(rdr.GetOrdinal("end_time")),
								DurationMinutes = rdr.GetInt32(rdr.GetOrdinal("duration_minutes")),
								Server = new Server()
								{
									Name = rdr.GetString(rdr.GetOrdinal("server_name")),
									Id = rdr.GetInt32(rdr.GetOrdinal("server_id"))
								}
							}
						};
						result.Add(snap);
					}
				}

			}

			return result;
		}

		public static List<PerformanceCounterSnapshot> Take(SqlConnection targetConnection, List<CounterDefinition> counters)
		{
			string sql = @"
					SELECT RTRIM(counter_name) AS counter_name,
						RTRIM(instance_name) AS instance_name,
						cntr_value,
						CAST(cumulative AS bit) AS cumulative
					FROM sys.dm_os_performance_counters AS p
					INNER JOIN(
						VALUES {0}
					) AS v(name, instance, cumulative)
						ON p.counter_name = v.name
						AND p.instance_name = v.instance
					ORDER BY 1,2,3 ";


			string counterDefinitions = String.Join(",\r\n",
				from t in counters
				select new
				{
					SQLLine = $"('{t.Name}', '{t.Instance}', {(t.Cumulative?1:0)} )"
				}.SQLLine);

			sql = String.Format(sql, counterDefinitions);

			List <PerformanceCounterSnapshot> result = new List<PerformanceCounterSnapshot>();

			DateTime intervalEnd = DateTime.Now;

			using (SqlCommand cmd = targetConnection.CreateCommand())
			{
				cmd.CommandText = sql;
				var reader = cmd.ExecuteReader();
				while (reader.Read())
				{
					result.Add(
						new PerformanceCounterSnapshot()
						{
							Counter = new CounterDefinition()
							{
								Name = reader.GetString(reader.GetOrdinal("counter_name")),
								Instance = reader.GetString(reader.GetOrdinal("instance_name")),
								Cumulative = reader.GetBoolean(reader.GetOrdinal("cumulative"))
							},
							Value = reader.GetInt64(reader.GetOrdinal("cntr_value")),
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

			return result;
		}


		public static void SaveToDatabase(SqlConnection conn, string TableName, IEnumerable<PerformanceCounterSnapshot> data)
		{

			using (SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
														   SqlBulkCopyOptions.KeepIdentity |
														   SqlBulkCopyOptions.FireTriggers |
														   SqlBulkCopyOptions.CheckConstraints |
														   SqlBulkCopyOptions.TableLock, 
														   null))
			{

				bulkCopy.DestinationTableName = TableName;
				bulkCopy.BatchSize = 1000;
				bulkCopy.BulkCopyTimeout = 300;

				var Table = from t in data
							group t by new
							{
								interval_id = t.Interval.Id,
								counter_name = t.Counter.Name,
								counter_instance = t.Counter.Instance
							}
							into grp
							select new
							{
								grp.Key.interval_id,
								grp.Key.counter_name,
								grp.Key.counter_instance,

								min_counter_value = grp.Min(t => t.Value),
								max_counter_value = grp.Max(t => t.Value),
								avg_counter_value = grp.Average(t => t.Value)
							};

				using (var dt = DataUtils.ToDataTable(Table))
				{
					bulkCopy.WriteToServer(dt);
				}
			}
		}
	}
}
