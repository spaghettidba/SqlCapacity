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

			return result;
		}


		public static void SaveToDatabase(SqlConnection conn, string TableName, IEnumerable<PerformanceCounterSnapshot> data)
		{
            using (SqlTransaction tran = conn.BeginTransaction())
            {

                try
                {

                    var Intervals = (
                        from t in data
                        group t by new
                        {
                            server_name = t.Interval.Server.Name
                        }
                        into grp
                        select new
                        {
                            grp.Key.server_name,
                            Interval.CreateNew(conn, grp.Key.server_name, tran).Id
                        }).ToList(); 

                    

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
                                counter_name = t.Counter.Name,
                                counter_instance = t.Counter.Instance
                            }
                            into grp
                            select new
                            {
                                interval_id = Intervals.First(i => i.server_name == grp.Key.server_name).Id,
                                grp.Key.counter_name,
                                grp.Key.counter_instance,
                                min_counter_value = grp.Min(t => t.Value),
                                max_counter_value = grp.Max(t => t.Value),
                                avg_counter_value = grp.Average(t => t.Value)
                            }).ToList();

                        using (var dt = DataUtils.ToDataTable(Table))
                        {
                            bulkCopy.WriteToServer(dt);
                        }
                    }

                    tran.Commit();

                }
                catch(Exception e)
                {
                    tran.Rollback();
                    logger.Error(e);
                    throw;
                }
            }
		}

		public object Clone()
		{
			return this.MemberwiseClone();
		}
	}
}
