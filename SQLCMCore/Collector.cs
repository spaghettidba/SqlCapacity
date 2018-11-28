using NLog;
using SQLCMCore.Util;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SQLCMCore
{
    public class Collector
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private List<PerformanceCounterSnapshot> baseCounters = new List<PerformanceCounterSnapshot>();

		private List<PerformanceCounterSnapshot> counterData = new List<PerformanceCounterSnapshot>();
		private bool stopped;

		public ConnectionInfo ConnectionInfo { get; set; } = new ConnectionInfo();

		public string Path { get; set; }

		public string TargetTable { get; set; } = "dbo.PerformanceCounters";
		public int CollectionInterval { get; set; } = 15;
		public int UploadInterval { get; set; } = 60;

		public Collector(string server, string database)
		{
			ConnectionInfo.ServerName = server;
			ConnectionInfo.DatabaseName = database;
		}

		public void Run()
		{
			Task t = Task.Factory.StartNew(() => UploadMain());
			while (!stopped)
			{
                try
                {
                    Collect();
                    for (int i = 0; i < CollectionInterval; i++)
                    {
                        if (stopped)
                        {
                            break;
                        }
                        Thread.Sleep(1000);
                    }
                }
                catch (Exception e)
                {
                    logger.Error("Error occured during counter collection");
                    logger.Error(e);
                }
				
			}
		}

		private void UploadMain()
		{
			while (!stopped)
			{
                try
                {
                    Upload();
                    for (int i = 0; i < UploadInterval; i++)
                    {
                        if (stopped)
                        {
                            break;
                        }
                        Thread.Sleep(1000);
                    }
                }
                catch (Exception e)
                {
                    logger.Error(e);
                }
			}
		}

		public void Stop()
		{
			stopped = true;
		}

		private void Collect()
		{
			using (SqlConnection conn = new SqlConnection())
			{
				conn.ConnectionString = ConnectionInfo.ConnectionString;
				conn.Open();

				List<Server> servers = Server.Load(conn, Path);
				List<CounterDefinition> counters = CounterDefinition.Load(conn);

				foreach(var target in servers)
				{
					using (SqlConnection targetConnection = new SqlConnection())
					{
						ConnectionInfo ci = new ConnectionInfo();
						ci.ServerName = target.Name;
						ci.DatabaseName = "master";
						targetConnection.ConnectionString = ci.ConnectionString;
						targetConnection.Open();

						var currentSnapshot = PerformanceCounterSnapshot.Take(targetConnection, counters);

						// For cumulative counters I subtract the value of the initial counter value as a base
						foreach(var snap in currentSnapshot.Where(s => s.Counter.Cumulative))
						{
							var baseSnap = baseCounters.FirstOrDefault(s => s.Counter.Name == snap.Counter.Name && s.Counter.Instance == snap.Counter.Instance && snap.Interval.Server.Name == target.Name);
							if(baseSnap != null)
							{
								// take a copy of this snapshot
								var tmpSnap = (PerformanceCounterSnapshot)snap.Clone();

								//
								// rebase counter
								// 
								// see https://blogs.msdn.microsoft.com/oldnewthing/20160219-00/?p=93052
								//
								snap.Value -= baseSnap.RawValue;
								if (snap.Counter.Name.EndsWith("/sec"))
								{
									snap.Value = snap.Value / (snap.Interval.EndDate.Subtract(baseSnap.Interval.EndDate).TotalSeconds);
								}
                                snap.Rebased = true;

								baseSnap = tmpSnap;
							}
							else
							{
								// base counter not found: it needs to be added to the cache
								baseCounters.Add(snap);
							}
						}

						lock (counterData)
						{
							counterData.AddRange(currentSnapshot.Where(t => (t.Counter.Cumulative && t.Rebased) || !t.Counter.Cumulative));
						}
					}
						
				}

			}
		}


		private void Upload()
		{
			lock (counterData)
			{
				using (SqlConnection conn = new SqlConnection())
				{
					conn.ConnectionString = ConnectionInfo.ConnectionString;
					conn.Open();
					PerformanceCounterSnapshot.SaveToDatabase(conn, TargetTable, counterData);
				}
				counterData = new List<PerformanceCounterSnapshot>();
			}
			
		}

    }
}
