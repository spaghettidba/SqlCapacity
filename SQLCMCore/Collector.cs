using NLog;
using SQLCMCore.Util;
using System;
using System.Collections;
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

        private List<WaitStatsSnapshot> baseWaitStats = new List<WaitStatsSnapshot>();
        private List<WaitStatsSnapshot> waitsData = new List<WaitStatsSnapshot>();

        private List<PerformanceCounterSnapshot> baseCounters = new List<PerformanceCounterSnapshot>();
        private List<PerformanceCounterSnapshot> counterData = new List<PerformanceCounterSnapshot>();
        
        private bool stopped;
        private bool saving = false;
        private bool collecting = false;

		public ConnectionInfo ConnectionInfo { get; set; } = new ConnectionInfo();

		public string Path { get; set; }

		public string PerformanceCountersTargetTable { get; set; } = "PerformanceCounters";
        public string WaitStatsTargetTable { get; set; } = "WaitStats";
        public int CollectionInterval { get; set; } = 15;
		public int UploadInterval { get; set; } = 60;

		public Collector(string server, string database)
		{
			ConnectionInfo.ServerName = server;
			ConnectionInfo.DatabaseName = database;
		}

		public void Run()
		{
			int errorCount = 0;
			DateTime dateOfLastError = DateTime.MinValue;
			Task t = Task.Factory.StartNew(() => UploadMain());
			while (!stopped)
			{
				try
                {
                    while (saving)
                    {
                        Thread.Sleep(10);
                    }
                    collecting = true;
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
					errorCount++;
					if (errorCount > 10)
					{
						if(dateOfLastError > DateTime.Now.AddSeconds(-CollectionInterval))
						{
							throw new InvalidOperationException("The maximum number of exceptions has been thrown", e);
						}
						else
						{
							errorCount = 0;
						}
					}
					dateOfLastError = DateTime.Now;
				}
                finally
                {
                    collecting = false;
                }

			}
		}

		private void UploadMain()
		{
			while (!stopped)
			{
                try
                {
                    while (collecting)
                    {
                        Thread.Sleep(10);
                    }
                    saving = true;
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
                finally
                {
                    saving = false;
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

                        CollectCounters(target.Name, targetConnection, counters);
                        CollectWaits(target.Name, targetConnection);
					}
						
				}

			}
		}

        private void CollectWaits(string serverName, SqlConnection targetConnection)
        {
            var currentSnapshot = WaitStatsSnapshot.Take(targetConnection);

            foreach(var snap in currentSnapshot)
            {
                var baseSnap = baseWaitStats.FirstOrDefault(s => s.Type == snap.Type && s.Interval.Server.Name == serverName);
                if(baseSnap != null)
                {
                    // take a copy of this snapshot
                    var tmpSnap = (WaitStatsSnapshot)snap.Clone();

                    // rebase wait stats
                    snap.Seconds -= baseSnap.Seconds;
                    snap.ResourceSeconds -= baseSnap.ResourceSeconds;
                    snap.SignalSeconds -= baseSnap.SignalSeconds;
                    snap.Count -= baseSnap.Count;

                    snap.Rebased = true;

                    baseWaitStats[baseWaitStats.IndexOf(baseSnap)] = tmpSnap;
                }
                else
                {
                    // base wait not found: it needs to be added to the cache
                    baseWaitStats.Add(snap);
                }
            }

            lock (waitsData)
            {
                waitsData.AddRange(currentSnapshot.Where(t => t.Rebased));
            }
        }

        private void CollectCounters(string serverName, SqlConnection targetConnection, List<CounterDefinition> counters)
        {
            var currentSnapshot = PerformanceCounterSnapshot.Take(targetConnection, counters);

            // For cumulative counters I subtract the value of the initial counter value as a base
            foreach (var snap in currentSnapshot.Where(s => s.Counter.Cumulative))
            {
                var baseSnap = baseCounters.FirstOrDefault(s => s.Counter.Name == snap.Counter.Name && s.Counter.Instance == snap.Counter.Instance && s.Interval.Server.Name == serverName);
                if (baseSnap != null)
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

                    baseCounters[baseCounters.IndexOf(baseSnap)] = tmpSnap;
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


        private void Upload()
        {
            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();

                using (SqlTransaction tran = conn.BeginTransaction())
                {
                    try
                    {
                        var Intervals = CreateIntervals(conn, tran);
                        UploadCounters(conn, tran, Intervals);
                        UploadWaits(conn, tran, Intervals);

                        tran.Commit();
                    }
                    catch (Exception e)
                    {
                        logger.Error(e);
                        tran.Rollback();
                        throw;
                    }
                }
            }
        }

        private IEnumerable<Interval> CreateIntervals(SqlConnection conn, SqlTransaction tran)
        {
            var Intervals = (
                    from t in counterData
                    group t by new
                    {
                        server_name = t.Interval.Server.Name
                    }
                    into grp
                    select new Interval() {
                        Id = Interval.CreateNew(conn, grp.Key.server_name, tran).Id,
                        Server = new Server() {
                            Name = grp.Key.server_name
                        }
                    }
                ).ToList();
            return Intervals;
        }

        private void UploadCounters(SqlConnection conn, SqlTransaction tran, IEnumerable<Interval> intervals)
		{
			lock (counterData)
			{
				PerformanceCounterSnapshot.SaveToDatabase(conn, PerformanceCountersTargetTable, counterData, tran, intervals);
			}
			
		}

        private void UploadWaits(SqlConnection conn, SqlTransaction tran, IEnumerable<Interval> intervals)
        {
            lock (waitsData)
            {
                WaitStatsSnapshot.SaveToDatabase(conn, WaitStatsTargetTable, waitsData, tran, intervals);
            }
        }


    }
}
