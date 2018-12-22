using NLog;
using SQLCMCore.Util;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLCMCore
{
    public class WaitStatsSnapshot : ICloneable
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public String Type { get; set; }
        public double Seconds { get; set; }
        public double ResourceSeconds { get; set; }
        public double SignalSeconds { get; set; }
        public long Count { get; set; }
        public bool Rebased { get; set; }
        public Interval Interval { get; set; }

        public object Clone()
        {
            return this.MemberwiseClone();
        }


        public static List<WaitStatsSnapshot> Take(SqlConnection targetConnection)
        {
            string sql = @"
                WITH [Waits] 
                AS (
	                SELECT wait_type, wait_time_ms/ 1000.0 AS [WaitS],
                            (wait_time_ms - signal_wait_time_ms) / 1000.0 AS [ResourceS],
                            signal_wait_time_ms / 1000.0 AS [SignalS],
                            waiting_tasks_count AS [WaitCount]
                    FROM sys.dm_os_wait_stats WITH (NOLOCK)
                    WHERE [wait_type] NOT IN (
                        N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP',
		                N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE',
                        N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE',
                        N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE',
		                N'DBMIRRORING_CMD', N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE',
                        N'EXECSYNC', N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX',
                        N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT', 
		                N'HADR_NOTIFICATION_DEQUEUE', N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE',
                        N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', 
		                N'MEMORY_ALLOCATION_EXT', N'ONDEMAND_TASK_QUEUE',
		                N'PARALLEL_REDO_DRAIN_WORKER', N'PARALLEL_REDO_LOG_CACHE', N'PARALLEL_REDO_TRAN_LIST',
		                N'PARALLEL_REDO_WORKER_SYNC', N'PARALLEL_REDO_WORKER_WAIT_WORK',
		                N'PREEMPTIVE_HADR_LEASE_MECHANISM', N'PREEMPTIVE_SP_SERVER_DIAGNOSTICS',
		                N'PREEMPTIVE_OS_LIBRARYOPS', N'PREEMPTIVE_OS_COMOPS', N'PREEMPTIVE_OS_CRYPTOPS',
		                N'PREEMPTIVE_OS_PIPEOPS', N'PREEMPTIVE_OS_AUTHENTICATIONOPS',
		                N'PREEMPTIVE_OS_GENERICOPS', N'PREEMPTIVE_OS_VERIFYTRUST',
		                N'PREEMPTIVE_OS_FILEOPS', N'PREEMPTIVE_OS_DEVICEOPS', N'PREEMPTIVE_OS_QUERYREGISTRY',
		                N'PREEMPTIVE_OS_WRITEFILE',
		                N'PREEMPTIVE_XE_CALLBACKEXECUTE', N'PREEMPTIVE_XE_DISPATCHER',
		                N'PREEMPTIVE_XE_GETTARGETSTATE', N'PREEMPTIVE_XE_SESSIONCOMMIT',
		                N'PREEMPTIVE_XE_TARGETINIT', N'PREEMPTIVE_XE_TARGETFINALIZE',
                        N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PWAIT_DIRECTLOGCONSUMER_GETNEXT',
		                N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
		                N'QDS_ASYNC_QUEUE',
                        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'REQUEST_FOR_DEADLOCK_SEARCH',
		                N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP',
		                N'SLEEP_DCOMSTARTUP', N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY',
                        N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK',
                        N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT', N'SP_SERVER_DIAGNOSTICS_SLEEP',
		                N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'SQLTRACE_WAIT_ENTRIES',
		                N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', N'WAIT_XTP_HOST_WAIT',
		                N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE', N'WAIT_XTP_RECOVERY',
		                N'XE_BUFFERMGR_ALLPROCESSED_EVENT', N'XE_DISPATCHER_JOIN',
                        N'XE_DISPATCHER_WAIT', N'XE_LIVE_TARGET_TVF', N'XE_TIMER_EVENT')
                    AND waiting_tasks_count > 0
                )
                SELECT
	                W1.wait_type,
                    CAST (MAX (W1.WaitS) AS DECIMAL (16,2)) AS [wait_sec],
                    CAST (MAX (W1.ResourceS) AS DECIMAL (16,2)) AS [resource_sec],
                    CAST (MAX (W1.SignalS) AS DECIMAL (16,2)) AS [signal_sec],
                    MAX (W1.WaitCount) AS [wait_count]
                FROM Waits AS W1
                GROUP BY W1.wait_type
                HAVING CAST (MAX (W1.WaitS) AS DECIMAL (16,2)) > 0
                ORDER BY wait_sec DESC
                OPTION (RECOMPILE);
            ";

            List<WaitStatsSnapshot> result = new List<WaitStatsSnapshot>();
            DateTime intervalEnd = DateTime.Now;

            using (SqlCommand cmd = targetConnection.CreateCommand())
            {
                cmd.CommandText = sql;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(
                            new WaitStatsSnapshot()
                            {
                                Type = reader.GetString(reader.GetOrdinal("wait_type")),
                                Seconds = Convert.ToDouble(reader.GetDecimal(reader.GetOrdinal("wait_sec"))),
                                SignalSeconds = Convert.ToDouble(reader.GetDecimal(reader.GetOrdinal("signal_sec"))),
                                ResourceSeconds = Convert.ToDouble(reader.GetDecimal(reader.GetOrdinal("resource_sec"))),
                                Count = reader.GetInt64(reader.GetOrdinal("wait_count")),
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

        public static void SaveToDatabase(SqlConnection conn, string TableName, IEnumerable<WaitStatsSnapshot> data, SqlTransaction tran, IEnumerable<Interval> intervals)
        {
            // first of all I need to save the wait types to the WaitDefinitions table
            var WaitDefinitions = (
                from t in data
                group t by new
                {
                    type_description = t.Type
                }
                into grp
                select new
                {
                    grp.Key.type_description
                }).ToList();

            using(var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = @"
                    IF OBJECT_ID('tempdb..#WaitDefinitions') IS NOT NULL
                        DROP TABLE #WaitDefinitions;
                    SELECT TOP(0) type_description
                    INTO #WaitDefinitions
                    FROM WaitDefinitions
                ";
                cmd.ExecuteNonQuery();
            }


            using (SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                                                        SqlBulkCopyOptions.KeepIdentity |
                                                                        SqlBulkCopyOptions.FireTriggers |
                                                                        SqlBulkCopyOptions.CheckConstraints |
                                                                        SqlBulkCopyOptions.TableLock,
                                                                        tran))
            {
                bulkCopy.DestinationTableName = "#WaitDefinitions";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;

                using (var dt = DataUtils.ToDataTable(WaitDefinitions))
                {
                    bulkCopy.WriteToServer(dt);
                }
            }


            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = @"
                    INSERT INTO WaitDefinitions (type_id, type_description) 
                    SELECT 
                         type_id = ROW_NUMBER() OVER (ORDER BY type_description) + ISNULL((SELECT MAX(type_id) FROM WaitDefinitions), 0) 
                        ,type_description 
                    FROM #WaitDefinitions AS src 
                    WHERE type_description NOT IN (
                            SELECT type_description 
                            FROM WaitDefinitions
                        )
                        AND type_description IS NOT NULL;
                ";

                cmd.ExecuteNonQuery();
            }


            string sql = 
                $"SELECT * " +
                $"FROM WaitDefinitions";
            DataTable waits = null;

            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    using (DataSet ds = new DataSet())
                    {
                        adapter.Fill(ds);
                        waits = ds.Tables[0];
                    }
                }
            }

            var WaitDefs = (
                    from t in waits.AsEnumerable()
                    select new
                    {
                        type_id = t["type_id"],
                        type_description = t["type_description"]
                    }
                ).ToList();


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
                    join u in WaitDefs
                        on t.Type equals u.type_description
                    group t by new
                    {
                        server_name = t.Interval.Server.Name,
                        wait = u.type_id
                    }
                    into grp
                    select new
                    {
                        interval_id = intervals.First(i => i.Server.Name == grp.Key.server_name).Id,
                        grp.Key.wait,
                        tot_wait_seconds     = grp.Sum(t => t.Seconds),
                        tot_resource_seconds = grp.Sum(t => t.ResourceSeconds),
                        tot_signal_seconds   = grp.Sum(t => t.SignalSeconds),
                        tot_wait_count       = grp.Sum(t => t.Count)
                    }).ToList();

                using (var dt = DataUtils.ToDataTable(Table.Where(t => t.tot_resource_seconds > 0 || t.tot_signal_seconds > 0 || t.tot_wait_seconds > 0 || t.tot_wait_count > 0)))
                {
                    bulkCopy.WriteToServer(dt);
                }
            }
        }
    }
}
