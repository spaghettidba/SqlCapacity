using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLCMCore
{
	public class Interval
	{
		public int Id { get; set; }
		public DateTime EndDate { get; set; }
		public int DurationMinutes { get; set; }
		public Server Server { get; set; }

		public static Interval CreateNew(SqlConnection conn, string ServerName)
		{
			string sql = @"
				INSERT INTO Intervals(
					interval_id,
					server_id,
					end_time,
					duration_minutes
				)

				OUTPUT INSERTED.interval_id
				SELECT
					interval_id = ISNULL(MAX(interval_id), 0) + 1,
					server_id = (SELECT server_id FROM Servers WHERE server_name = '{0}'),
					end_time = GETDATE(),
					duration_minutes = DATEDIFF(minute, ISNULL((SELECT end_time FROM Intervals WHERE server_id = (SELECT server_id FROM Servers WHERE server_name = '{0}')),GETDATE()), GETDATE())
				FROM Intervals
			";

			Interval interval = null;
			using(SqlCommand cmd = conn.CreateCommand())
			{
				cmd.CommandText = String.Format(sql, ServerName);

				SqlDataReader reader = cmd.ExecuteReader();
				while (reader.Read())
				{
					interval = new Interval();
					interval.Id = reader.GetInt32(reader.GetOrdinal("interval_id"));
					interval.Server = new Server() {
						Id = reader.GetOrdinal("server_id"),
						Name = ServerName
					};
					interval.EndDate = reader.GetDateTime(reader.GetOrdinal("end_time"));
					interval.DurationMinutes = reader.GetInt32(reader.GetOrdinal("duration_minutes"));
				}
			}

			return interval;
		}
	}
}
