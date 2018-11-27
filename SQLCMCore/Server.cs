using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SQLCMCore.Util;

namespace SQLCMCore
{
	public class Server
	{
		public int Id { get; set; }
		public string Name { get; set; }
		public string Path { get; set; }

		public static List<Server> Load(SqlConnection conn, string path)
		{
			string sql = @"
				SELECT * 
				FROM Servers
				WHERE path LIKE @path + '%'
			";

			List<Server> result = new List<Server>();

			using(SqlCommand cmd = conn.CreateCommand())
			{
				cmd.CommandText = sql;
				cmd.CommandType = System.Data.CommandType.Text;
				var prm = new SqlParameter("@path", System.Data.DbType.String);
				prm.Direction = System.Data.ParameterDirection.Input;
				prm.ParameterName = "@path";
				prm.Size = 255;
				prm.Value = path;
				cmd.Parameters.Add(prm);

				using (SqlDataReader rdr = cmd.ExecuteReader())
				{
					while (rdr.Read())
					{
						result.Add(new Server()
						{
							Name = rdr.GetString(rdr.GetOrdinal("server_name")),
							Path = rdr.GetString(rdr.GetOrdinal("path")),
							Id = rdr.GetInt32(rdr.GetOrdinal("server_id"))
						});
					}
				}

			}
			

			return result;
		}
	}
}
