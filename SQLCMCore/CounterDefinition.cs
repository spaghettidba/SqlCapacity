using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLCMCore
{
	public class CounterDefinition
	{
        public int Id { get; set; }
		public string Name { get; set; }
		public string Instance { get; set; }
		public bool Cumulative { get; set; }

		public static List<CounterDefinition> Load(SqlConnection conn)
		{
			List<CounterDefinition> result = new List<CounterDefinition>();
			string sql = @"
				SELECT counter_id, counter_name, instance_name, cumulative 
				FROM CounterDefinitions
			";
			using (SqlCommand cmd = conn.CreateCommand())
			{
				cmd.CommandText = sql;
				using (SqlDataReader rdr = cmd.ExecuteReader())
				{
					while (rdr.Read())
					{
						result.Add(new CounterDefinition() {
                            Id         = rdr.GetInt32(rdr.GetOrdinal("counter_id")),
                            Name       = rdr.GetString(rdr.GetOrdinal("counter_name")),
                            Instance   = rdr.GetString(rdr.GetOrdinal("instance_name")),
                            Cumulative = rdr.GetBoolean(rdr.GetOrdinal("cumulative"))
                        });
					}
				}

			}
			return result;
		}
	}
}
