using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Specialized;

namespace ReporteOC
{
	/// <summary>
	/// Summary description for ClaseBaseDatos.
	/// </summary>
	public class ClaseBaseDatos
	{
		public ClaseBaseDatos()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		public SqlConnection Conexion()
		{
			string  strCon = "";
			SqlConnection mySqlConnection = null;
			
			strCon = Class1.stringDB;
			mySqlConnection = new SqlConnection(strCon);

			return mySqlConnection;
		}

		public SqlCommand Comando(string stringSelect, SqlConnection mySqlConnection)
		{
			SqlCommand mySqlCommand = mySqlConnection.CreateCommand();
			mySqlCommand.CommandText = stringSelect;

			return mySqlCommand;
		}

		public SqlDataAdapter DataAdapter(SqlCommand mySqlCommand)
		{
			SqlDataAdapter mySqlDataAdapter = new SqlDataAdapter();
			mySqlDataAdapter.SelectCommand = mySqlCommand;

			return mySqlDataAdapter;
		}
	}	
}
