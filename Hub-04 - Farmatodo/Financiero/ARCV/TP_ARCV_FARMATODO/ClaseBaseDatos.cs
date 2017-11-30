using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Specialized;

namespace TP_RETENCIONARCV
{
	/// <summary>
	/// Clase de BD
	/// </summary>
	public class ClaseBaseDatos
	{
		/// <summary>
		/// Builder
		/// </summary>
        public ClaseBaseDatos()
		{
		}

        /// <summary>
        /// Establece conexion
        /// </summary>
        /// <returns>Conexion a BD</returns>
		public SqlConnection Conexion()
		{
			string  strCon = "";
			SqlConnection mySqlConnection = null;
			
			strCon = Class1.stringDB;
			mySqlConnection = new SqlConnection(strCon);

			return mySqlConnection;
		}

        /// <summary>
        /// Crea comando SQL
        /// </summary>
        /// <param name="stringSelect">Query a utilizar</param>
        /// <param name="mySqlConnection">Conexion BD</param>
        /// <returns>Comando SQL</returns>
		public SqlCommand Comando(string stringSelect, SqlConnection mySqlConnection)
		{
			SqlCommand mySqlCommand = mySqlConnection.CreateCommand();
			mySqlCommand.CommandText = stringSelect;

			return mySqlCommand;
		}

        /// <summary>
        /// Adapta la data, aplica formato
        /// </summary>
        /// <param name="mySqlCommand">Comando SQL</param>
        /// <returns>Data SQL modificada</returns>
		public SqlDataAdapter DataAdapter(SqlCommand mySqlCommand)
		{
			SqlDataAdapter mySqlDataAdapter = new SqlDataAdapter();
			mySqlDataAdapter.SelectCommand = mySqlCommand;

			return mySqlDataAdapter;
		}
	}	
}
