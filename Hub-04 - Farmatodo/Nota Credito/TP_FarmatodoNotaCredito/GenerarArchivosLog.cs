using System;
using System.IO;

namespace NotaCredito
{
	/// <summary>
	/// Summary description for GenerarArchivosLog.
	/// </summary>
	public class GenerarArchivosLog
	{
		public GenerarArchivosLog()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		public void EscribirLog(string strTextoLog)
		{

			// VARIABLES
			StreamWriter SW;
			string strFecha = Convert.ToString(System.DateTime.Now);


			// DETERMINA SI EL ARCHIVO DE LOG EXISTE
			if (File.Exists(Class1.strLogFile))
			{
				// ABRE EL ARCHIVO PARA EJECUTAR UNOS APPEND
				SW=File.AppendText(Class1.strLogFile);

			}
			else
			{
				// CREA UN ARCHIVO NUEVO DE LOG
				SW=File.CreateText(Class1.strLogFile);
				SW.WriteLine(" ");
				SW.WriteLine(" ");
				SW.WriteLine(" ");
				SW.WriteLine(" ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("---                      PROCESO DE CARGA DE NOTAS DE CRÉDITO                --- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine(" ");
				SW.WriteLine("  NOMBRE DEL ARCHIVO DE ENTRADA:    " + Class1.strFileName.Replace(" ",""));
				SW.WriteLine("  FECHA DE LA CARGA: " + strFecha);
				SW.WriteLine(" ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine(" ");
			}	
			
			SW.WriteLine(strFecha + ",  "  + strTextoLog);
			SW.Close();

		}

		public void EscribirLogError(string strTextoLogError)
		{

			// VARIABLES
			StreamWriter SW;
			string strFecha = Convert.ToString(System.DateTime.Now);


			// DETERMINA SI EL ARCHIVO DE LOG EXISTE
			if (File.Exists(Class1.strLogErrorFile))
			{
				// ABRE EL ARCHIVO PARA EJECUTAR UNOS APPEND
				SW=File.AppendText(Class1.strLogErrorFile);

			}
			else
			{
				// CREA UN ARCHIVO NUEVO DE LOG
				SW=File.CreateText(Class1.strLogErrorFile);
				SW.WriteLine(" ");
				SW.WriteLine(" ");
				SW.WriteLine(" ");
				SW.WriteLine(" ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("---               PROCESO DE CARGA DE NOTAS DE CRÉDITO CON ERRORES           --- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine(" ");
				SW.WriteLine("  NOMBRE DEL ARCHIVO DE ENTRADA:    " + Class1.strFileName.Replace(" ",""));
				SW.WriteLine("  FECHA DE LA CARGA: " + strFecha);
				SW.WriteLine(" ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine(" ");
			}	
			
			SW.WriteLine(strFecha + ",  "  + strTextoLogError);
			SW.Close();

		}
	}
}
