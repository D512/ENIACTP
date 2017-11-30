using System;
using System.IO;

namespace TP_FarmatodoOC
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

			}
			

			if (Class1.strEscrituraLog == "")
			{

				SW.WriteLine("  NOMBRE ARCHIVO:    " + Class1.strFileName.Replace(" ",""));				
				SW.WriteLine("  FECHA DE LA CARGA: " + strFecha);				
				SW.WriteLine("-------------------------------------------------------------------------------- ");
							
				Class1.strEscrituraLog = "SI";

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

			}
			

			if (Class1.strEscrituraLogError == "")
			{
				

				SW.WriteLine(" ");
				SW.WriteLine(" ");
				SW.WriteLine(" ");
				SW.WriteLine(" ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("---             PROCESO DE CARGA DE ARCHIVOS DE FARMATODO CON ERRORES        --- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine(" ");
				SW.WriteLine("  NOMBRE ARCHIVO:    " + Class1.strFileName.Replace(" ",""));
				SW.WriteLine("  TIPO DE ARCHIVO:   " + Class1.strTipoArchivo);
				SW.WriteLine("  FECHA DE LA CARGA: " + strFecha);
				SW.WriteLine(" ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine(" ");
			
				Class1.strEscrituraLogError = "SI";

			}	
			
			SW.WriteLine(strFecha + ",  "  + strTextoLogError);
			SW.Close();

		}


	}
}
