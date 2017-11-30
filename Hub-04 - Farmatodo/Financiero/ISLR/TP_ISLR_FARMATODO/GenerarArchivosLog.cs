using System;
using System.IO;

namespace TP_RETENCIONISLR
{
	/// <summary>
	/// Clase Genera Archivo LOG
	/// </summary>
	public class GenerarArchivosLog
	{
        /// <summary>
        /// Builder
        /// </summary>
		public GenerarArchivosLog()
		{    
		}

        /// <summary>
        /// Escribe en log
        /// </summary>
        /// <param name="strTextoLog">String a escribir</param>
		public void EscribirLog(string strTextoLog)
		{
			// VARIABLES
            StreamWriter SW;
			string strFecha = "";
			
			if (strTextoLog != "")
			{
				strFecha = Convert.ToString(System.DateTime.Now);
			}

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
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("---           PROCESO DE CARGA DE RETENCIONES ISLR FARMATODO VE               --- ");
				SW.WriteLine("-------------------------------------------------------------------------------- ");
				SW.WriteLine("  NOMBRE ARCHIVO:    " + Class1.strFileName.Replace(" ",""));
				SW.WriteLine("  FECHA DE LA CARGA: " + strFecha);				
				SW.WriteLine(" ");			
				Class1.strEscrituraLog = "SI";
			}	
			
			if (strTextoLog != "")
			{
				SW.WriteLine(strFecha + ",  "  + strTextoLog);
			}
			
			SW.Close();
		}		
	}
}
