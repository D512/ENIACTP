using System;
using System.IO;
using System.Xml;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;
using System.Text.RegularExpressions;


namespace TP_Farmatodo
{
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	class Class1
	{

		#region Variables
		public static string stringDB = String.Empty;
		public static string strLogFile = String.Empty;
		public static string strEmailFile = String.Empty;
		public static string strUbicacionArchivo = String.Empty;
		public static string strCodError = String.Empty;
		public static string strTipoError = String.Empty;
		public static string strEscrituraLog = String.Empty;
		#endregion

		#region Clases
		public static ClaseInventarioFinal objInventario = new ClaseInventarioFinal();
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		#endregion


		
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main()
		{


			try 
			{
				
				// ESCRITURA EN CONSOLA
				Console.WriteLine("	 ");
				Console.WriteLine("	 ");
				Console.WriteLine("  ------------ IMPORTANDO ARCHIVOS DE FARMATODO ------------");
				Console.WriteLine("	 ");
				Console.WriteLine("	   <- POR FAVOR NO CERRAR LA PANTALLA ->");
				Console.WriteLine("	 ");

				
				// DETERMINA LA UBICACION DONDE ESTA CORRIENDO EL EJECUTABLE DE IMPORT
				strUbicacionArchivo = System.Reflection.Assembly.GetExecutingAssembly().Location.Substring(0,System.Reflection.Assembly.GetExecutingAssembly().Location.LastIndexOf("\\"));
				
				// LEE EL XML DE CONFIGURACION
				
				leerArchivoXML(strUbicacionArchivo);

				objTextFile.EscribirLog("LEYENDO ARCHIVO XML DE CONFIGURACION");
				
				// INSERTAR INFORMACION EN EL LOG
				objTextFile.EscribirLog("PARAMETROS DEL ARCHIVO XML CARGADOS SATISFACTORIAMENTE");
			
			}
			catch (Exception ex) 
			{

				strCodError = ex.Message;
				Console.WriteLine("   --> ERROR -- ERROR LEYENDO ARCHIVO XML DE CONFIGURACION. CODIGO: " + strCodError);
				
			}

				try
				{
			
					objTextFile.EscribirLog("PARAMETROS DE ENTRADA DEL SISTEMA CARGADOS SATISFACTORIAMENTE.");
						
					objInventario.insertarInventarioFinal();

					objTextFile.EscribirLog("FINALIZO SATISFACTORIAMENTE EL PROCESO DE IMPORT");

				}			
				catch (Exception ex) 
				{
					strCodError = ex.Message;

					objTextFile.EscribirLog("  ---> ERROR -- OCURRIO UN ERROR CON EL ARCHIVO DE ENTRADA O EL TIPO DE ARCHIVO. CODIGO: " + strCodError);
				
				}
		}

		
		// CLASE QUE LEE ARCHIVO XML DE CONFIGURACION
		public static void leerArchivoXML(string strUbicacion)
		{
			
			try
			{
				XmlTextReader reader = new XmlTextReader (strUbicacion + "\\" + "XMLConfig.xml");
			
				while (reader.Read())
				{
					if(reader.AttributeCount > 0)
					{
					
						if (reader.GetAttribute(0) == "STRINGDB")
						{
							stringDB = reader.GetAttribute(1);
						}
						
						if (reader.GetAttribute(0) == "STRINGLOGTOTAL")
						{
							strLogFile = reader.GetAttribute(1);
						}						

					}
				}
		
			}
			catch(Exception ex)
			{
				strCodError = ex.Message;

				Console.WriteLine("   --> ERROR -- ERROR LEYENDO ARCHIVO XML DE CONFIGURACION. CODIGO: " + strCodError);

				// SLEEP DEL SISTEMA
				Thread.Sleep(15000);
				
			}
		
		}


	}
}
