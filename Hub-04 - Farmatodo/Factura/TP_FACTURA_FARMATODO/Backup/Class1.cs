using System;
using System.IO;
using System.Xml;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;
using System.Text.RegularExpressions;


namespace TP_FacturaFarmatodo
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
		public static string strFileName = String.Empty;
		public static string strTipoArchivo = String.Empty;
		public static string strTableName01 = String.Empty;
		public static string strTableName02 = String.Empty;
		public static string strTableName03 = String.Empty;
		public static string strHub = String.Empty;
		public static string strDir = String.Empty;
		public static string strCliente = String.Empty;
		//correo
		public static string strEmail = String.Empty;
		public static string strEmailCC = String.Empty;
		public static string strProveedor = String.Empty;
		#endregion

		#region Clases
		public static ClaseFacturaFarmatodo objFactura = new ClaseFacturaFarmatodo();
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
				Console.WriteLine("  ------------ IMPORTANDO FACTURAS FARMATODO ------------");
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

			// VERIFICA QUE LOS ARGUMENTOS DE ENTRADA NO SEAN VALORES NULOS
			if (strFileName != "")
			{
				try
				{
					objTextFile.EscribirLog("PARAMETROS DE ENTRADA DEL SISTEMA CARGADOS SATISFACTORIAMENTE.  NOMBRE ARCHIVO: " + strFileName + "  TIPO DE ARCHIVO: " + strTipoArchivo);

					// VERIFICA QUE EL ARCHIVO EXISTA
					if (File.Exists(strFileName))
					{							
							objFactura.InsertFacturaFarmatodo(strFileName);
							objTextFile.EscribirLog("FINALIZO SATISFACTORIAMENTE EL PROCESO DE IMPORT DEL ARCHIVO: " + strFileName);
						
					}
					// EL ARCHIVO NO EXISTE
					else
					{
						strCodError = "99";
						objTextFile.EscribirLog("  ---> ERROR -- EL ARCHIVO DE ENTRADA: " + strFileName + "  NO EXISTE");					
					}
				}			
				catch (Exception ex) 
				{
					strCodError = ex.Message;
					objTextFile.EscribirLog("  ---> ERROR -- OCURRIO UN ERROR CON EL ARCHIVO DE ENTRADA. CODIGO: " + strCodError);
				}
			}
			// LOS PARAMETROS DE ENTRADA NO PUEDEN SER VALORES NULOS
			else
			{
				strCodError = "99";
				objTextFile.EscribirLog("  ---> ERROR -- LOS PARAMETROS DE ENTRADA DEL SISTEMA NO PUEDEN SER VALORES NULOS");
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
						
						if (reader.GetAttribute(0) == "STRINGLOG")
						{
							strLogFile = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRINGFILENAME")
						{
							strFileName = reader.GetAttribute(1);
						}

						if (reader.GetAttribute(0) == "STRTABLENAME01")
						{
							strTableName01 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRTABLENAME02")
						{
							strTableName02 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRTABLENAME03")
						{
							strTableName03 = reader.GetAttribute(1);
						}

						if (reader.GetAttribute(0) == "STRHUB")
						{
							strHub = reader.GetAttribute(1);
						}

						if (reader.GetAttribute(0) == "STRDIR")
						{
							strDir = reader.GetAttribute(1);
						}

						if (reader.GetAttribute(0) == "STRCLIENTE")
						{
							strCliente = reader.GetAttribute(1);
						}

						if (reader.GetAttribute(0) == "STRINGEMAIL")
						{
							strEmailFile = reader.GetAttribute(1);
						}

					}
				}
			}
			catch(Exception ex)
			{
				strCodError = ex.Message;
				objTextFile.EscribirLog("   --> ERROR -- ERROR LEYENDO ARCHIVO XML DE CONFIGURACION. CODIGO: " + strCodError);
				// SLEEP DEL SISTEMA
				Thread.Sleep(15000);
			}
		}
	}
}
