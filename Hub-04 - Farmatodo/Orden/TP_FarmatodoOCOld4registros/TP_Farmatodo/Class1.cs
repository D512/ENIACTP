using System;
using System.IO;
using System.Xml;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;
using System.Text.RegularExpressions;


namespace TP_FarmatodoOC
{
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	class Class1
	{

		#region "DECLARACION VARIABLES"

		public static string stringDB = String.Empty;
		public static string strLogFile = String.Empty;
		public static string strEmailFile = String.Empty;
		public static string strUbicacionArchivo = String.Empty;
		public static string strCodError = String.Empty;
		public static string strTipoError = String.Empty;
		public static string strEscrituraLog = String.Empty;
		public static string strFileName = String.Empty;
		public static string strTipoArchivo = String.Empty;

		#endregion

		#region "DECLARACION LIBRERIAS"

		public static ClaseInventario objInventario = new ClaseInventario();
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		public static ClaseOrdenCompra objOrdenCompra = new ClaseOrdenCompra();

		#endregion


		
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main(string[] args)
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


				// ASIGNANDO VARIABLES DE ENTRADA
				strFileName = args[0];
				strTipoArchivo = args[1];



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
			if (strFileName != "" || strTipoArchivo != "")
			{

				try
				{
			
					objTextFile.EscribirLog("PARAMETROS DE ENTRADA DEL SISTEMA CARGADOS SATISFACTORIAMENTE.  NOMBRE ARCHIVO: " + strFileName + "  TIPO DE ARCHIVO: " + strTipoArchivo);
			
					
					// VERIFICA QUE EL ARCHIVO EXISTA
					if (File.Exists(strFileName))
					{
			
						// ARCHIVO DE INVENTARIO //
						if (strTipoArchivo == "INV")
						{
							objInventario.InsertInventario(strFileName);

							objTextFile.EscribirLog("FINALIZO SATISFACTORIAMENTE EL PROCESO DE IMPORT DEL ARCHIVO: " + strFileName);

						}
						else if (strTipoArchivo == "OC")
						{
							objOrdenCompra.InsertOrdenCompra(strFileName);

							objTextFile.EscribirLog("FINALIZO SATISFACTORIAMENTE EL PROCESO DE IMPORT DEL ARCHIVO: " + strFileName);

						}
						// TIPO DE ARCHIVO INVALIDO
						else
						{
							strCodError = "99";
							objTextFile.EscribirLog("  ---> ERROR -- EL TIPO DE ARCHIVO DE ENTRADA: " + strTipoArchivo + "  ES INVALIDO");
						}

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

					objTextFile.EscribirLog("  ---> ERROR -- OCURRIO UN ERROR CON EL ARCHIVO DE ENTRADA O EL TIPO DE ARCHIVO. CODIGO: " + strCodError);
				
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

				Console.WriteLine("   --> ERROR -- ERROR LEYENDO ARCHIVO XML DE CONFIGURACION. CODIGO: " + strCodError);

				// SLEEP DEL SISTEMA
				Thread.Sleep(15000);
				
			}
		
		}


	}
}
