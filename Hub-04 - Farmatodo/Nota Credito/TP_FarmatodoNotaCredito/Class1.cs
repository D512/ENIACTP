using System;
using System.IO;
using System.Xml;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;
using System.Text.RegularExpressions;


namespace NotaCredito
{
	/// <summary>
	/// CLASE PRINCIPAL O EJECUTABLE
	/// </summary>
	class Class1
	{

		public static string stringDB="";//STRING DE CONEXION
		public static string strLogFile="";//RUTA DEL ARCHIVO LOG
		public static string strLogErrorFile="";//RUTA DEL ARCHIVO LOG DE ERROR
		public static string strEmailFile="";//RUTA DEL ARCHIVO MAIL
		public static string strFileName="";//RUTA DEL ARCHIVO PLANO DE INICIO
		static string  strUbicacionArchivo="";//RUTA DEL EJECUTABLE
		public static GenerarArchivosLog objTextFile=new GenerarArchivosLog();//OBJETO DE LA CLASE QUE ESCRIBE EN EL LOG
		public static ClaseNotaCredito objNotaCredito=new ClaseNotaCredito();//OBJETO DE LA CLASE QUE HACE EL PROCESO DE CARGA DE LA NOTA DE CREDITO
		
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
				Console.WriteLine("  ------------ IMPORTANDO NOTAS DE CREDITO ------------");
				Console.WriteLine("	 ");
				Console.WriteLine("	      <- POR FAVOR NO CERRAR LA PANTALLA ->");
				Console.WriteLine("	 ");


				// ASIGNANDO VARIABLES DE ENTRADA
	
				// DETERMINA LA UBICACION DONDE ESTA CORRIENDO EL EJECUTABLE DE IMPORT
				strUbicacionArchivo = System.Reflection.Assembly.GetExecutingAssembly().Location.Substring(0,System.Reflection.Assembly.GetExecutingAssembly().Location.LastIndexOf("\\"));
				
				// LEE EL XML DE CONFIGURACION
				
				leerArchivoXML(strUbicacionArchivo);
				objTextFile.EscribirLog("LEYENDO ARCHIVO XML DE CONFIGURACION");
				objTextFile.EscribirLog("PARAMETROS DEL ARCHIVO XML CARGADOS SATISFACTORIAMENTE");
			
			}
			catch (Exception ex) 
			{
				objTextFile.EscribirLog("   --> ERROR -- ERROR LEYENDO ARCHIVO XML DE CONFIGURACION. CODIGO: " + ex.Message);	
			}			
			
			// VERIFICA QUE LOS ARGUMENTOS DE ENTRADA NO SEAN VALORES NULOS
			if (strFileName != "")
			{
				try
				{
					objTextFile.EscribirLog("EL ARCHIVO PLANO DE ENTRADA SE HA ABIERTO EXITOSAMENTE. NOMBRE ARCHIVO: " + strFileName);
			
					// VERIFICA QUE EL ARCHIVO EXISTA
					if (File.Exists(strFileName))
					{
					    try
						{
			             //SE INSERTA LA NotaCredito
						 objNotaCredito.InsertNotaCredito(strFileName);
						 Class1.objTextFile.EscribirLog("HA FINALIZADO EL PROCESO DE IMPORT DE LAS NOTAS DE CREDITO DEL ARCHIVO PLANO DE ENTRADA: " + strFileName);
						}
						catch(Exception e)
						{
							objTextFile.EscribirLog("ERROR AL CARGAR LAS NOTAS DE CREDITO DEL ARCHIVO PLANO: "+strFileName);
							objTextFile.EscribirLog("  CODIGO DE ERROR: "+e.Message); 
						}
					}
					// EL ARCHIVO NO EXISTE
					else
					{
						objTextFile.EscribirLog("  ---> ERROR -- EL ARCHIVO DE ENTRADA: " + strFileName + "  NO EXISTE");					
					}
				}			
				catch (Exception ex) 
				{
					objTextFile.EscribirLog("  ---> ERROR -- OCURRIO UN ERROR CON EL ARCHIVO DE ENTRADA. CODIGO: " + ex.Message);			
				}

			}
			// LOS PARAMETROS DE ENTRADA NO PUEDEN SER VALORES NULOS
			else
			{				
				objTextFile.EscribirLog("  ---> ERROR -- LA RUTA DEL ARCHIVO PLANO DE ENTRADA NO SE ENCUENTRA.");
			}
		}//FIN DEL METODO DE INSERCION DE NOTA DE CREDITO

		
		// METODO QUE LEE ARCHIVO XML DE CONFIGURACION
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

						if (reader.GetAttribute(0) == "STRINGLOGERROR")
						{
							strLogErrorFile = reader.GetAttribute(1);
						}

						if (reader.GetAttribute(0) == "STRINGEMAIL")
						{
							strEmailFile = reader.GetAttribute(1);
						}

						if (reader.GetAttribute(0) == "STRINGFILEIN")
						{
							strFileName = reader.GetAttribute(1);
						}
					}
				}
			}
			catch(Exception ex)
			{
				objTextFile.EscribirLog("  ---> ERROR -- ERROR LEYENDO ARCHIVO XML DE CONFIGURACION. CODIGO: " + ex.Message);
				// SLEEP DEL SISTEMA
				Thread.Sleep(15000);				
			}		
		}
	}
}
