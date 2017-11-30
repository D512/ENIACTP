using System;
using System.IO;
using System.Xml;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;
using System.Text.RegularExpressions;


namespace TP_RETENCIONISLR
{
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	class Class1
	{

		#region Variables

		public static string stringDB = String.Empty;

		public static string strLogFile = String.Empty;
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
        public static char caracter;
		
		public static string strCampoEnc01 = String.Empty;
		public static string strCampoEnc02 = String.Empty;
		public static string strCampoEnc03 = String.Empty;
		public static string strCampoEnc04 = String.Empty;
		public static string strCampoEnc05 = String.Empty;
		public static string strCampoEnc06 = String.Empty;
		public static string strCampoEnc07 = String.Empty;
		public static string strCampoEnc08 = String.Empty;
		public static string strCampoEnc09 = String.Empty;
		public static string strCampoEnc10 = String.Empty;
		public static string strCampoEnc11 = String.Empty;
		public static string strCampoEnc12 = String.Empty;
		public static string strCampoEnc13 = String.Empty;
		public static string strCampoEnc14 = String.Empty;
		public static string strCampoEnc15 = String.Empty;
		public static string strCampoEnc16 = String.Empty;
		public static string strCampoEnc17 = String.Empty;
		public static string strCampoEnc18 = String.Empty;
        public static string strCampoEnc19 = String.Empty;
 
		
		public static string strCampoDet01 = String.Empty;
		public static string strCampoDet02 = String.Empty;
		public static string strCampoDet03 = String.Empty;
		public static string strCampoDet04 = String.Empty;
		public static string strCampoDet05 = String.Empty;
		public static string strCampoDet06 = String.Empty;
		public static string strCampoDet07 = String.Empty;
		public static string strCampoDet08 = String.Empty;
		public static string strCampoDet09 = String.Empty;
		public static string strCampoDet10 = String.Empty;
		public static string strCampoDet11 = String.Empty;
		public static string strCampoDet12 = String.Empty;
		public static string strCampoDet13 = String.Empty;
		public static string strCampoDet14 = String.Empty;
		public static string strCampoDet15 = String.Empty;
		public static string strCampoDet16 = String.Empty;
		public static string strCampoDet17 = String.Empty;
		public static string strCampoDet18 = String.Empty;
		public static string strCampoDet19 = String.Empty;
		
		public static string strCampoTra01 = String.Empty;
		public static string strCampoTra02 = String.Empty;
		public static string strCampoTra03 = String.Empty;
		public static string strCampoTra04 = String.Empty;
		public static string strCampoTra05 = String.Empty;
        public static string strCampoTra06 = String.Empty;
        public static string strCampoTra07 = String.Empty;
	

		#endregion

		#region Clases
		
		public static ClaseRetencionIslr objRetencionIslr = new ClaseRetencionIslr();
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		
		#endregion

		/// <summary>
		/// Metodo main
		/// </summary>
		[STAThread]
		static void Main()
		{

			try 
			{
				
				// ESCRITURA EN CONSOLA
				Console.WriteLine("	 ");
				Console.WriteLine("  ------------ CARGA RETENCION ISLR - FARMATODO VE (HUB-04) ------------");
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
				objTextFile.EscribirLog(" --> ERROR - LEYENDO ARCHIVO XML DE CONFIGURACION. CODIGO: " + strCodError);
				
			}
			
			// VERIFICA QUE LOS ARGUMENTOS DE ENTRADA NO SEAN VALORES NULOS
			if (strFileName != "")
			{
				try
				{			
					objTextFile.EscribirLog("PARAMETROS DE ENTRADA DEL SISTEMA CARGADOS SATISFACTORIAMENTE.  NOMBRE ARCHIVO: " + strFileName);
			
					// VERIFICA QUE EL ARCHIVO EXISTA
					if (File.Exists(strFileName))
					{						
						//ARCHIVO DE PAGOS
                        objRetencionIslr.InsertRetencionIslr(strFileName);
						objTextFile.EscribirLog("FINALIZO SATISFACTORIAMENTE EL PROCESO DE IMPORT DEL ARCHIVO: " + strFileName);
						
					}
					// EL ARCHIVO NO EXISTE
					else
					{
						strCodError = "99";
						objTextFile.EscribirLog(" --> ERROR - EL ARCHIVO DE ENTRADA: " + strFileName + "  NO EXISTE");					
					}
				}			
				catch (Exception ex) 
				{
					strCodError = ex.Message;
					objTextFile.EscribirLog(" --> ERROR - OCURRIO UN ERROR CON EL ARCHIVO DE ENTRADA. CODIGO: " + strCodError);				
				}
			}
		}
		
		/// <summary>
		/// Lee el archivo XML de configuracion
		/// </summary>
		/// <param name="strUbicacion">Ubicacion archivo XML</param>
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
                        if (reader.GetAttribute(0) == "STRINGFILE")
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
                        if (reader.GetAttribute(0) == "CARACTER")
                        {
                            caracter = Char.Parse(reader.GetAttribute(1));
                        }
						if (reader.GetAttribute(0) == "STRCAMPOENC01")
						{
							strCampoEnc01 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC02")
						{
							strCampoEnc02 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC03")
						{
							strCampoEnc03 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC04")
						{
							strCampoEnc04 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC05")
						{
							strCampoEnc05 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC06")
						{
							strCampoEnc06 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC07")
						{
							strCampoEnc07 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC08")
						{
							strCampoEnc08 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC09")
						{
							strCampoEnc09 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC10")
						{
							strCampoEnc10 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC11")
						{
							strCampoEnc11 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC12")
						{
							strCampoEnc12 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC13")
						{
							strCampoEnc13 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC14")
						{
							strCampoEnc14 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC15")
						{
							strCampoEnc15 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOENC16")
						{
							strCampoEnc16 = reader.GetAttribute(1);
						}
                        if (reader.GetAttribute(0) == "STRCAMPOENC17")
                        {
                            strCampoEnc17 = reader.GetAttribute(1);
                        }
						if (reader.GetAttribute(0) == "STRCAMPOENC18")
                        {
                            strCampoEnc18 = reader.GetAttribute(1);
                        }
                        if (reader.GetAttribute(0) == "STRCAMPOENC19")
                        {
                            strCampoEnc19 = reader.GetAttribute(1);
                        }
                        
						if (reader.GetAttribute(0) == "STRCAMPODET01")
						{
							strCampoDet01 = reader.GetAttribute(1);
						}						
						if (reader.GetAttribute(0) == "STRCAMPODET02")
						{
							strCampoDet02 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET03")
						{
							strCampoDet03 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET04")
						{
							strCampoDet04 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET05")
						{
							strCampoDet05 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET06")
						{
							strCampoDet06 = reader.GetAttribute(1);
						}						
						if (reader.GetAttribute(0) == "STRCAMPODET07")
						{
							strCampoDet07 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET08")
						{
							strCampoDet08 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET09")
						{
							strCampoDet09 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET10")
						{
							strCampoDet10 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET11")
						{
							strCampoDet11 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET12")
						{
							strCampoDet12 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET13")
						{
							strCampoDet13 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET14")
						{
						    strCampoDet14 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET15")
						{
						    strCampoDet15 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET16")
						{
						    strCampoDet16 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET17")
						{
						    strCampoDet17 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET18")
						{
						    strCampoDet18 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPODET19")
						{
						    strCampoDet19 = reader.GetAttribute(1);
						}
						
						if (reader.GetAttribute(0) == "STRCAMPOTRA01")
						{
						    strCampoTra01 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOTRA02")
						{
						    strCampoTra02 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOTRA03")
						{
						    strCampoTra03 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOTRA04")
						{
						    strCampoTra04 = reader.GetAttribute(1);
						}
						if (reader.GetAttribute(0) == "STRCAMPOTRA05")
						{
						    strCampoTra05 = reader.GetAttribute(1);
						}
                        if (reader.GetAttribute(0) == "STRCAMPOTRA06")
                        {
                            strCampoTra06 = reader.GetAttribute(1);
                        }
                        if (reader.GetAttribute(0) == "STRCAMPOTRA07")
                        {
                            strCampoTra07 = reader.GetAttribute(1);
                        }
					}
				}		
			}
			catch(Exception ex)
			{
				strCodError = ex.Message;
				objTextFile.EscribirLog(" --> ERROR - LEYENDO ARCHIVO XML DE CONFIGURACION. CODIGO: " + strCodError);
				// SLEEP DEL SISTEMA
				Thread.Sleep(15000);				
			}
		}
	}
}