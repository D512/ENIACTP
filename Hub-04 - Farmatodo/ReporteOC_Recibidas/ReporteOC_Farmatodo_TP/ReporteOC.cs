using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace ReporteOC
{
	/// <summary>
	/// Summary description for ClaseOrdenCompra.
	/// </summary>
	public class ClaseReporteOC
	{
		
		#region Clases
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		public static Email objEmail = new Email();
		

		#endregion

		public ClaseReporteOC()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		public void InsertReporteOC(string strFileName)
		{

			string query = "";
			string strTempNroDoc = "";	
			string strErrorIntentos = "NO";			
			int intReintentos = 4; // REINTENTA UNA VEZ MENOS DE LO ESPECIFICADO
			int intNumeroLinea = 0;			
			int intReintentosAplicados = 0;
			string strTempNumero_doc = "";
			string strTempCod_prov_hub_tp = "";
			string strTempNombreProveedor= "";
			string strNombreProveedor= "";			
			string strCodigoProveedorHub = "";		
			string strTempCodigoProveedorHub = "";
			

			//para el correo
			string strBody="";
			string strEnvioCorreo="NO";
			string strEmail="";
			string strEmailCuerpo="";			


			string strDecimal = System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator;
						
			try
			{
				// CREA UNA INSTANCIA DE StreamReader PARA LEER DEL ARCHIVO.
				using (StreamReader sr = new StreamReader(strFileName, System.Text.Encoding.Default)) 
				{

					string strLineaLeida;
					Array arrCamposLinea;

					// VARIABLES ASIGNADAS PARA TRABAJAR
					string strNumeroDoc = "   ";
					
					// INSTANCIA LA CONEXION DE BASE DE DATOS
					SqlDataReader myReader = null;
					SqlConnection mySqlConnection = new SqlConnection();
					SqlCommand mySqlCommand = new SqlCommand();
					mySqlConnection = objBDatos.Conexion();

					// LEE CADA UNA DE LAS LINEAS DEL ARCHIVO
					while ((strLineaLeida = sr.ReadLine()) != null) 
					{
						// INCREMENTA EL NUMERO DE LA LINEA
						intNumeroLinea += 1;

                        //ELIMINAMOS CARACTERES ESPECIALES
                        strLineaLeida = strLineaLeida.Replace("`", "");
                        //SEPARA CADA UNO DE LOS CAMPOS DE LA LINEA LEIDA						
						arrCamposLinea = strLineaLeida.Split('|');                        
				

							// ASIGNA VALORES A LAS VARIABLES DEL ENCABEZADO strNumeroDoc, strCodigoProveedorHub y strNombreProveedor
							try
							{
								strNumeroDoc = arrCamposLinea.GetValue(0).ToString().Replace("'", "").Trim();
                                strCodigoProveedorHub = arrCamposLinea.GetValue(1).ToString().Replace("'", "").Trim();
                                strNombreProveedor = arrCamposLinea.GetValue(2).ToString().Replace("'", "").Trim();
									
								if (strNumeroDoc == "")
								{
									objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(1) NUMERO DE ORDEN " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);									
								}

							}
                            catch (Exception ex1)
							{                                
                                Class1.strCodError = ex1.Message;
                                objTextFile.EscribirLog("  ---> ERROR CODIGO: " + Class1.strCodError + ".LINEA: " + intNumeroLinea);	
							}	
						

							string strFecha = Convert.ToString(System.DateTime.Now);
							
						
							#region //VERIFICA LA ORDENES EN BASE DE DATOS					
								
							query = " SELECT * FROM " + Class1.strTableName01 + " ";
							query += " WHERE numero_doc = '" + strNumeroDoc + "' ";
							query += " AND cod_prov_hub_tp = '" + strCodigoProveedorHub + "' ";
								
							#region VERIFICA SI LA CONEXION A BD ESTA CERRADA PARA ABRIRLA
								
							if(mySqlConnection.State.ToString() == "Closed")
							{

								// TRATAR DE HACER UN REINTENTO DE CONEXION A BASE DE DATOS.
								intReintentosAplicados = 1;
								strErrorIntentos = "NO";
									
								while ((intReintentosAplicados) == intReintentos || (strErrorIntentos != "SI"))
								{
									try
									{
										mySqlConnection.Open();
										//objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS");
										strErrorIntentos = "SI";
									}
									catch (Exception ex) 
									{
										Class1.strCodError = ex.Message;											
										objTextFile.EscribirLog("  ---> ERROR -- ERROR ABRIENDO LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados + " CODIGO: " + Class1.strCodError);

										intReintentosAplicados += intReintentosAplicados;
										// SLEEP DEL SISTEMA
										Thread.Sleep(15000);
									}
								} // FIN DEL WHILE
							} // FIN DEL IF DE LA CONEXION CERRADA
							#endregion
								
							try
							{
								mySqlCommand = objBDatos.Comando(query, mySqlConnection);
								myReader = mySqlCommand.ExecuteReader();
							}
							catch (Exception ex) 
							{
								Class1.strCodError = ex.Message;															
								objTextFile.EscribirLog("  ---> ERROR SELECT  ENCABEZADO. CODIGO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
								objTextFile.EscribirLog("  ---> ERROR -- QUERY: " + query);								
							}

							while (myReader.Read())
							{
								strTempNumero_doc = myReader["numero_doc"].ToString();
								strTempCod_prov_hub_tp = myReader["cod_prov_hub_tp"].ToString();
								break;
							}

							myReader.Close();

							#endregion                            
							
							// EVALUA SI EL REGISTRO EXISTE							
							if ((strTempNumero_doc == strNumeroDoc) && (strTempCod_prov_hub_tp == strCodigoProveedorHub))
							{
								// INSERT EN EL LOG LA ACTUALIZACION DE TOTALES DEL ENCABEZADO
								objTextFile.EscribirLog("REGISTRO DE ENCABEZADO YA EXISTE PARA EL DOCUMENTO NUMERO: " + arrCamposLinea.GetValue(0) + " DEL PROVEEDOR: " + arrCamposLinea.GetValue(1).ToString());
								

							}//FIN IF UPDATE
							else
							{
                                objTextFile.EscribirLog("  ---> ERROR ORDEN DE COMPRA: " + strNumeroDoc + " del Proveedor: " + strNombreProveedor + " NO SE HA RECIBIDO. LINEA: " + intNumeroLinea);
								if(strEmailCuerpo=="")
                                {
                                    strEmailCuerpo += "<tr><td align=center><strong>Numero Orden</strong></td><td align=center><strong>Proveedor</strong></td></tr>";
                                }
                                strEmailCuerpo+= "<tr>";
                                strEmailCuerpo += "<td align=center> " + strNumeroDoc.ToString().PadRight(15, ' ') + "</td><td>" + strNombreProveedor.ToString().PadRight(70, ' ') + "</td></tr>";
								
								strEnvioCorreo="SI";			

							}// fin de registro existe
					

							strTempNroDoc = strNumeroDoc;
							strTempCodigoProveedorHub = strCodigoProveedorHub;
							strTempNombreProveedor= strNombreProveedor;				
				

					}// fin while
					

					#region ULTIMO CORREO DE ERRORES DE CARGA FACTURA ELECTRONICA
							

					if((strEnvioCorreo=="SI"))
					{

							strEmail = Class1.strEmailCC;
						

						if (strEmail!="" && strEmailCuerpo!="")
						{
							try
							{
								strBody= "<center><strong>ALERTA!! ORDENES DE COMPRA NO RECIBIDAS EN TRADEPLACE</strong></center>";
								strBody+="   <br><br>";					
								strBody+="Ordenes generadas por: <strong>" + Class1.strCliente + "</strong><br>";
								strBody+="   <br>";
								strBody+= " <table border=1>" +strEmailCuerpo + "</table>";
                                strBody += "   <br><br>";
                                strBody += "<font color=red><strong>Por favor realizar la transferencia a TRADEPLACE</strong></font>";

							

								//bool OK = objEmail.EnviarEmail("mailing@tradeplace.net",strEmail, "" ,"Errores Carga Factura", System.Web.Mail.MailFormat.Html,strBody, "mailfe.eniac.com.ve");
                                bool OK = objEmail.EnviarEmail("mailing@tradeplace.net", strEmail, Class1.strEmailCC, "Ordenes de Compra NO Recibidas en TradePlace", strBody, Class1.strServer);
                                if (OK)
								{
									objTextFile.EscribirLog("--> EMAIL ERRORES DE CARGA ORDEN DE COMPRA EXITOSO... ");
																		
								}
							
							}
							catch (Exception ex1)
							{
								Class1.strCodError = ex1.Message;
                                objTextFile.EscribirLog("  ---> ERROR -- ERROR ENVIANDO EL EMAIL ERRORES DE CARGA ORDEN DE COMPRA.  CODIGO: " + Class1.strCodError);								
							}
						}
					}

					#endregion

                    

					// VERIFICA SI LA CONEXION A BD ESTA ABIERTA PARA CERRARLA Y FINALIZAR LA OPERACION DE IMPORT
					if(mySqlConnection.State.ToString() == "Open")
					{
						mySqlConnection.Close();
						//objTextFile.EscribirLog("");
						objTextFile.EscribirLog("CERRANDO CONEXION CON LA BASE DE DATOS");
					}
				}
			}
			catch (Exception ex) 
			{
				Class1.strCodError = ex.Message;

				objTextFile.EscribirLog("  ---> ERROR -- ERROR LEYENDO EL ARCHIVO DE ENTRADA: " + strFileName + "  CODIGO: " + Class1.strCodError);
				
			}
		}
		
	}
}
