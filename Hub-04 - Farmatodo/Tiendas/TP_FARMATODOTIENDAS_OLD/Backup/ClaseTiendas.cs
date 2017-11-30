using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_TIENDAS
{
	/// <summary>
	/// Summary description for ClaseOrdenCompra.
	/// </summary>
	public class ClaseTiendas
	{

		#region Clases
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();

		#endregion

		public ClaseTiendas()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		public void InsertTiendas(string strFileName)
		{

			string query = "";
			string strTempCodLoc = "";
			string strErrorDetalles = "NO";
			string strErrorIntentos = "NO";
			string strError_num_doc_enc = "";
			string strTempCodLocalizacion = "";					
			string strLocalizacion = "";						
			int intReintentos = 4; // REINTENTA UNA VEZ MENOS DE LO ESPECIFICADO
			int intNumeroLinea = 0;
			int intNumeroEncabezado = 0;
			int intReintentosAplicados = 0;
			

			string strDecimal = System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator;
						
			try
			{
				// CREA UNA INSTANCIA DE StreamReader PARA LEER DEL ARCHIVO.
				using (StreamReader sr = new StreamReader(strFileName, System.Text.Encoding.Default)) 
				{

					string strLineaLeida;
					Array arrCamposLinea;

					// VARIABLES ASIGNADAS PARA TRABAJAR
					string strCodLocalizacion = "   ";

					// INCREMENTA EL NUMERO DE LA LINEA
					intNumeroLinea = 1;	

					// INSTANCIA LA CONEXION DE BASE DE DATOS
					SqlDataReader myReader = null;
					SqlConnection mySqlConnection = new SqlConnection();
					SqlCommand mySqlCommand = new SqlCommand();
					mySqlConnection = objBDatos.Conexion();

					// LEE CADA UNA DE LAS LINEAS DEL ARCHIVO
					while ((strLineaLeida = sr.ReadLine()) != null) 
					{												
						//SEPARA CADA UNO DE LOS CAMPOS DE LA LINEA LEIDA
						strLineaLeida = strLineaLeida.Replace("!","");						
						arrCamposLinea = strLineaLeida.Split(';');

						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						//DETERMINA EL TIPO DE REGISTRO ENCABEZADO
						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
													
							string strFecha = Convert.ToString(System.DateTime.Now);

							// ASIGNA VALORES A LAS VARIABLES DEL ENCABEZADO
							strCodLocalizacion = arrCamposLinea.GetValue(0).ToString().TrimStart().TrimEnd();
							strLocalizacion = arrCamposLinea.GetValue(2).ToString().TrimStart().TrimEnd();

							strErrorDetalles = "NO";
												
												
							if(strErrorDetalles == "NO")
							{
								//VERIFICA SI YA EXISTE CODIGO_LOCALIZACION Y LOCALIZACION EN BASE DE DATOS PARA ACTULIZAR 
								query = " SELECT * FROM TradePlace.farmatodo_localidades WHERE codigo_localizacion = '" + arrCamposLinea.GetValue(0).ToString().TrimStart().TrimEnd()+ "' "; // AND localizacion = '" + arrCamposLinea.GetValue(2).ToString().TrimStart().TrimEnd() + "' ";

								// VERIFICA SI LA CONEXION A BD ESTA CERRADA PARA ABRIRLA
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
											objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS");
									
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

								try
								{
									// UPDATE DEL REGISTRO 
									mySqlCommand = objBDatos.Comando(query, mySqlConnection);
									myReader = mySqlCommand.ExecuteReader();
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR: " + Class1.strCodError);
									objTextFile.EscribirLog(" QUERY: " + query);
									strErrorDetalles = "SI";
								}

								while (myReader.Read())
								{
									strTempCodLocalizacion = myReader["cod_localizacion"].ToString();
									//strTempLocalizacion = myReader["localizacion"].ToString();
									break;
								}

								myReader.Close();
								
								// -------------------------------------------------------------------------------------
								// UPDATE DE ENCABEZADO
								// -------------------------------------------------------------------------------------
								if ((strTempCodLocalizacion == arrCamposLinea.GetValue(0).ToString().TrimStart().TrimEnd())) //&& (strTempLocalizacion == arrCamposLinea.GetValue(2).ToString().TrimStart().TrimEnd()))
								{
								
									// GENERA EL QUERY DE UPDATE 

									query = " UPDATE TradePlace.farmatodo_localidades SET ";
									query += " ean = '" + arrCamposLinea.GetValue(1).ToString().TrimStart().TrimEnd().Replace("'", "") + "', ";
									query += " localizacion = '" + arrCamposLinea.GetValue(2).ToString().TrimStart().TrimEnd().Replace("'", "") + "', ";
									query += " direccion = '" + arrCamposLinea.GetValue(3).ToString().TrimStart().TrimEnd().Replace("'", "") + "' ";
									query += " WHERE codigo_localizacion = '" + arrCamposLinea.GetValue(0).ToString().TrimStart().TrimEnd() + "' "; 
									

									// INSERT EN EL LOG LA ACTUALIZACION DE TOTALES DEL ENCABEZADO									
									objTextFile.EscribirLog("ACTUALIZANDO TIENDA COD LOCALIZACION: " + arrCamposLinea.GetValue(0).ToString().TrimStart().TrimEnd());
								
									// VERIFICA SI LA CONEXION A BD ESTA CERRADA PARA ABRIRLA
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
												objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS");
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
								
									try
									{
										// UPDATE DEL REGISTRO DE ENCABEZADO
										mySqlCommand = objBDatos.Comando(query, mySqlConnection);
										mySqlCommand.ExecuteNonQuery();

										strTempCodLoc = strCodLocalizacion;
									}
									catch (Exception ex) 
									{
										Class1.strCodError = ex.Message;
										objTextFile.EscribirLog("  ---> ERROR ACTUALIZANDO TIENDA: " + strCodLocalizacion + "Error:" + Class1.strCodError);
										objTextFile.EscribirLog(" QUERY: " + query);
										strErrorDetalles = "SI";
									}

								
								}//FIN IF UPDATE
								else // SINO SON IGUALES INSERTO
								{
									
									// INCREMENTA EL NUMERO DE ENCABEZADO
									intNumeroEncabezado += 1;
							
									// INCREMENTA EL NUMERO DE LA LINEA
									intNumeroLinea = 1;

									// ASIGNA VALORES A LAS VARIABLES DEL ENCABEZADO
									strCodLocalizacion = arrCamposLinea.GetValue(0).ToString().TrimStart().TrimEnd();

									// VARIABLES ASIGNADAS PARA TRABAJAR
									strErrorDetalles = "NO";
							
									// VERIFICA QUE EL NUMERO DEL DOCUMENTO O EL CODIGO DEL PROVEEDOR NO SEAN NULOS
									if (arrCamposLinea.GetValue(0).ToString().TrimStart().TrimEnd() != "")
									{
																	
											// INICIALIZA LAS VARIABLES
											strErrorDetalles = "NO";

											query = " INSERT INTO TradePlace.farmatodo_localidades ";
											query += " ( codigo_localizacion, ean, localizacion, direccion) ";
											query += " VALUES ";
											query += " ( '" + arrCamposLinea.GetValue(0).ToString().TrimStart().TrimEnd() + "', '" + arrCamposLinea.GetValue(1).ToString().TrimStart().TrimEnd() + "', ";																	
											query += " '" + arrCamposLinea.GetValue(2).ToString().TrimStart().TrimEnd().Replace("'", "") + "', '" + arrCamposLinea.GetValue(3).ToString().TrimStart().TrimEnd().Replace("'", "") + "') ";

											// VERIFICA SI LA CONEXION A BD ESTA CERRADA PARA ABRIRLA
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
														objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS");
														strErrorIntentos = "SI";
													}
													catch (Exception ex) 
													{
														Class1.strCodError = ex.Message;
														objTextFile.EscribirLog("  ---> ERROR -- ERROR ABRIENDO LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados + "  CODIGO: " +  Class1.strCodError);
														intReintentosAplicados += intReintentosAplicados;
														// SLEEP DEL SISTEMA
														Thread.Sleep(15000);
													}
														
												} // FIN DEL WHILE
											} // FIN DEL IF DE LA CONEXION CERRADA							
								
											try
											{
												// INSERTA EL REGISTRO DE ENCABEZADO
												mySqlCommand = objBDatos.Comando(query, mySqlConnection);
												mySqlCommand.ExecuteNonQuery();
												
												objTextFile.EscribirLog("INSERTANDO ENCABEZADO DEL COD LOCALIZACION: " + strCodLocalizacion);
											
									
											}
											catch (Exception ex) 
											{
												Class1.strCodError = ex.Message;
												objTextFile.EscribirLog("  ---> ERROR -- INSERTANDO TIENDA: " + strCodLocalizacion + "ERROR:" + Class1.strCodError);
												objTextFile.EscribirLog(" QUERY: " + query);
												strError_num_doc_enc = arrCamposLinea.GetValue(0).ToString();
												strErrorDetalles = "SI";
											}										
									}
									else
									{
										objTextFile.EscribirLog("  ---> ERROR -- EL CAMPO COD LOCALIZACION NO PUEDE SER UN VALOR NULO.  EL NUMERO DE ENCABEZADO DENTRO DEL ARCHIVO ES: " + intNumeroEncabezado);
										strErrorDetalles = "SI";
									}
									//}
								}
							}
	
					}
				
					// VERIFICA SI LA CONEXION A BD ESTA ABIERTA PARA CERRARLA Y FINALIZAR LA OPERACION DE IMPORT
					if(mySqlConnection.State.ToString() == "Open")
					{
						mySqlConnection.Close();
						objTextFile.EscribirLog("");
						objTextFile.EscribirLog("CERRANDO CONEXION CON LA BASE DE DATOS");
					}
				}
			}
			catch (Exception ex) 
			{
				Class1.strCodError = ex.Message;

				objTextFile.EscribirLog("  ---> ERROR -- ERROR LEYENDO EL ARCHIVO DE ENTRADA: " + strFileName + "  CODIGO: " + Class1.strCodError);
				objTextFile.EscribirLog(" QUERY: " + query);
				
			}
		}		
		
	}
}