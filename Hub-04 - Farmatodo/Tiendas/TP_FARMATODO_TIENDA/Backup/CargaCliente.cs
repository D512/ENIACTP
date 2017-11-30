using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_CargaCliente
{
	/// <summary>
	/// Summary description for ClaseOrdenCompra.
	/// </summary>
	public class ClaseCargaCliente
	{
		
		#region Clases
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();

		#endregion

		public ClaseCargaCliente()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		public void InsertCargaCliente(string strFileName)
		{

			string query = "";
			string strErrorIntentos = "NO";
			string ErrorTipoRegistro = "NO";
			int intReintentos = 4; // REINTENTA UNA VEZ MENOS DE LO ESPECIFICADO
			int intNumeroLinea = 0;
			int intReintentosAplicados = 0;
			string strGlnSucursal = "";
			string strCodigoAsesor = "";
			string strCodigoCliente="";
			string strFaltaCamposEncabezado = "NO";
			string strTempGlnSucursal = "";
			

			string strDecimal = System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator;
						
			try
			{


				// CREA UNA INSTANCIA DE StreamReader PARA LEER DEL ARCHIVO.
				using (StreamReader sr = new StreamReader(strFileName, System.Text.Encoding.Default)) 
				{

					string strLineaLeida;
					Array arrCamposLinea;

					
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


						//SEPARA CADA UNO DE LOS CAMPOS DE LA LINEA LEIDA
						strLineaLeida = strLineaLeida.Replace(";","|");
						arrCamposLinea = strLineaLeida.Split('|');



						string strFecha = Convert.ToString(System.DateTime.Now);
						ErrorTipoRegistro = "NO";
						strFaltaCamposEncabezado = "NO";
												
	

						#region VALIDA CAMPOS REQUERIDOS

						try
						{

			
							if (arrCamposLinea.GetValue(0).ToString().Trim() == "")
							{
								objTextFile.EscribirLog("---> ERROR SE REQUIERE VALOR PARA EL CAMPO(1) GLN SUCURSAL, LINEA: " + intNumeroLinea);
								strFaltaCamposEncabezado = "SI";
								ErrorTipoRegistro = "SI";
																		
							}

							if (arrCamposLinea.GetValue(1).ToString().Trim() == "")
							{
								objTextFile.EscribirLog("---> ERROR SE REQUIERE VALOR PARA EL CAMPO(2) COD. ASESOR, LINEA: " + intNumeroLinea);
								strFaltaCamposEncabezado="SI";
								ErrorTipoRegistro = "SI";
									
							}

							if(arrCamposLinea.GetValue(2).ToString().Trim() == "")
							{
								objTextFile.EscribirLog("---> ERROR SE REQUIERE VALOR PARA EL CAMPO(3) COD. CLIENTE SUCURSAL, LINEA: " + intNumeroLinea);
								strFaltaCamposEncabezado="SI";
								ErrorTipoRegistro = "SI";
									
							}
													
																
						}
						catch (Exception ex) 
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR LEYENDO ARCHIVO CODIGO:" + Class1.strCodError + " LINEA: " + intNumeroLinea);
							strFaltaCamposEncabezado="SI";
							ErrorTipoRegistro = "SI";
						}


						#endregion
							
						
							
						if(strFaltaCamposEncabezado == "NO")
						{

							strGlnSucursal = arrCamposLinea.GetValue(0).ToString().Trim();
							strCodigoAsesor = arrCamposLinea.GetValue(1).ToString().Trim();
							strCodigoCliente = arrCamposLinea.GetValue(2).ToString().Trim();

							
							query = " SELECT * FROM " + Class1.strTableName01 + " ";
							query += " WHERE GLN_sucursal = '" + strGlnSucursal + "' ";
																
									
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
								// UPDATE DEL REGISTRO DE ENCABEZADO
								mySqlCommand = objBDatos.Comando(query, mySqlConnection);
								myReader = mySqlCommand.ExecuteReader();
										
							}
							catch (Exception ex) 
							{
								Class1.strCodError = ex.Message;
								objTextFile.EscribirLog("  ---> ERROR LEYENDO EL ENCABEZADO CODIGO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
								objTextFile.EscribirLog("  ---> ERROR -- QUERY: " + query);
							}
									

							while (myReader.Read())
							{
								strTempGlnSucursal = myReader["GLN_sucursal"].ToString();
								break;
							}

							myReader.Close();

							if 	(strTempGlnSucursal == strGlnSucursal)
							{										
										
								query = "UPDATE " + Class1.strTableName01 + " SET ";							
								query += "codigo_asesor='" + strCodigoAsesor + "', ";
								query += "codigo_cliente=" + strCodigoCliente + ", ";
								query += "fecha_status=CONVERT(DATETIME, GETDATE()) ";
								query += "WHERE GLN_sucursal='" + strTempGlnSucursal + "' ";


								// VERIFICA SI LA CONEXION A BD ESTA CERRADA PARA ABRIRLA
								if (mySqlConnection.State.ToString() == "Closed")
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
											objTextFile.EscribirLog("  ---> ERROR -- ERROR ABRIENDO LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados + "  CODIGO: " + Class1.strCodError);
											intReintentosAplicados += intReintentosAplicados;
											// SLEEP DEL SISTEMA
											Thread.Sleep(15000);
										}

									} // FIN DEL WHILE

								} // FIN DEL IF DE LA CONEXION CERRADA							

								try
								{
									// MODIFICA EL REGISTRO DE ENCABEZADO
									mySqlCommand = objBDatos.Comando(query, mySqlConnection);
									mySqlCommand.ExecuteNonQuery();
									//objTextFile.EscribirLog("ACTUALIZANDO REGISTROS DEL PROVEEDOR: " + GLN_sucursal + " LINEA: " + intNumeroLinea);


								}
								catch (Exception ex)
								{
									Class1.strCodError = ex.Message;
									strFaltaCamposEncabezado = "SI";
									objTextFile.EscribirLog("  ---> ERROR -- ACTUALIZANDO REGISTROS: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									objTextFile.EscribirLog(" QUERY: " + query);
								}
							
							
							
							}
							else
							{

								query = " INSERT INTO " + Class1.strTableName01 + " (GLN_sucursal, codigo_asesor,  codigo_cliente,fecha_status) "; 
								query += " VALUES "; 
								query += " ('" + strGlnSucursal + "', '" + strCodigoAsesor + "', '" + strCodigoCliente + "',CONVERT(DATETIME, GETDATE())) "; 
								

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
											objTextFile.EscribirLog("  ---> ERROR -- ERROR ABRIENDO LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados + "  CODIGO: " +  Class1.strCodError);

											intReintentosAplicados += intReintentosAplicados;
											// SLEEP DEL SISTEMA
											Thread.Sleep(15000);
										}
									} // FIN DEL WHILE
								} // FIN DEL IF DE LA CONEXION CERRADA							
								#endregion

								try
								{
									// INSERTA EL REGISTRO DE ENCABEZADO
									mySqlCommand = objBDatos.Comando(query, mySqlConnection);
									mySqlCommand.ExecuteNonQuery();		
									
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									objTextFile.EscribirLog("  ---> ERROR -- QUERY: " + query);
									ErrorTipoRegistro = "SI";

								}

							}

						}//fin (strFaltaCamposEncabezado == "NO")


					}// fin while

			
					
					if(	ErrorTipoRegistro != "SI")
					{
						// INSERT EN EL LOG 
						objTextFile.EscribirLog("SE INSERTARON Y/O MODIFICARON " + (intNumeroLinea) + " REGISTROS" );
			
					}


				}
				
			}
			catch (Exception ex) 
			{
				Class1.strCodError = ex.Message;

				objTextFile.EscribirLog("  ---> ERROR -- ERROR LEYENDO EL ARCHIVO DE ENTRADA: " + strFileName + "  CODIGO: " + Class1.strCodError +" LINEA: " + intNumeroLinea);
				
			}
		}

	}//FIN FUNCION

}
