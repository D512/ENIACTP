using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_Pago_Farmatodo
{
	/// <summary>
	/// Summary description for ClasePagos.
	/// </summary>
	public class ClasePagos
	{


		#region Clases
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		#endregion


		public ClasePagos()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		public void InsertPagos(string strFileName)
		{


			string query = "";			
			string strFecha_Doc;
			string strFecha_Pago ;
			string strTempNroDoc = "";
			string strErrorIntentos = "NO";
			string strNombre_Banco = "";
			string strNombre_TipoPago = "";
			string strNombre_BancoReceptor = "";
			string strNombre_Tienda = "";
			string strTempCod_prov_hub_tp = "";
			string strTempId_pago = "";
			string strTempNumero_doc = "";
			string strCod_Localizacion = "";
			string strErrorDetalle = "";
			double dobMonto_pago = 0;
			double dobMonto_documento = 0;
			double dobMonto_gravable = 0;
			double dobMonto_excento = 0;
			double dobMonto_impuesto = 0;
			double dobMonto_iva = 0;
			double dobMonto_islr = 0;
			double dobMonto_ret = 0;

			int intReintentos = 4; // REINTENTA UNA VEZ MENOS DE LO ESPECIFICADO
			int intNumeroLinea = 0;
			int intErrorCarga = 0;
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
					string strCodigoProveedorHub = "";
					string strNumeroDoc = "   ";


					// INSTANCIA LA CONEXION DE BASE DE DATOS
					SqlDataReader myReader = null;
					SqlConnection mySqlConnection = new SqlConnection();
					SqlCommand mySqlCommand = new SqlCommand();
					mySqlConnection = objBDatos.Conexion();
					SqlDataReader cursor;

					// PRIMERO HACE DELETE DE TODOS AQUELLOS PAGOS MAYORES DE 45 DIAS A LA FECHA ACTUAL
					//DeletePago (mySqlConnection);

					// LEE CADA UNA DE LAS LINEAS DEL ARCHIVO
					while ((strLineaLeida = sr.ReadLine()) != null) 
					{
						
						//SEPARA CADA UNO DE LOS CAMPOS DE LA LINEA LEIDA
						strLineaLeida = strLineaLeida.Replace("!","");
						arrCamposLinea = strLineaLeida.Split(';');
						
						strErrorDetalle = "NO";
						strNombre_Tienda = "";
						strNombre_Banco = "";
						strNombre_BancoReceptor = "";
						strTempCod_prov_hub_tp = "";
						strTempId_pago = "";
						strTempNumero_doc = "";
						strNombre_TipoPago = "";
						dobMonto_pago= 0;
						dobMonto_documento = 0;
						dobMonto_gravable = 0;
						dobMonto_excento = 0;
						dobMonto_impuesto = 0;
						dobMonto_iva = 0;
						dobMonto_islr = 0;
						dobMonto_ret = 0;

						// ASIGNA VALORES A LAS VARIABLES DEL ENCABEZADO
						strCodigoProveedorHub = arrCamposLinea.GetValue(0).ToString().Trim();
						strNumeroDoc = arrCamposLinea.GetValue(4).ToString().Trim();

						query = "SELECT * FROM farmatodo_cod_prov where cod_prov_hub_tp_nuevo = '" + arrCamposLinea.GetValue(0).ToString().Replace("'","").Trim() + "'";
						try
						{
							mySqlCommand=objBDatos.Comando(query,mySqlConnection);
							cursor=mySqlCommand.ExecuteReader();
							while(cursor.Read())
							{
								strCodigoProveedorHub = cursor["cod_prov_hub_tp"].ToString().Trim();
								//strNombre_Proveedor = cursor["nombre_proveedor"].ToString().Trim();
							}
							cursor.Close();
						}
						catch(Exception e)
						{
							Class1.objTextFile.EscribirLog("  ---> ERROR REALIZANDO EL QUERY QUE VERIFICA LA EXISTENCIA DEL COD_PROV_HUB_TP VIEJO EN LA BD");
							Class1.objTextFile.EscribirLog("     ---> ERROR :" + e.Message);
						}	

						if(arrCamposLinea.GetValue(7).ToString().TrimStart().TrimEnd() == "" )
						{
							strFecha_Doc = "NULL";
						}
						else
						{
							strFecha_Doc = arrCamposLinea.GetValue(7).ToString().Substring(0,4) + arrCamposLinea.GetValue(7).ToString().Substring(4,2) + arrCamposLinea.GetValue(7).ToString().Substring(6,2);
						}

						if(arrCamposLinea.GetValue(1).ToString().TrimStart().TrimEnd() == "" )
						{
							strFecha_Pago = "NULL";
						}
						else
						{
							strFecha_Pago = arrCamposLinea.GetValue(1).ToString().Substring(0,4) + arrCamposLinea.GetValue(1).ToString().Substring(4,2) + arrCamposLinea.GetValue(1).ToString().Substring(6,2);
						}


						try
						{
							if(arrCamposLinea.GetValue(3).ToString().TrimStart().TrimEnd() == "")
							{
								dobMonto_pago = 0;
							}
							else
							{
								dobMonto_pago = Convert.ToDouble(arrCamposLinea.GetValue(3));
							}
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO PAGO: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						try
						{
							if(arrCamposLinea.GetValue(11).ToString().TrimStart().TrimEnd() == "")
							{
								dobMonto_gravable = 0;
							}
							else
							{
								dobMonto_gravable = Convert.ToDouble(arrCamposLinea.GetValue(11));
							}							
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO GRAVABLE: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						try
						{
							if(arrCamposLinea.GetValue(12).ToString().TrimStart().TrimEnd() == "")
							{
								dobMonto_excento = 0;
							}
							else
							{
								dobMonto_excento = Convert.ToDouble(arrCamposLinea.GetValue(12));
							}							
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO EXCENTO: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						try
						{
							if(arrCamposLinea.GetValue(13).ToString().TrimStart().TrimEnd() == "")
							{
								dobMonto_impuesto = 0;
							}
							else
							{
								dobMonto_impuesto = Convert.ToDouble(arrCamposLinea.GetValue(13));
							}							
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO IMPUESTO: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						try
						{
							if(arrCamposLinea.GetValue(14).ToString().TrimStart().TrimEnd() == "")
							{
								dobMonto_iva = 0;
							}
							else
							{
								dobMonto_iva = Convert.ToDouble(arrCamposLinea.GetValue(14));
							}							
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO IVA: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						try
						{
							if(arrCamposLinea.GetValue(15).ToString().TrimStart().TrimEnd() == "")
							{
								dobMonto_islr = 0;
							}
							else
							{
								dobMonto_islr = Convert.ToDouble(arrCamposLinea.GetValue(15));
							}							
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO ISLR: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						try
						{
							if(arrCamposLinea.GetValue(16).ToString().TrimStart().TrimEnd() == "")
							{
								dobMonto_ret = 0;
							}
							else
							{
								dobMonto_ret = Convert.ToDouble(arrCamposLinea.GetValue(16));
							}							
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO RET: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						try
						{
							dobMonto_documento = Convert.ToDouble(arrCamposLinea.GetValue(5));
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO DOCUMENTO: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						if (arrCamposLinea.GetValue(6).ToString().Trim() == "")
						{
							strCod_Localizacion = "000";
							strNombre_Tienda = "Administración";
						}
						else
						{
							strCod_Localizacion = arrCamposLinea.GetValue(6).ToString().Trim();
						}

						if (arrCamposLinea.GetValue(8).ToString().Trim() != "")//Tipo de Pago
						{
							switch (arrCamposLinea.GetValue(8).ToString().Trim())
							{
								case "10" : strNombre_TipoPago = "Transferencia";
									break;
								case "20" : strNombre_TipoPago  = "Cheque";
									break;
							}
						}

						//CAMBIA EL CODIGO DE PROVEEDOR NUEVO POR EL CODIGO VIEJO::::::::::::::::::::::.
						query = "SELECT * FROM farmatodo_cod_prov where cod_prov_hub_tp_nuevo = '" + arrCamposLinea.GetValue(0).ToString().Replace("'","").Trim() + "'";
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
							mySqlCommand = objBDatos.Comando(query, mySqlConnection);
							myReader = mySqlCommand.ExecuteReader();
						}
						catch (Exception ex) 
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR REALIZANDO EL QUERY QUE VERIFICA LA EXISTENCIA DEL COD_PROV_HUB_TP VIEJO EN LA BD: " + Class1.strCodError);
						}

						while (myReader.Read())
						{
							strCodigoProveedorHub = myReader["cod_prov_hub_tp"].ToString().Trim();
							break;
						}

						myReader.Close();						
						// FIN CAMBIA EL CODIGO DE PROVEEDOR NUEVO POR EL CODIGO VIEJO::::::::::::::::..

						
						//BUSCA NOMBRE DE BANCO EMISOR SEGUN CODIGO SWIFT:::::::::::::::::::::::::::::::..
						if(arrCamposLinea.GetValue(9).ToString().TrimStart().TrimEnd()!= "")
						{
							query = " SELECT codigo_swift, nombre_banco ";
							query += " FROM codigos_bancos ";
							query += " WHERE codigo_swift  = '" + arrCamposLinea.GetValue(9).ToString().Replace("'", "").Trim() + "' ";

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
								mySqlCommand = objBDatos.Comando(query, mySqlConnection);
								myReader = mySqlCommand.ExecuteReader();
							}
							catch (Exception ex) 
							{
								Class1.strCodError = ex.Message;
								objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError);
							}

							while (myReader.Read())
							{
								strNombre_Banco = myReader["nombre_banco"].ToString().Trim();
								break;
							}

							myReader.Close();
						}
						//FIN BUSCA NOMBRE DE BANCO EMISOR

						//BUSCA NOMBRE DE BANCO RECEPTOR SEGUN CODIGO SWIFT:::::::::::::::::::::::::::::::..
						if(arrCamposLinea.GetValue(10).ToString().TrimStart().TrimEnd() != "")
						{
							query = " SELECT codigo_swift, nombre_banco ";
							query += " FROM codigos_bancos ";
							query += " WHERE codigo_swift  = '" + arrCamposLinea.GetValue(10).ToString().Replace("'", "").Trim() + "' ";

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
								mySqlCommand = objBDatos.Comando(query, mySqlConnection);
								myReader = mySqlCommand.ExecuteReader();
							}
							catch (Exception ex) 
							{
								Class1.strCodError = ex.Message;
								objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError);
							}

							while (myReader.Read())
							{
								strNombre_BancoReceptor = myReader["nombre_banco"].ToString().Trim();
								break;
							}

							myReader.Close();
						}
						// FIN BUSCA NOMBRE DE BANCO RECEPTOR 

						//BUSCA NOMBRE DE TIENDA SEGUN CODIGO::::::::::::::::::::::::::::::::::
						if (strCod_Localizacion != "000")
						{
							query = " SELECT * ";
							query += " FROM farmatodo_localidades ";
							query += " WHERE codigo_localizacion = '" + strCod_Localizacion.Replace("'", "").Trim() + "' ";

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
								mySqlCommand = objBDatos.Comando(query, mySqlConnection);
								myReader = mySqlCommand.ExecuteReader();
							}
							catch (Exception ex) 
							{
								Class1.strCodError = ex.Message;
								objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError);
							}

							while (myReader.Read())
							{
								strNombre_Tienda = myReader["localizacion"].ToString();
								break;
							}

							myReader.Close();
						
						}
						

						// UPDATE numero_documento=';!4;!' and cod_prov_hub_tp=';!0;!' and id_pago=';!2;!'
						if (arrCamposLinea.GetValue(0).ToString().Trim() != "" && arrCamposLinea.GetValue(2).ToString().Trim() != "" && arrCamposLinea.GetValue(4).ToString().Trim() != "")
						{
							query = " SELECT * FROM farmatodo_detalles_pagos WHERE ";
							query += " numero_documento = '" + arrCamposLinea.GetValue(4).ToString().Replace("'", "").Trim() + "' ";
							query += " and cod_prov_hub_tp = '" + strCodigoProveedorHub.ToString().Replace("'", "").Trim() + "' ";
							query += " and id_pago = '" + arrCamposLinea.GetValue(2).ToString().Replace("'", "").Trim() + "' ";

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
								mySqlCommand = objBDatos.Comando(query, mySqlConnection);
								myReader = mySqlCommand.ExecuteReader();
							}
							catch (Exception ex) 
							{
								Class1.strCodError = ex.Message;
								objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError);
								objTextFile.EscribirLog("  ---> ERROR -- ERROR QUERY: " + query);
							}

							while (myReader.Read())
							{
								strTempCod_prov_hub_tp = myReader["cod_prov_hub_tp"].ToString();
								strTempId_pago = myReader["id_pago"].ToString();
								strTempNumero_doc = myReader["numero_documento"].ToString();
								break;
							}

							myReader.Close();
///////////////////////////////////////////

							if (strErrorDetalle == "NO")
							{
								if (strTempCod_prov_hub_tp != "" && strTempId_pago != "" && strTempNumero_doc != "")
								{
									// GENERA DE UPDATE
									query = " UPDATE farmatodo_detalles_pagos SET ";
									query += " hub_tp = 'HUB-04', ";									
									if(strFecha_Pago == "NULL")
									{
										query += " fecha_pago = CONVERT(DATETIME, " + strFecha_Pago + "), ";
									}
									else
									{
										query += " fecha_pago = CONVERT(DATETIME, '" + strFecha_Pago + "'), ";
									}
									query += " monto_pago = " + dobMonto_pago + ", ";									
									query += " monto_documento = " + dobMonto_documento + ", ";
									if(strFecha_Doc == "NULL")
									{
										query += " fecha_documento = CONVERT(DATETIME, " + strFecha_Doc + "), ";
									}
									else
									{
										query += " fecha_documento = CONVERT(DATETIME, '" + strFecha_Doc + "'), ";
									}
									query += " codigo_localizacion = '" + strCod_Localizacion.Replace("'", "").Trim() + "', ";
									query += " nombre_tienda = '" + strNombre_Tienda.Replace("'", "").Trim() + "', ";
									query += " tipo_pago = '" + arrCamposLinea.GetValue(8).ToString().Replace("'", "").Trim() + "', ";
									query += " nombre_tpago= '" + strNombre_TipoPago.ToString().Replace("'", "").Trim() + "', ";
									query += " banco_emisor = '" + arrCamposLinea.GetValue(9).ToString().Replace("'", "").Trim() + "', ";
									query += " nombre_bancoemisor = '" + strNombre_Banco.Replace("'", "").Trim() + "', ";
									query += " banco_receptor = '" + arrCamposLinea.GetValue(10).ToString().Replace("'", "").Trim() + "', ";
									query += " nombre_bancoreceptor = '" + strNombre_BancoReceptor.Replace("'", "").Trim() + "', ";
									query += " retiro_oficina = '" + arrCamposLinea.GetValue(17).ToString().Replace("'", "").Trim() + "', ";
									query += " monto_gravable = " + dobMonto_gravable + ", ";
									query += " monto_excento = " + dobMonto_excento + ", ";
									query += " monto_impuesto = " + dobMonto_impuesto + ", ";
									query += " monto_iva = " + dobMonto_iva + ", ";
									query += " monto_islr = " + dobMonto_islr + ", ";
									query += " monto_ret = " + dobMonto_ret + " ";
									query += " WHERE cod_prov_hub_tp = '" + strCodigoProveedorHub.ToString().Replace("'", "").Trim() + "' ";
									query += " AND id_pago = '" + arrCamposLinea.GetValue(2).ToString().Replace("'", "").Trim() + "' ";
									query += " AND numero_documento = '" + arrCamposLinea.GetValue(4).ToString().Replace("'", "").Trim() + "' ";

								
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
										// INCREMENTA EL NUMERO DE LA LINEA
										intNumeroLinea += 1;

										strTempNroDoc = strNumeroDoc;
									}
									catch (Exception ex)
									{
										Class1.strCodError = ex.Message;
										objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError);
										intErrorCarga += 1;
									}


								}
								else
								{
									if (arrCamposLinea.GetValue(0).ToString() != "")
									{	
														
										// INCREMENTA EL NUMERO DE ENCABEZADO
										intNumeroEncabezado += 1;
								
										// INCREMENTA EL NUMERO DE LA LINEA
										intNumeroLinea += 1;
																		
										// VERIDICA QUE EL NUMERO DEL DOCUMENTO O EL CODIGO DEL PROVEEDOR NO SEAN NULOS
										if (strNumeroDoc != "")
										{
											if (strCodigoProveedorHub != "")
											{

												query = " INSERT INTO farmatodo_detalles_pagos ";
												query += " (hub_tp, cod_prov_hub_tp, status_pago, fecha_pago, id_pago, monto_pago, numero_documento, monto_documento, fecha_documento, codigo_localizacion, nombre_tienda, ";
												query += " tipo_pago, nombre_tpago, banco_emisor, nombre_bancoemisor, banco_receptor, nombre_bancoreceptor, retiro_oficina, monto_gravable, monto_excento, monto_impuesto, monto_iva, monto_islr, monto_ret, enviar_correo) ";
												query += " VALUES ";
												query += " ('HUB-04', '" + strCodigoProveedorHub.ToString().Replace("'", "").Trim() + "', '1', ";
												if(strFecha_Pago == "NULL")
												{
													query += " CONVERT(DATETIME, " + strFecha_Pago + "), ";
												}
												else
												{
													query += " CONVERT(DATETIME, '" + strFecha_Pago + "'), ";
												}
												query += " '" + arrCamposLinea.GetValue(2).ToString().Replace("'", "").Trim() + "', " + dobMonto_pago + ", '" + arrCamposLinea.GetValue(4).ToString().Replace("'", "").Trim() + "', " + dobMonto_documento + ", ";
												if(strFecha_Doc == "NULL")
												{
													query += " CONVERT(DATETIME, " + strFecha_Doc + "), ";
												}
												else
												{
													query += " CONVERT(DATETIME, '" + strFecha_Doc + "'), ";
												}
												query += " '" + strCod_Localizacion.Replace("'", "").Trim() + "', '" + strNombre_Tienda.Replace("'", "") + "', ";
												query += " '" + arrCamposLinea.GetValue(8).ToString().Replace("'", "").Trim() + "', '" + strNombre_TipoPago.ToString().Replace("'", "").Trim() + "', '" + arrCamposLinea.GetValue(9).ToString().Replace("'", "").Trim() + "', '" + strNombre_Banco.Replace("'", "").Trim() + "', '" + arrCamposLinea.GetValue(10).ToString().Replace("'", "").Trim() + "', '" + strNombre_BancoReceptor.Replace("'", "").Trim() + "', ";
												query += " '" + arrCamposLinea.GetValue(17).ToString().Replace("'", "").Trim() + "', " + dobMonto_gravable + ", " + dobMonto_excento + ", " + dobMonto_impuesto + ", " + dobMonto_iva + ", " + dobMonto_islr + ", " + dobMonto_ret + ", '1') ";

										
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
													// INSERTA EL REGISTRO DE DETALLE
													mySqlCommand = objBDatos.Comando(query, mySqlConnection);
													mySqlCommand.ExecuteNonQuery();		

													strTempNroDoc = strNumeroDoc;
												}
												catch (Exception ex) 
												{
													Class1.strCodError = ex.Message;
													intErrorCarga += 1;
											
													// VERIFICA QUE EL ERROR DE BASE DE DATOS ES POR DUPLICIDAD DEL KEY
													if(Class1.strCodError.Substring(0,41) == "Cannot insert duplicate key row in object")
													{
												
														objTextFile.EscribirLog("  ---> ERROR -- EL REGISTRO DE ENCABEZADO YA EXISTE EN LA BASE DE DATOS.  DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);

												
														try
														{
															//strFecha_Pago = arrCamposLinea.GetValue(1).ToString().Substring(0,4) + arrCamposLinea.GetValue(1).ToString().Substring(4,2) + arrCamposLinea.GetValue(1).ToString().Substring(6,2);

															if(arrCamposLinea.GetValue(1).ToString().TrimStart().TrimEnd() == "" )
															{
																strFecha_Pago = "NULL";
															}
															else
															{
																strFecha_Pago = arrCamposLinea.GetValue(1).ToString().Substring(0,4) + arrCamposLinea.GetValue(1).ToString().Substring(4,2) + arrCamposLinea.GetValue(1).ToString().Substring(6,2);
															}


															// GENERA QUERY DE UPDATE
															query = " UPDATE farmatodo_detalles_pagos SET ";
															query += " hub_tp = 'HUB-04', ";
															query += " monto_documento = '" + arrCamposLinea.GetValue(5).ToString().Replace("'", "") + "', ";
															query += " codigo_localizacion = '" + arrCamposLinea.GetValue(7).ToString().Replace("'", "''") + "', ";
															if(strFecha_Doc == "NULL")
															{
																query += " fecha_documento = CONVERT(DATETIME, " + strFecha_Doc + "), ";
															}
															else
															{
																query += " fecha_documento = CONVERT(DATETIME, '" + strFecha_Doc + "'), ";
															}
															query += " WHERE id_pago = '" + arrCamposLinea.GetValue(2).ToString().Replace("'", "").Trim() + "' ";
															query += " AND cod_prov_hub_tp = '" + strCodigoProveedorHub.ToString().Replace("'", "").Trim() + "' ";
															query += " AND numero_documento = '" + arrCamposLinea.GetValue(4).ToString().Replace("'", "").Trim() + "' ";


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
																	catch (Exception ex2) 
																	{
																		Class1.strCodError = ex2.Message;															
																		objTextFile.EscribirLog("  ---> ERROR -- ERROR ABRIENDO LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados + "  CODIGO: " +  Class1.strCodError);
																		intReintentosAplicados += intReintentosAplicados;
																		// SLEEP DEL SISTEMA
																		Thread.Sleep(15000);

																	}
															
																} // FIN DEL WHILE

															} // FIN DEL IF DE LA CONEXION CERRA

															// UPDATE DEL REGISTRO DE ENCABEZADO
															try
															{
																mySqlCommand = objBDatos.Comando(query, mySqlConnection);
																mySqlCommand.ExecuteNonQuery();		

																objTextFile.EscribirLog("REALIZANDO UPDATE DEL ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);

																strTempNroDoc = strNumeroDoc;

															}
															catch (Exception ex1)
															{
																Class1.strCodError = ex1.Message;
																objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError);
																intErrorCarga += 1;
															}

														}
														catch (Exception ex2) 
														{
															Class1.strCodError = ex2.Message;
												
															objTextFile.EscribirLog("  ---> ERROR -- ERROR REALIZANDO EL UPDATE !!AQUI DEL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
															intErrorCarga += 1;
														}
														// HACER EL UPDATE DEL DETALLE YA QUE EL REGISTRO EXISTE EN LA BASE DE DATOS

													}//FIN IF
														// EL ERROR AL INSERTAR EL REGISTRO DE ENCABEZADO EN LA BASE DE DATOS --> NO ES POR DUPLICIDAD
													else
													{
														objTextFile.EscribirLog("  ---> ERROR -- ERROR INSERTANDO EL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + "   " + query + "    DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
													}
												}//FIN CATCH

											}//FIN IF
											else
											{
												objTextFile.EscribirLog("  ---> ERROR -- EL CAMPO DE CODIGO DE PROVEEDOR NO PUEDE SER UN VALOR NULO.  EL NUMERO DE ENCABEZADO DENTRO DEL ARCHIVO ES: " + intNumeroEncabezado);
									
											}

										}//FIN IF
										else
										{
											objTextFile.EscribirLog("  ---> ERROR -- EL CAMPO DE NUMERO DE DOCUMENTO NO PUEDE SER UN VALOR NULO.  EL NUMERO DE ENCABEZADO DENTRO DEL ARCHIVO ES: " + intNumeroEncabezado);
									
										}
									}
								}
							}
						}
					}


					// INSERT EN EL LOG DE LA CANTIDAD DE DOCUMENTOS INSERTADOS O MODIFICADOS
					intNumeroLinea = intNumeroLinea - intErrorCarga;
					objTextFile.EscribirLog("SE INSERTARON Y/O MODIFICARON " + intNumeroLinea + " REGISTROS");

					
					// VERIFICA SI LA CONEXION A BD ESTA ABIERTA PARA CERRARLA Y FINALIZAR LA OPERACION DE IMPORT
					if(mySqlConnection.State.ToString() == "Open")
					{
						mySqlConnection.Close();

						objTextFile.EscribirLog("CERRANDO CONEXION CON LA BASE DE DATOS");
					}
				}
			}
			catch (Exception ex) 
			{
				Class1.strCodError = ex.Message;

				objTextFile.EscribirLog("  ---> ERROR -- ERROR LEYENDO EL ARCHIVO DE ENTRADA: " + strFileName + "  LINEA ARCHIVO: " + intNumeroLinea + "  CODIGO: " + Class1.strCodError);
				
			}

		}

		// --------------------------------------------------------------------------------------------------
		// --------------------------------------------------------------------------------------------------
		// DELETE PAGOS
		// --------------------------------------------------------------------------------------------------
		// --------------------------------------------------------------------------------------------------
		public void DeletePago (SqlConnection mySqlConnection)
		{
		
			// INSTANCIA LA CONEXION DE BASE DE DATOS
			SqlCommand mySqlCommand = new SqlCommand();
			

			// VARIABLES
			string query;
			string strErrorIntentos = "NO";
			int intReintentos = 4; // CANTIDAD DE INTENTOS -1 
			int intReintentosAplicados = 0;

			// -------------------------------------------------------------------------------------------
			// GENERA EL QUERY PARA HACER EL DELETE DE LOS REGISTROS DE BASE DE DATOS DE ENCABEZADO
			// -------------------------------------------------------------------------------------------
			query = " DELETE farmatodo_detalles_pagos WHERE DATEDIFF(DAY,FECHA_PAGO,GETDATE())>45 ";

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
					catch (Exception ex2) 
					{
						Class1.strCodError = ex2.Message;
								
						objTextFile.EscribirLog("  ---> ERROR -- ERROR ABRIENDO LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados + "  CODIGO: " +  Class1.strCodError);

						intReintentosAplicados += intReintentosAplicados;

						// SLEEP DEL SISTEMA
						Thread.Sleep(15000);

					}
										
				} // FIN DEL WHILE

			} // FIN DEL IF DE LA CONEXION CERRADA

			try
			{
				// ELIMINA EL REGISTRO DE ENCABEZADO
				mySqlCommand = objBDatos.Comando(query, mySqlConnection);
				mySqlCommand.ExecuteNonQuery();

				// INSERT EN EL LOG LA ELIMINACION DEL ENCABEZADO
				objTextFile.EscribirLog("ELIMINANDO REGISTROS 45 DIAS MAYORES A LA FECHA ACTUAL ");

			}
			catch (Exception ex) 
			{
				Class1.strCodError = ex.Message;
								
				objTextFile.EscribirLog("  ---> ERROR -- ERROR ELIMINANDO EL REGISTRO ");
				objTextFile.EscribirLog("  ---> ERROR QUERY -- " + query);

			}
		}
	}
}
