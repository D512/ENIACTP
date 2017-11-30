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
			string strNombre_Tienda = "";
			string strTempCod_prov_hub_tp = "";
			string strTempId_pago = "";
			string strTempNumero_doc = "";
			string strCod_Localizacion = "";
			string strErrorDetalle = "";
			double dobMonto_pago = 0;
			double dobMonto_documento = 0;

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

					// PRIMERO HACE DELETE DE TODOS AQUELLOS ARCHIVO MAYORES DE 45 DIAS A LA FECHA ACTUAL
					DeletePago (mySqlConnection);

					// LEE CADA UNA DE LAS LINEAS DEL ARCHIVO
					while ((strLineaLeida = sr.ReadLine()) != null) 
					{
						
						//SEPARA CADA UNO DE LOS CAMPOS DE LA LINEA LEIDA
						strLineaLeida = strLineaLeida.Replace("!","");
						arrCamposLinea = strLineaLeida.Split(';');
						
						strErrorDetalle = "NO";
						strNombre_Tienda = "";
						strNombre_Banco = "";
						strTempCod_prov_hub_tp = "";
						strTempId_pago = "";
						strTempNumero_doc = "";

						if(arrCamposLinea.GetValue(8).ToString().TrimStart().TrimEnd() == "" )
						{
							strFecha_Doc = "NULL";
						}
						else
						{
							strFecha_Doc = arrCamposLinea.GetValue(8).ToString().Substring(0,4) + arrCamposLinea.GetValue(8).ToString().Substring(4,2) + arrCamposLinea.GetValue(8).ToString().Substring(6,2);
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
							dobMonto_pago = Convert.ToDouble(arrCamposLinea.GetValue(3));
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO PAGO: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						try
						{
							dobMonto_documento = Convert.ToDouble(arrCamposLinea.GetValue(6));
						}
						catch(Exception ex)
						{
							Class1.strCodError = ex.Message;
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN CAMPO MONTO DOCUMENTO: " + Class1.strCodError);
							strErrorDetalle = "SI";
						}

						if (arrCamposLinea.GetValue(7).ToString() == "")
						{
							strCod_Localizacion = "000";
							strNombre_Tienda = "Administración";
						}
						else
						{
							strCod_Localizacion = arrCamposLinea.GetValue(7).ToString();
						}
						
						//BUSCA NOMBRE DE BANCO SEGUN CODIGO
						query = " SELECT codigo_banco, nombre_banco ";
						query += " FROM codigos_bancos ";
						query += " WHERE codigo_banco = '" + arrCamposLinea.GetValue(4).ToString().Replace("'", "''") + "' ";

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
							strNombre_Banco = myReader["nombre_banco"].ToString();
							break;
						}

						myReader.Close();

						//BUSCA NOMBRE DE TIENDA SEGUN CODIGO
						if (strCod_Localizacion != "000")
						{
							query = " SELECT * ";
							query += " FROM farmatodo_localidades ";
							query += " WHERE codigo_localizacion = '" + strCod_Localizacion.Replace("'", "''") + "' ";

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
						

						// UPDATE numero_documento=';!5;!' and cod_prov_hub_tp=';!0;!' and id_pago=';!2;!'
						if (arrCamposLinea.GetValue(0).ToString() != "" && arrCamposLinea.GetValue(2).ToString() != "" && arrCamposLinea.GetValue(5).ToString() != "")
						{
							query = " SELECT * FROM farmatodo_detalles_pagos WHERE ";
							query += " numero_documento = '" + arrCamposLinea.GetValue(5).ToString().Replace("'", "''") + "' ";
							query += " and cod_prov_hub_tp = '" + arrCamposLinea.GetValue(0).ToString().Replace("'", "''") + "' ";
							query += " and id_pago = '" + arrCamposLinea.GetValue(2).ToString().Replace("'", "''") + "' ";

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
										query += " fecha_pago = CONVERT(DATETIME, " + strFecha_Pago.Replace("'", "''") + "), ";
									}
									else
									{
										query += " fecha_pago = CONVERT(DATETIME, '" + strFecha_Pago.Replace("'", "''") + "'), ";
									}
									query += " monto_pago = " + dobMonto_pago + ", ";
									query += " codigo_banco = '" + arrCamposLinea.GetValue(4).ToString().Replace("'", "''") + "', ";
									query += " nombre_banco = '" + strNombre_Banco.Replace("'", "''") + "', ";
									query += " monto_documento = " + dobMonto_documento + ", ";
									if(strFecha_Doc == "NULL")
									{
										query += " fecha_documento = CONVERT(DATETIME, " + strFecha_Doc.Replace("'", "''") + "), ";
									}
									else
									{
										query += " fecha_documento = CONVERT(DATETIME, '" + strFecha_Doc.Replace("'", "''") + "'), ";
									}
									query += " codigo_localizacion = '" + strCod_Localizacion.Replace("'", "''") + "', ";
									query += " nombre_tienda = '" + strNombre_Tienda.Replace("'", "''") + "' ";
									query += " WHERE cod_prov_hub_tp = '" + arrCamposLinea.GetValue(0).ToString().Replace("'", "''") + "' ";
									query += " AND id_pago = '" + arrCamposLinea.GetValue(2).ToString().Replace("'", "''") + "' ";
									query += " AND numero_documento = '" + arrCamposLinea.GetValue(5).ToString().Replace("'", "''") + "' ";

								
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

										// ASIGNA VALORES A LAS VARIABLES DEL ENCABEZADO
										strCodigoProveedorHub = arrCamposLinea.GetValue(0).ToString();
										strNumeroDoc = arrCamposLinea.GetValue(5).ToString();

								
										// VERIDICA QUE EL NUMERO DEL DOCUMENTO O EL CODIGO DEL PROVEEDOR NO SEAN NULOS
										if (strNumeroDoc != "")
										{
											if (strCodigoProveedorHub != "")
											{

												query = " INSERT INTO farmatodo_detalles_pagos ";
												query += " (hub_tp, cod_prov_hub_tp, status_pago, fecha_pago, id_pago, monto_pago, codigo_banco, nombre_banco, numero_documento, monto_documento, fecha_documento, codigo_localizacion, nombre_tienda, enviar_correo) ";
												query += " VALUES ";
												query += " ('HUB-04', '" + arrCamposLinea.GetValue(0).ToString().Replace("'", "''") + "', '1', ";
												if(strFecha_Pago == "NULL")
												{
													query += " CONVERT(DATETIME, " + strFecha_Pago.Replace("'", "''") + "), ";
												}
												else
												{
													query += " CONVERT(DATETIME, '" + strFecha_Pago.Replace("'", "''") + "'), ";
												}
												query += " '" + arrCamposLinea.GetValue(2).ToString().Replace("'", "''") + "', " + dobMonto_pago + ", '" + arrCamposLinea.GetValue(4).ToString().Replace("'", "''") + "', '" + strNombre_Banco.Replace("'", "''") + "', '" + arrCamposLinea.GetValue(5).ToString().Replace("'", "''") + "', " + dobMonto_documento + ", ";
												if(strFecha_Doc == "NULL")
												{
													query += " CONVERT(DATETIME, " + strFecha_Doc.Replace("'", "''") + "), ";
												}
												else
												{
													query += " CONVERT(DATETIME, '" + strFecha_Doc.Replace("'", "''") + "'), ";
												}
												query += " '" + strCod_Localizacion.Replace("'", "''") + "', '" + strNombre_Tienda.Replace("'", "''") + "', '1') ";

										
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
															query += " monto_documento = '" + arrCamposLinea.GetValue(6).ToString().Replace("'", "''") + "', ";
															query += " codigo_localizacion = '" + arrCamposLinea.GetValue(7).ToString().Replace("'", "''") + "', ";
															if(strFecha_Doc == "NULL")
															{
																query += " fecha_documento = CONVERT(DATETIME, " + strFecha_Doc.Replace("'", "''") + "), ";
															}
															else
															{
																query += " fecha_documento = CONVERT(DATETIME, '" + strFecha_Doc.Replace("'", "''") + "'), ";
															}
															query += " WHERE id_pago = '" + arrCamposLinea.GetValue(2).ToString().Replace("'", "''") + "' ";
															query += " AND cod_prov_hub_tp = '" + arrCamposLinea.GetValue(0).ToString().Replace("'", "''") + "' ";
															query += " AND numero_documento = '" + arrCamposLinea.GetValue(5).ToString().Replace("'", "''") + "' ";


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
												
															objTextFile.EscribirLog("  ---> ERROR -- ERROR REALIZANDO EL UPDATE DEL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
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
