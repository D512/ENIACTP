using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;

namespace TP_FarmatodoOC
{
	/// <summary>
	/// Summary description for ClaseOrdenCompra.
	/// </summary>
	public class ClaseOrdenCompra
	{
		#region "DECLARACION LIBRERIAS"
		
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		
		#endregion

		#region "CONSTRUCTOR CLASE"
		public ClaseOrdenCompra()
		{
			//
			// TODO: Add constructor logic here
			//
		}
		#endregion

		#region "INSERT ORDEN COMPRA"
		public void InsertOrdenCompra(string strFileName)
		{
			string query = "";			
			string strErrorDetalles = "NO";
			string strErrorIntentos = "NO";
			string strRegistro02 = "NO";
			string strRegistro03 = "NO";
			int intReintentos = 4; // REINTENTA UNA VEZ MENOS DE LO ESPECIFICADO
			int intNumeroLinea = 0;			
			int intNumeroEncabezado = 0;
			int intReintentosAplicados = 0;	

			string strTempNroDoc = "";
			string strFecha_Doc;
			string strFecha_Entrega ;
		
			string strDecimal = System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator;

						
			try
			{
				// CREA UNA INSTANCIA DE StreamReader PARA LEER DEL ARCHIVO.
				using (StreamReader sr = new StreamReader(strFileName)) 
				{
					
					string strLineaLeida;
					Array arrCamposLinea;

					string strUpdateDoc = "NO";

					// VARIABLES ASIGNADAS PARA TRABAJAR CON ENCABEZADOS
					string strCodigoProveedorHub = "";
					string strNumeroDoc = "   ";
					string strHub_TP = "";
					string strCodigo_Comprador = "";
					string strNomber_Comprador = "";
					string strTlf_Cont_Comprador = "";
					string strCodigo_Despacho = "";
					string strNombre_Despacho = "";
					string strDireccion_Despacho = "";

					// VARIABLES DE CAMPOS OBLIGATIRIOS EN LOS ENCABEZADOS
					string nombre_proveedor="";
					string status_doc="";
					string numero_doc="";
                    string fecha_doc="";
					string comentario="";
					string nota_encabezado="";
					string fecha_entrega="";


					// VARIABLES ASIGNADAS PARA TRABAJAR CON DETALLES
					string strCod_Prov_Hub_tp = "";
					string strNumero_OC = "";
					string strCase_Id = "";
					string strDesc_Case_Id = "";
					int intBultos = 0;
					int intUnidades_Cajas = 0;
					int intLinea_Bulto = 0;
					double dblCosto_Bulto = 0;


					// INSTANCIA LA CONEXION DE BASE DE DATOS
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
						if (arrCamposLinea.GetValue(0).ToString() == "01")
						{							
							// ESCRIBIR EN EL LOG LA CANTIDAD DE REGISTROS PROCESADOS
							if ((strNumeroDoc == strTempNroDoc) && (strErrorDetalles == "NO"))
							{
								// INSERT EN EL LOG DE LA CANTIDAD DE DETALLES INSERTADOS POR EL ENCABEZADO
								objTextFile.EscribirLog("SE INSERTARON Y/O MODIFICARON " + (intNumeroLinea - 1) + " REGISTROS DE DETALLE PARA EL DOCUMENTO NUMERO: " + strNumeroDoc);
							}


							// INCREMENTA EL NUMERO DE ENCABEZADO
							intNumeroEncabezado += 1;
							
							// INCREMENTA EL NUMERO DE LA LINEA
							intNumeroLinea = 1;

							// ASIGNA VALORES A LAS VARIABLES DEL ENCABEZADO
							strCodigoProveedorHub = arrCamposLinea.GetValue(42).ToString().Trim();
							strNumeroDoc = arrCamposLinea.GetValue(3).ToString().Trim();
							strUpdateDoc = "NO";

							// VERIDICA QUE EL NUMERO DEL DOCUMENTO O EL CODIGO DEL PROVEEDOR NO SEAN NULOS
							if (strNumeroDoc != "")
							{
								if (strCodigoProveedorHub != "")
								{

									query = "SELECT * FROM farmatodo_cod_prov where cod_prov_hub_tp_nuevo = '" + arrCamposLinea.GetValue(42).ToString().Replace("'","").Trim() + "'";
									try
									{
										comando=objDatos.Comando(query,conexion);
										cursor=comando.ExecuteReader();
										while(cursor.Read())
										{
											strCod_Prov_Hub_tp = cursor["cod_prov_hub_tp"].ToString().Trim();
										}
										cursor.Close();
									}
									catch(Exception e)
									{
										Class1.objTextFile.EscribirLog("  ---> ERROR REALIZANDO EL QUERY QUE VERIFICA LA EXISTENCIA DEL CODIGO_COMPRADOR EN LA BD");
										Class1.objTextFile.EscribirLog("     ---> ERROR :" + e.Message);
									}	
									
									// INICIALIZA LAS VARIABLES CONTADORES
										strErrorDetalles = "NO";
										strRegistro02 = "NO";
										strRegistro03 = "NO";
									
										strFecha_Doc = arrCamposLinea.GetValue(5).ToString().Substring(0,4) + arrCamposLinea.GetValue(5).ToString().Substring(4,2) + arrCamposLinea.GetValue(5).ToString().Substring(6,2);
										strFecha_Entrega = arrCamposLinea.GetValue(8).ToString().Substring(0,4) + arrCamposLinea.GetValue(8).ToString().Substring(4,2) + arrCamposLinea.GetValue(8).ToString().Substring(6,2);
									
										strHub_TP = arrCamposLinea.GetValue(1).ToString().Replace("'","").Trim();
										if (strHub_TP == "")
											strHub_TP = "HUB-04";
									
										strCodigo_Comprador = arrCamposLinea.GetValue(16).ToString().Replace("'","").Trim();
										if (strCodigo_Comprador == "")
											strCodigo_Comprador = "7591472900019";

										strNomber_Comprador = arrCamposLinea.GetValue(17).ToString().Replace("'","").Trim();
										if (strNomber_Comprador == "")
											strNomber_Comprador = "Farmatodo, S.A.";

										strTlf_Cont_Comprador = arrCamposLinea.GetValue(27).ToString().Replace("'","").Trim();
										if (strTlf_Cont_Comprador == "")
											strTlf_Cont_Comprador = "(0212) 9498111";

										strCodigo_Despacho = arrCamposLinea.GetValue(30).ToString().Replace("'","").Trim();
										if (strCodigo_Despacho == "")
										{
											strCodigo_Despacho = "7591472900101";
											strDireccion_Despacho = "Avenida 1, Urb. Industrial Río Tuy Carretera Charallave Cua Estado Miranda";
										}
										else
											strDireccion_Despacho = arrCamposLinea.GetValue(32).ToString().Replace("'","").Trim();

										strNombre_Despacho = arrCamposLinea.GetValue(31).ToString().Replace("'","").Trim();
										if (strNombre_Despacho == "")
											strNombre_Despacho = "Centro de Distribución";


										// GENERA QUERY DE INSERT EN BASE DE DATOS
										query = "INSERT INTO FARMATODO_ENCABEZADO_TOTALES_OC ";
										query += " (HUB_TP, ";
										query += " STATUS_DOC, ";
										query += " NUMERO_DOC, ";
										query += " FECHA_DOC, "; 
										query += " FECHA_ENTREGA, ";
										query += " CODIGO_COMPRADOR, ";
										query += " NOMBRE_COMPRADOR, ";
										query += " TLF_CONT_COMPRADOR, ";
										query += " CODIGO_DESPACHO, ";
										query += " NOMBRE_DESPACHO, ";
										query += " DIRECCION_DESPACHO, ";
										query += " COD_PROV_HUB_TP, ";
										query += " NOMBRE_PROVEEDOR, ";
										query += " TOTAL_UNIDADES, ";
										query += " TOTAL_CAJAS, ";
										query += " TOTAL_VOLUMEN, ";
										query += " TOTAL_PESO, ";
										query += " MONTO_NETO) ";
										query += " VALUES (";
										query += " '" + strHub_TP + "', ";
										query += " '" + arrCamposLinea.GetValue(2).ToString().Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(3).ToString().Replace("'","").Trim() + "', ";
										query += " CONVERT(DATETIME, '" + strFecha_Doc + "'), ";
										query += " CONVERT(DATETIME, '" + strFecha_Entrega + "'), ";
										query += " '" + strCodigo_Comprador + "', ";
										query += " '" + strNomber_Comprador + "', ";
										query += " '" + strTlf_Cont_Comprador + "', ";
										query += " '" + strCodigo_Despacho + "', ";
										query += " '" + strNombre_Despacho + "', ";
										query += " '" + strDireccion_Despacho + "', ";
										query += " '" + strCod_Prov_Hub_tp.Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(43).ToString().Replace("'","").Trim() + "', ";
									
										if(arrCamposLinea.GetValue(56).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(56).ToString().Replace("'","").Trim() + ", ";
										else
											query += "0, ";

										if(arrCamposLinea.GetValue(57).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(57).ToString().Replace("'","").Trim() + ", ";
										else
											query += "0, ";

										if(arrCamposLinea.GetValue(58).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(58).ToString().Replace("'","").Trim() + ", ";
										else
											query += "0, ";

										if(arrCamposLinea.GetValue(59).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(59).ToString().Replace("'","").Trim() + ", ";
										else
											query += "0, ";

										if(arrCamposLinea.GetValue(63).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(63).ToString().Replace("'","").Trim() + ") ";
										else
											query += "0) ";									

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

											objTextFile.EscribirLog("INSERTANDO ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);

											strTempNroDoc = strNumeroDoc;
										}
										catch (Exception ex) 
										{
											Class1.strCodError = ex.Message;
							
											// VERIFICA QUE EL ERROR DE BASE DE DATOS ES POR DUPLICIDAD DEL KEY
											if(Class1.strCodError.Substring(0,41) == "Cannot insert duplicate key row in object")
											{
							
												objTextFile.EscribirLog("  ---> ERROR -- EL REGISTRO DE ENCABEZADO YA EXISTE EN LA BASE DE DATOS.  DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);

							
												// HACER EL UPDATE DEL ENCABEZADO YA QUE EL REGISTRO EXISTE EN LA BASE DE DATOS
												try
												{
													strFecha_Entrega = arrCamposLinea.GetValue(8).ToString().Substring(0,4) + arrCamposLinea.GetValue(8).ToString().Substring(4,2) + arrCamposLinea.GetValue(8).ToString().Substring(6,2);

													// GENERA QUERY DE UPDATE DEL ENCABEZADO
													query = "UPDATE FARMATODO_ENCABEZADO_TOTALES_OC SET ";
													query += " FECHA_ENTREGA = CONVERT(DATETIME, '" + strFecha_Entrega + "'), "; 
													query += " CODIGO_COMPRADOR = '" + strCodigo_Comprador + "', ";
													query += " NOMBRE_COMPRADOR = '" + strNomber_Comprador + "', ";
													query += " TLF_CONT_COMPRADOR = '" + strTlf_Cont_Comprador + "', ";
													query += " CODIGO_DESPACHO = '" + strCodigo_Despacho + "', "; 
													query += " NOMBRE_DESPACHO = '" + strNombre_Despacho + "', ";
													query += " DIRECCION_DESPACHO = '" + strDireccion_Despacho + "', "; 												
													query += " COD_PROV_HUB_TP = '" + arrCamposLinea.GetValue(42).ToString().Replace("'","").Trim() + "', "; 
													query += " NOMBRE_PROVEEDOR = '" + arrCamposLinea.GetValue(43).ToString().Replace("'","").Trim() + "', ";

													if(arrCamposLinea.GetValue(56).ToString().Trim()!="")
														query += " TOTAL_UNIDADES = " + arrCamposLinea.GetValue(56).ToString().Trim() + ", ";
													else
														query += " TOTAL_UNIDADES = 0, ";

													if(arrCamposLinea.GetValue(57).ToString().Trim()!="")
														query += " TOTAL_CAJAS = " + arrCamposLinea.GetValue(57).ToString().Trim() + ", ";
													else
														query += " TOTAL_CAJAS = 0, ";

													if(arrCamposLinea.GetValue(58).ToString().Trim()!="")
														query += " TOTAL_VOLUMEN = " + arrCamposLinea.GetValue(58).ToString().Trim() + ", ";
													else
														query += " TOTAL_VOLUMEN = 0, ";

													if(arrCamposLinea.GetValue(59).ToString().Trim()!="")
														query += " TOTAL_PESO = " + arrCamposLinea.GetValue(59).ToString().Trim() + ", ";
													else
														query += " TOTAL_PESO = 0, ";

													if(arrCamposLinea.GetValue(63).ToString().Trim()!="")
														query += " MONTO_NETO = " + arrCamposLinea.GetValue(63).ToString().Trim() + ", ";
													else
														query += " MONTO_NETO = 0, ";

													query += " WHERE NUMERO_DOC ='" + strNumeroDoc + "' ";
													query += " AND COD_PROV_HUB_TP = '" + strCodigoProveedorHub + "' ";


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
													mySqlCommand = objBDatos.Comando(query, mySqlConnection);
													mySqlCommand.ExecuteNonQuery();		

													objTextFile.EscribirLog("REALIZANDO UPDATE DEL ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);

													strUpdateDoc = "SI";

													strTempNroDoc = strNumeroDoc;

												}
												catch (Exception ex2) 
												{
													Class1.strCodError = ex2.Message;
							
													objTextFile.EscribirLog("  ---> ERROR -- ERROR REALIZANDO EL UPDATE DEL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
												}

											}
												// EL ERROR AL INSERTAR EL REGISTRO DE ENCABEZADO EN LA BASE DE DATOS --> NO ES POR DUPLICIDAD
											else
											{
												objTextFile.EscribirLog("  ---> ERROR -- ERROR INSERTANDO EL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + "   " + query + "    DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);

												// LLAMAR PROCESO DE DELETE DE DOCUMENTO
												DeleteOrdenCompra (strNumeroDoc, strCodigoProveedorHub, mySqlConnection);

												strErrorDetalles = "SI";

											}
										}
								}
								else
								{
									objTextFile.EscribirLog("  ---> ERROR -- EL CAMPO DE CODIGO DE PROVEEDOR NO PUEDE SER UN VALOR NULO.  EL NUMERO DE ENCABEZADO DENTRO DEL ARCHIVO ES: " + intNumeroEncabezado);
									strErrorDetalles = "SI";
								}
							}
							else
							{
								objTextFile.EscribirLog("  ---> ERROR -- EL CAMPO DE NUMERO DE DOCUMENTO NO PUEDE SER UN VALOR NULO.  EL NUMERO DE ENCABEZADO DENTRO DEL ARCHIVO ES: " + intNumeroEncabezado);
								strErrorDetalles = "SI";
							
							}

	
						}
							// -------------------------------------------------------------------------------------
							// -------------------------------------------------------------------------------------
							//DETERMINA EL TIPO DE REGISTRO DETALLE
							// -------------------------------------------------------------------------------------
							// -------------------------------------------------------------------------------------
						else if(arrCamposLinea.GetValue(0).ToString() == "02")
						{
						
							if (strErrorDetalles == "NO")
							{
						
								if((arrCamposLinea.GetValue(31).ToString().Trim()!="")&&(arrCamposLinea.GetValue(2).ToString().Trim()!="")&&(arrCamposLinea.GetValue(3).ToString().Trim()!=""))
								{
									// INCREMENTA EL NUMERO DE LA LINEA
									intNumeroLinea += 1;

									query = "SELECT * FROM farmatodo_cod_prov where cod_prov_hub_tp_nuevo = '" + arrCamposLinea.GetValue(2).ToString().Replace("'","").Trim() + "'";
									try
									{
										comando=objDatos.Comando(query,conexion);
										cursor=comando.ExecuteReader();
										while(cursor.Read())
										{
											strCod_Prov_Hub_tp = cursor["cod_prov_hub_tp"].ToString().Trim();
										}
										cursor.Close();
									}
									catch(Exception e)
									{
										Class1.objTextFile.EscribirLog("  ---> ERROR REALIZANDO EL QUERY QUE VERIFICA LA EXISTENCIA DEL CODIGO_COMPRADOR EN LA BD");
										Class1.objTextFile.EscribirLog("     ---> ERROR :" + e.Message);
									}
									
									try
									{
										// COLOCA LOS VALORES DE LOS CAMPOS EN LAS VARIABLES CORRESPONDIENTES
										//strCod_Prov_Hub_tp = arrCamposLinea.GetValue(2).ToString().Replace("'","").Trim();
										strNumero_OC = arrCamposLinea.GetValue(3).ToString().Replace("'","").Trim();
										strCase_Id = arrCamposLinea.GetValue(5).ToString().Replace("'","").Trim();
										strDesc_Case_Id = arrCamposLinea.GetValue(9).ToString().Replace("'","").Trim();
										intBultos = Convert.ToInt32(arrCamposLinea.GetValue(11).ToString().Trim());

										if(arrCamposLinea.GetValue(12).ToString().Trim()!="")
										   intUnidades_Cajas = Convert.ToInt32(arrCamposLinea.GetValue(12).ToString().Trim());
										else
										   intUnidades_Cajas = 0;
										intLinea_Bulto = Convert.ToInt32(arrCamposLinea.GetValue(31).ToString().Trim());
										dblCosto_Bulto = Convert.ToDouble(arrCamposLinea.GetValue(36).ToString().Trim());

										strRegistro02 = "SI";
									}
									catch(Exception e)
									{
                                        objTextFile.EscribirLog("  ---> ERROR EN ALGUN CAMPO NUMERICO(Linea_bulto o unidades_caja o bulto o costo_bulto). ERROR: " + e.Message);
									}
								}
							}
						}

							// -------------------------------------------------------------------------------------
							// -------------------------------------------------------------------------------------
							//DETERMINA EL TIPO DE REGISTRO DETALLE
							// -------------------------------------------------------------------------------------
							// -------------------------------------------------------------------------------------
						else if(arrCamposLinea.GetValue(0).ToString() == "03")
						{
						
							if (strErrorDetalles == "NO")
							{

								// SE PROCESO EL REGISTRO 02 DEL DETALLE ESPECIFICO
								if (strRegistro02 == "SI")
								{

										// INCREMENTA EL NUMERO DE LA LINEA
										intNumeroLinea += 1;

										// GENERA QUERY DE INSERT EN BASE DE DATOS
										query = "INSERT INTO FARMATODO_DETALLES_OC (";
										query += " HUB_TP, ";
										query += " COD_PROV_HUB_TP, ";
										query += " NUMERO_DOC, ";
										query += " CASE_ID, ";
										query += " DESC_CASE_ID, ";
										query += " BULTOS, ";
										query += " UNIDADES_CAJA, ";
										query += " LINEA_BULTO, ";
										query += " COSTO_BULTO, ";
										query += " STATUS_ITEM, ";
										query += " COD_BARRA_PRODUCTO, ";
										query += " COD_PROD_PROVEEDOR, ";
										query += " SKU_FARMATODO, ";
										query += " DESC_PRODUCTO, ";
										query += " UNIDADES, ";
										query += " COSTO_BASE_UNIDAD_REATIL, ";
										query += " PVP_UNIDAD_RETAIL, ";
										query += " PORCENTAJE_DESCUENTO, ";
										query += " MONTO_TOTAL_ITEM, ";
										query += " UNIDADES_BULTO, ";
										query += " COSTO_UNIDAD_RETAIL) ";
										query += " VALUES( ";
										query += " '" + strHub_TP + "', ";
										query += " '" + strCod_Prov_Hub_tp + "', ";
										query += " '" + strNumero_OC + "', ";
										query += " '" + strCase_Id + "', ";
										query += " '" + strDesc_Case_Id + "', ";
										query += intBultos + ", ";
										query += intUnidades_Cajas + ", ";
										query += intLinea_Bulto + ", ";
										query += dblCosto_Bulto + ", ";
										query += " '100', ";
										query += " '" + arrCamposLinea.GetValue(5).ToString().Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(6).ToString().Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(7).ToString().Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(8).ToString().Replace("'","").Trim() + "', ";
										
										if(arrCamposLinea.GetValue(11).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(11).ToString().Replace("'","").Trim() + ", ";
										else
										   query += "0, ";

										if(arrCamposLinea.GetValue(17).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(17).ToString().Replace("'","").Trim() + ", ";
									    else
										   query += "0, ";

										if(arrCamposLinea.GetValue(21).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(21).ToString().Replace("'","").Trim() + ", ";
									    else
										   query += "0, ";

										if(arrCamposLinea.GetValue(22).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(22).ToString().Replace("'","").Trim() + ", ";
										else
										   query += "0, ";

										if(arrCamposLinea.GetValue(24).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(24).ToString().Replace("'","").Trim() + ", ";
										else
										   query += "0, ";

										if(arrCamposLinea.GetValue(33).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(33).ToString().Replace("'","").Trim() + ", ";
										else
										   query += "0, ";

										if(arrCamposLinea.GetValue(35).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(35).ToString().Replace("'","").Trim() + ") ";
									    else
										   query += "0) ";

									
									
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

													objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados);

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

										} // FIN DEL IF DE LA CONEXION CERRA
									
					
									
										// INSERTA EL REGISTRO DE DETALLE
										try
										{
											mySqlCommand = objBDatos.Comando(query, mySqlConnection);
											mySqlCommand.ExecuteNonQuery();	
										}
										catch (Exception ex) 
										{
											Class1.strCodError = ex.Message;

											// VERIFICA QUE EL ERROR DE BASE DE DATOS ES POR DUPLICIDAD DEL KEY
											if(Class1.strCodError.Substring(0,41) == "Cannot insert duplicate key row in object")
											{
												objTextFile.EscribirLog("  ---> ERROR -- EL REGISTRO DE DETALLE YA EXISTE EN LA BASE DE DATOS.  LINEA REPETIDA: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
									
												// HACER EL UPDATE DEL ENCABEZADO YA QUE EL REGISTRO EXISTE EN LA BASE DE DATOS
												try
												{
													// GENERA QUERY DE UPDATE DEL DETALLE
													query = "UPDATE FARMATODO_DETALLES_OC SET ";
													query += " CASE_ID = '" + strCase_Id + "', ";
													query += " DESC_CASE_ID = '" + strDesc_Case_Id + "', ";
													query += " BULTOS = " + intBultos + ", ";
													query += " UNIDADES_CAJA = " + intUnidades_Cajas + ", ";
													query += " COSTO_BULTO = " + dblCosto_Bulto + ", ";
													query += " COD_BARRA_PRODUCTO = '" + arrCamposLinea.GetValue(5).ToString().Replace("'","").Trim() + "', ";
													query += " COD_PROD_PROVEEDOR = '" + arrCamposLinea.GetValue(6).ToString().Replace("'","").Trim() + "', ";
													query += " SKU_FARMATODO = '" + arrCamposLinea.GetValue(7).ToString().Replace("'","").Trim() + "', ";
													query += " DESC_PRODUCTO = '" + arrCamposLinea.GetValue(8).ToString().Replace("'","").Trim() + "', ";
													
													if(arrCamposLinea.GetValue(11).ToString().Trim()!="")
													   query += " UNIDADES = " + arrCamposLinea.GetValue(11).ToString().Replace("'","").Trim() + ", ";
													else
													   query += " UNIDADES = 0, ";
													
													if(arrCamposLinea.GetValue(17).ToString().Trim()!="")
													   query += " COSTO_BASE_UNIDAD_REATIL = " + arrCamposLinea.GetValue(17).ToString().Replace("'","").Trim() + ", ";
													else
													   query += " COSTO_BASE_UNIDAD_REATIL = 0, ";

													if(arrCamposLinea.GetValue(21).ToString().Trim()!="")
													   query += " PVP_UNIDAD_RETAIL = " + arrCamposLinea.GetValue(21).ToString().Replace("'","").Trim()+ ", ";
													else
													   query += " PVP_UNIDAD_RETAIL = 0, ";
													
													if(arrCamposLinea.GetValue(22).ToString().Trim()!="")
													   query += " PORCENTAJE_DESCUENTO = " + arrCamposLinea.GetValue(22).ToString().Replace("'","").Trim() + ", ";
													else
													   query += " PORCENTAJE_DESCUENTO = 0, ";

													if(arrCamposLinea.GetValue(24).ToString().Trim()!="")
													   query += " MONTO_TOTAL_ITEM = " + arrCamposLinea.GetValue(24).ToString().Replace("'","").Trim() + ", ";
													else
													   query += " MONTO_TOTAL_ITEM = 0, ";

													if(arrCamposLinea.GetValue(33).ToString().Trim()!="")
													   query += " UNIDADES_BULTO = " + arrCamposLinea.GetValue(33).ToString().Replace("'","").Trim() + ", ";
													else
													   query += " UNIDADES_BULTO = 0, ";

													if(arrCamposLinea.GetValue(35).ToString().Trim()!="")													
													   query += " COSTO_UNIDAD_RETAIL = " + arrCamposLinea.GetValue(35).ToString().Replace("'","").Trim();
													else
													   query += " COSTO_UNIDAD_RETAIL =0";
													query += " WHERE NUMERO_DOC ='" + strNumero_OC + "' ";
													query += " AND COD_PROV_HUB_TP = '" + strCod_Prov_Hub_tp + "' ";
													query += " AND LINEA_BULTO = " + intLinea_Bulto;

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
												

													// UPDATE DEL REGISTRO DE DETALLE
													mySqlCommand = objBDatos.Comando(query, mySqlConnection);
													mySqlCommand.ExecuteNonQuery();		

													strTempNroDoc = strNumeroDoc;

												}
												catch (Exception ex2) 
												{
													Class1.strCodError = ex2.Message;
								
													objTextFile.EscribirLog("  ---> ERROR -- ERROR REALIZANDO EL UPDATE DEL REGISTRO DE DETALLE REGISTRO DE DETALLE: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
												}

											}
												// EL ERROR AL INSERTAR EL REGISTRO DE DETALLE EN LA BASE DE DATOS --> NO ES POR DUPLICIDAD
											else
											{
												objTextFile.EscribirLog("  ---> ERROR -- ERROR INSERTANDO EL REGISTRO DE DETALLE: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + query);

												// LLAMAR PROCESO DE DELETE DE DOCUMENTO
												DeleteOrdenCompra (strNumeroDoc, strCodigoProveedorHub, mySqlConnection);

											}
										} // CATCH
								} 
									// NO SE PROCESO EL REGISTRO 02 POR LO QUE HAY ERROR EN EL DETALLE
								else
								{
									strErrorDetalles = "SI";

									objTextFile.EscribirLog("  ---> ERROR -- ERROR INSERTANDO EL REGISTRO DE DETALLE - NO HAY REGISTRO 02 ASOCIADO AL REGISTRO 03 ACTUAL: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);

									// LLAMAR PROCESO DE DELETE DE DOCUMENTO
									DeleteOrdenCompra (strNumeroDoc, strCodigoProveedorHub, mySqlConnection);
								}
							
							} // IF DE strErrorDetalles = "SI";
					
	
						}

						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						//DETERMINA EL TIPO DE REGISTRO TRACKING
						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						else if(arrCamposLinea.GetValue(0).ToString() == "04")
						{
						
							if (strErrorDetalles == "NO")
							{

								// SE PROCESO EL REGISTRO 02 DEL DETALLE ESPECIFICO
								if (strUpdateDoc == "NO")
								{
                                    // VALIDACION DE LOS CAMPOS NUMERICOS
									if((arrCamposLinea.GetValue(2).ToString().Trim()!="")&&(arrCamposLinea.GetValue(3).ToString().Trim()!=""))
									{
									
										// INCREMENTA EL NUMERO DE LA LINEA
										intNumeroLinea += 1;

										// GENERA QUERY DE INSERT EN BASE DE DATOS
										query = "INSERT INTO FARMATODO_OC_TRACKING (";
										query += " HUB_TP, ";
										query += " COD_PROV_HUB_TP, ";
										query += " NUMERO_DOC, ";
										query += " FECHA_REGISTRO, ";
										query += " USUARIO, ";
										query += " STATUS_ANTERIOR, ";
										query += " STATUS_ACTUAL) ";
										query += " VALUES( ";

										if(arrCamposLinea.GetValue(1).ToString().Trim()!="")
											query += " '" + arrCamposLinea.GetValue(1).ToString().Replace("'","").Trim() + "', ";
										else
											query += " 'HUB-04', ";

										query += " '" + arrCamposLinea.GetValue(2).ToString().Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(3).ToString().Replace("'","").Trim() + "', ";
										query += " GetDate(), ";
										query += " 'Farmatodo', ";
										query += " '100', ";
										query += " '101') ";

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

											objTextFile.EscribirLog("INSERTANDO TRACKING DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);

											strTempNroDoc = strNumeroDoc;
										}
										catch (Exception ex) 
										{
											strErrorDetalles = "SI";

											objTextFile.EscribirLog("  ---> ERROR -- ERROR INSERTANDO EL REGISTRO DE TRACKING: " + ex.Message + " En la línea: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);

											// LLAMAR PROCESO DE DELETE DE DOCUMENTO
											DeleteOrdenCompra (strNumeroDoc, strCodigoProveedorHub, mySqlConnection);

										}
									}
									else
                                       objTextFile.EscribirLog(" NO SE PUEDE INSERTAR EL REGISTRO DE TRACKING EN LA LINEA: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + ", PORQUE EXISTEN CAMPOS OBLIGATORIOS VACIOS(cod_prov_hub_tp o numero_doc)");

								}
								else
								{
									objTextFile.EscribirLog("--> NO SE INSERTA EL REGISTRO DE TRACKING YA QUE LA ORDEN YA EXISTIA EN BASE DE DATOS");
								}
							}
						}


						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						// ERROR DE TIPO DE REGISTRO INVALIDO NO ES NI '01' NI '02' NI '03'
						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						else
						{
				
							// INCREMENTA EL NUMERO DE LA LINEA
							intNumeroLinea += 1;
				
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN EL CONTENIDO DEL ARCHIVO DE ENTRADA.  TIPO DE REGISTRO INVALIDO EN LA LINEA: " + intNumeroLinea);

							// SE SALE DEL WHILE
							break;

						}

					} // END DEL WHILE DE LECTURA DEL ARCHIVO


					// INSERT EN EL LOG DE LA CANTIDAD DE DETALLES INSERTADOS POR EL ENCABEZADO
					objTextFile.EscribirLog("SE INSERTARON Y/O MODIFICARON " + (intNumeroLinea - 1) + " REGISTROS DE DETALLE PARA EL DOCUMENTO NUMERO: " + strNumeroDoc);


					
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
		#endregion
		
		#region "DELETE ORDEN INCOMPLETA"
		public void DeleteOrdenCompra (string strNumeroDoc, string strCodigoProveedorHub, SqlConnection mySqlConnection)
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
			query = " DELETE FARMATODO_ENCABEZADO_TOTALES_OC ";
			query += " WHERE NUMERO_DOC ='" + strNumeroDoc + "' ";
			query += " AND COD_PROV_HUB_TP = '" + strCodigoProveedorHub + "' ";

			// INSERT EN EL LOG LA ELIMINACION DEL ENCABEZADO
			objTextFile.EscribirLog("ELIMINANDO REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);

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
			}
			catch (Exception ex) 
			{
				Class1.strCodError = ex.Message;
								
				objTextFile.EscribirLog("  ---> ERROR -- ERROR ELIMINANDO EL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
			}

			
			// -------------------------------------------------------------------------------------------
			// GENERA EL QUERY PARA HACER EL DELETE DE LOS REGISTROS DE BASE DE DATOS DE DETALLES
			// -------------------------------------------------------------------------------------------
			query = " DELETE FARMATODO_DETALLES_OC ";
			query += " WHERE NUMERO_DOC ='" + strNumeroDoc + "' ";
			query += " AND COD_PROV_HUB_TP = '" + strCodigoProveedorHub + "' ";

			// INSERT EN EL LOG LA ELIMINACION DE LOS DETALLES
			objTextFile.EscribirLog("ELIMINANDO LOS REGISTROS DE DETALLES DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);
			
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

			try
			{
				// ELIMINA LOS REGISTROS DE DETALLES
				mySqlCommand = objBDatos.Comando(query, mySqlConnection);
				mySqlCommand.ExecuteNonQuery();
			}
			catch (Exception ex) 
			{
				Class1.strCodError = ex.Message;
								
				objTextFile.EscribirLog("  ---> ERROR -- ERROR ELIMINANDO LOS REGISTROS DE DETALLE DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
			}

		}
		#endregion
	
	}
}
