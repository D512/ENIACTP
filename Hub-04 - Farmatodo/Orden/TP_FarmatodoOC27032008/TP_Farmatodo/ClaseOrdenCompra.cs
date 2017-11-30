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
					
					// VARIABLES ASIGNADAS PARA TRABAJAR CON DETALLES
					string strCod_Prov_Hub_tp = "";
					string strNumero_OC = "";					
					int intBultos = 0;
					int intUnidades_Bulto = 0;
					int intLinea_Bulto = 0;
					double dblCosto_Bulto = 0;


					// INSTANCIA LA CONEXION DE BASE DE DATOS
					SqlConnection mySqlConnection = new SqlConnection();
					SqlCommand mySqlCommand = new SqlCommand();
					SqlDataReader cursor;
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
							intLinea_Bulto = 0;

							// ASIGNA VALORES A LAS VARIABLES DEL ENCABEZADO
							strCodigoProveedorHub = arrCamposLinea.GetValue(9).ToString().Trim();
							strNumeroDoc = arrCamposLinea.GetValue(3).ToString().Trim();
							

							// VERIFICA QUE EL NUMERO DEL DOCUMENTO O EL CODIGO DEL PROVEEDOR NO SEAN NULOS
							if (strNumeroDoc != "")
							{
								if (strCodigoProveedorHub != "")
								{

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
									
									// INICIALIZA LAS VARIABLES CONTADORES
										strErrorDetalles = "NO";
									
									query = "SELECT * FROM farmatodo_cod_prov where cod_prov_hub_tp_nuevo = '" + arrCamposLinea.GetValue(9).ToString().Replace("'","").Trim() + "'";
									try
									{
										mySqlCommand=objBDatos.Comando(query,mySqlConnection);
										cursor=mySqlCommand.ExecuteReader();
										while(cursor.Read())
										{
											strCodigoProveedorHub = cursor["cod_prov_hub_tp"].ToString().Trim();
										}
										cursor.Close();
									}
									catch(Exception e)
									{
										Class1.objTextFile.EscribirLogError("  ---> ERROR REALIZANDO EL QUERY QUE VERIFICA LA EXISTENCIA DEL COD_PROV_HUB_TP VIEJO EN LA BD");
										Class1.objTextFile.EscribirLogError("     ---> ERROR :" + e.Message);
									}	
									
										strFecha_Doc = arrCamposLinea.GetValue(4).ToString().Substring(0,4) + arrCamposLinea.GetValue(4).ToString().Substring(4,2) + arrCamposLinea.GetValue(4).ToString().Substring(6,2);
										strFecha_Entrega = arrCamposLinea.GetValue(5).ToString().Substring(0,4) + arrCamposLinea.GetValue(5).ToString().Substring(4,2) + arrCamposLinea.GetValue(5).ToString().Substring(6,2);
									
										strHub_TP = arrCamposLinea.GetValue(1).ToString().Replace("'","").Trim();
										if (strHub_TP == "")
											strHub_TP = "HUB-04";
									
										strCodigo_Comprador = arrCamposLinea.GetValue(6).ToString().Replace("'","").Trim();
										if (strCodigo_Comprador == "")
											strCodigo_Comprador = "7591472900019";

										strNomber_Comprador = arrCamposLinea.GetValue(7).ToString().Replace("'","").Trim();
										if (strNomber_Comprador == "")
											strNomber_Comprador = "Farmatodo, C.A.";

										strTlf_Cont_Comprador = arrCamposLinea.GetValue(8).ToString().Replace("'","").Trim();
										if (strTlf_Cont_Comprador == "")
											strTlf_Cont_Comprador = "(0212) 9498111";

										strCodigo_Despacho = arrCamposLinea.GetValue(11).ToString().Replace("'","").Trim();
									if (strCodigo_Despacho == "7591472900101")
									{										
										strDireccion_Despacho = "Avenida 1, Urb. Industrial Río Tuy Carretera Charallave Cua Estado Miranda";
										strNombre_Despacho = "Centro de Distribución";
									}
									else
									{
										strDireccion_Despacho = arrCamposLinea.GetValue(13).ToString().Replace("'","").Trim();
										strNombre_Despacho = arrCamposLinea.GetValue(12).ToString().Replace("'","").Trim();
									}
										

										// GENERA QUERY DE INSERT EN BASE DE DATOS
										query = "INSERT INTO FARMATODO_ENCABEZADO_TOTALES_OC ";
										query += " (HUB_TP, ";
										query += " STATUS_DOC, ";
										query += " NUMERO_DOC, ";
										query += " FECHA_DOC, "; 
										query += " FECHA_ENTREGA, ";
										query += " FECHA_STATUS, ";
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
										query += " CONVERT(DATETIME, GETDATE()), ";
										query += " '" + strCodigo_Comprador + "', ";
										query += " '" + strNomber_Comprador + "', ";
										query += " '" + strTlf_Cont_Comprador + "', ";
										query += " '" + strCodigo_Despacho + "', ";
										query += " '" + strNombre_Despacho + "', ";
										query += " '" + strDireccion_Despacho + "', ";
										query += " '" + strCodigoProveedorHub.Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(10).ToString().Replace("'","").Trim() + "', ";
									
										if(arrCamposLinea.GetValue(14).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(14).ToString().Replace("'","").Trim() + ", ";
										else
											query += "0, ";

										if(arrCamposLinea.GetValue(15).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(15).ToString().Replace("'","").Trim() + ", ";
										else
											query += "0, ";

										if(arrCamposLinea.GetValue(17).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(17).ToString().Replace("'","").Trim() + ", ";
										else
											query += "0, ";

										if(arrCamposLinea.GetValue(18).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(18).ToString().Replace("'","").Trim() + ", ";
										else
											query += "0, ";

										if(arrCamposLinea.GetValue(16).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(16).ToString().Replace("'","").Trim() + ") ";
										else
											query += "0) ";																
									
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
							
												//objTextFile.EscribirLog("  ---> ERROR -- EL REGISTRO DE ENCABEZADO YA EXISTE EN LA BASE DE DATOS.  DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);

							
												// HACER EL UPDATE DEL ENCABEZADO YA QUE EL REGISTRO EXISTE EN LA BASE DE DATOS
												try
												{
													strFecha_Entrega = arrCamposLinea.GetValue(5).ToString().Substring(0,4) + arrCamposLinea.GetValue(5).ToString().Substring(4,2) + arrCamposLinea.GetValue(5).ToString().Substring(6,2);

													// GENERA QUERY DE UPDATE DEL ENCABEZADO
													query = "UPDATE FARMATODO_ENCABEZADO_TOTALES_OC SET ";
													query += " FECHA_ENTREGA = CONVERT(DATETIME, '" + strFecha_Entrega + "'), "; 													
													query += " CODIGO_COMPRADOR = '" + strCodigo_Comprador + "', ";
													query += " NOMBRE_COMPRADOR = '" + strNomber_Comprador + "', ";
													query += " TLF_CONT_COMPRADOR = '" + strTlf_Cont_Comprador + "', ";
													query += " CODIGO_DESPACHO = '" + strCodigo_Despacho + "', "; 
													query += " NOMBRE_DESPACHO = '" + strNombre_Despacho + "', ";
													query += " DIRECCION_DESPACHO = '" + strDireccion_Despacho + "', ";												
													query += " NOMBRE_PROVEEDOR = '" + arrCamposLinea.GetValue(10).ToString().Replace("'","").Trim() + "', ";

													if(arrCamposLinea.GetValue(14).ToString().Trim()!="")
														query += " TOTAL_UNIDADES = " + arrCamposLinea.GetValue(14).ToString().Trim() + ", ";
													else
														query += " TOTAL_UNIDADES = 0, ";

													if(arrCamposLinea.GetValue(15).ToString().Trim()!="")
														query += " TOTAL_CAJAS = " + arrCamposLinea.GetValue(15).ToString().Trim() + ", ";
													else
														query += " TOTAL_CAJAS = 0, ";

													if(arrCamposLinea.GetValue(17).ToString().Trim()!="")
														query += " TOTAL_VOLUMEN = " + arrCamposLinea.GetValue(17).ToString().Trim() + ", ";
													else
														query += " TOTAL_VOLUMEN = 0, ";

													if(arrCamposLinea.GetValue(18).ToString().Trim()!="")
														query += " TOTAL_PESO = " + arrCamposLinea.GetValue(18).ToString().Trim() + ", ";
													else
														query += " TOTAL_PESO = 0, ";

													if(arrCamposLinea.GetValue(16).ToString().Trim()!="")
														query += " MONTO_NETO = " + arrCamposLinea.GetValue(16).ToString().Trim() + " ";
													else
														query += " MONTO_NETO = 0 ";

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
													
													strTempNroDoc = strNumeroDoc;

												}
												catch (Exception ex2) 
												{
													Class1.strCodError = ex2.Message;
							
													objTextFile.EscribirLogError("  ---> ERROR -- ERROR REALIZANDO EL UPDATE DEL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
												}

											}
												// EL ERROR AL INSERTAR EL REGISTRO DE ENCABEZADO EN LA BASE DE DATOS --> NO ES POR DUPLICIDAD
											else
											{
												objTextFile.EscribirLogError("  ---> ERROR -- ERROR INSERTANDO EL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + "   " + query + "    DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);

												// LLAMAR PROCESO DE DELETE DE DOCUMENTO
												DeleteOrdenCompra (strNumeroDoc, strCodigoProveedorHub, mySqlConnection);

												strErrorDetalles = "SI";

											}
										}
								}
								else
								{
									objTextFile.EscribirLogError("  ---> ERROR -- EL CAMPO DE CODIGO DE PROVEEDOR NO PUEDE SER UN VALOR NULO.  EL NUMERO DE ENCABEZADO DENTRO DEL ARCHIVO ES: " + intNumeroEncabezado);
									strErrorDetalles = "SI";
								}
							}
							else
							{
								objTextFile.EscribirLogError("  ---> ERROR -- EL CAMPO DE NUMERO DE DOCUMENTO NO PUEDE SER UN VALOR NULO.  EL NUMERO DE ENCABEZADO DENTRO DEL ARCHIVO ES: " + intNumeroEncabezado);
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
								
								if((arrCamposLinea.GetValue(7).ToString().Trim()!="")&&(arrCamposLinea.GetValue(3).ToString().Trim()!="")&&(arrCamposLinea.GetValue(4).ToString().Trim()!=""))
								{
										
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
									
									
									query = "SELECT * FROM farmatodo_cod_prov where cod_prov_hub_tp_nuevo = '" + arrCamposLinea.GetValue(3).ToString().Replace("'","").Trim() + "'";
									try
									{
										mySqlCommand=objBDatos.Comando(query,mySqlConnection);
										cursor=mySqlCommand.ExecuteReader();
										while(cursor.Read())
										{
											strCodigoProveedorHub = cursor["cod_prov_hub_tp"].ToString().Trim();
										}
										cursor.Close();
									}
									catch(Exception e)
									{
										Class1.objTextFile.EscribirLogError("  ---> ERROR REALIZANDO EL QUERY QUE VERIFICA LA EXISTENCIA DEL COD_PROV_HUB_TP VIEJO EN LA BD");
										Class1.objTextFile.EscribirLogError("     ---> ERROR :" + e.Message);
									}
									
									// INCREMENTA EL NUMERO DE LA LINEA
										intNumeroLinea += 1;
										intLinea_Bulto += 1;

										// COLOCA LOS VALORES DE LOS CAMPOS EN LAS VARIABLES CORRESPONDIENTES
										//strCod_Prov_Hub_tp = arrCamposLinea.GetValue(3).ToString().Replace("'","").Trim();
										strNumero_OC = arrCamposLinea.GetValue(4).ToString().Replace("'","").Trim();										
										
										if(arrCamposLinea.GetValue(13).ToString().Trim()!="")
											intBultos = Convert.ToInt32(arrCamposLinea.GetValue(13).ToString().Trim());
										else
											intBultos = 0;	

										if(arrCamposLinea.GetValue(10).ToString().Trim()!="")
											intUnidades_Bulto = Convert.ToInt32(arrCamposLinea.GetValue(10).ToString().Trim());
										else
											intUnidades_Bulto = 0;										
										
										if(arrCamposLinea.GetValue(14).ToString().Trim()!="")
											dblCosto_Bulto = Convert.ToDouble(arrCamposLinea.GetValue(14).ToString().Trim());
										else
											dblCosto_Bulto = 0.0;

										// GENERA QUERY DE INSERT EN BASE DE DATOS
										query = "INSERT INTO FARMATODO_DETALLES_OC (";
										query += " HUB_TP, ";
										query += " COD_PROV_HUB_TP, ";
										query += " NUMERO_DOC, ";
										query += " FECHA_STATUS, ";	
										query += " BULTOS, ";
										query += " UNIDADES_BULTO, ";
										query += " LINEA_BULTO, ";
										query += " COSTO_BULTO, ";
										query += " STATUS_ITEM, ";
										query += " COD_BARRA_PRODUCTO, ";
										query += " COD_PROD_PROVEEDOR, ";
										query += " SKU_FARMATODO, ";
										query += " DESC_PRODUCTO, ";
										query += " UNIDADES, ";										
										query += " PVP_UNIDAD_RETAIL, ";
										query += " PORCENTAJE_DESCUENTO, ";
										query += " MONTO_TOTAL_ITEM, ";
										query += " COSTO_UNIDAD_RETAIL, ";	
										query += " CASE_ID) ";
										query += " VALUES( ";
										query += " '" + strHub_TP + "', ";
										query += " '" + strCodigoProveedorHub + "', ";
										query += " '" + strNumero_OC + "', ";
										query += " CONVERT(DATETIME, GETDATE()), ";
										query += intBultos + ", ";
										query += intUnidades_Bulto + ", ";
										query += intLinea_Bulto + ", ";
										query += dblCosto_Bulto + ", ";
										query += " '100', ";
										query += " '" + arrCamposLinea.GetValue(5).ToString().Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(6).ToString().Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(7).ToString().Replace("'","").Trim() + "', ";
										query += " '" + arrCamposLinea.GetValue(8).ToString().Replace("'","").Trim() + "', ";
										
										if(arrCamposLinea.GetValue(9).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(9).ToString().Replace("'","").Trim() + ", ";
										else
										   query += "0, ";

										if(arrCamposLinea.GetValue(11).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(11).ToString().Replace("'","").Trim() + ", ";
									    else
										   query += "0, ";

										if(arrCamposLinea.GetValue(16).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(16).ToString().Replace("'","").Trim() + ", ";
										else
										   query += "0, ";

										if(arrCamposLinea.GetValue(15).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(15).ToString().Replace("'","").Trim() + ", ";
										else
										   query += "0, ";

										if(arrCamposLinea.GetValue(12).ToString().Trim()!="")
										   query += arrCamposLinea.GetValue(12).ToString().Replace("'","").Trim() + ", ";
										else
										   query += "0, ";

										
										  query += "'" + arrCamposLinea.GetValue(17).ToString().Replace("'","").Trim() + "')";
									   
									
					
									
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
												//objTextFile.EscribirLog("  ---> ERROR -- EL REGISTRO DE DETALLE YA EXISTE EN LA BASE DE DATOS.  LINEA REPETIDA: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
									
												// HACER EL UPDATE DEL ENCABEZADO YA QUE EL REGISTRO EXISTE EN LA BASE DE DATOS
												//try
												//{
													// GENERA QUERY DE UPDATE DEL DETALLE
													query = "UPDATE FARMATODO_DETALLES_OC SET ";													
													query += " BULTOS = " + intBultos + ", ";													
													query += " COSTO_BULTO = " + dblCosto_Bulto + ", ";
													query += " COD_BARRA_PRODUCTO = '" + arrCamposLinea.GetValue(5).ToString().Replace("'","").Trim() + "', ";
													query += " COD_PROD_PROVEEDOR = '" + arrCamposLinea.GetValue(6).ToString().Replace("'","").Trim() + "', ";
													query += " DESC_PRODUCTO = '" + arrCamposLinea.GetValue(8).ToString().Replace("'","").Trim() + "', ";
													query += " CASE_ID = '" + arrCamposLinea.GetValue(17).ToString().Replace("'","").Trim() + "', ";
													
													if(arrCamposLinea.GetValue(9).ToString().Trim()!="")
													   query += " UNIDADES = " + arrCamposLinea.GetValue(9).ToString().Trim() + ", ";
													else
													   query += " UNIDADES = 0, ";
													
													if(arrCamposLinea.GetValue(11).ToString().Trim()!="")
													   query += " PVP_UNIDAD_RETAIL = " + arrCamposLinea.GetValue(11).ToString().Trim()+ ", ";
													else
													   query += " PVP_UNIDAD_RETAIL = 0, ";
													
													if(arrCamposLinea.GetValue(16).ToString().Trim()!="")
													   query += " PORCENTAJE_DESCUENTO = " + arrCamposLinea.GetValue(16).ToString().Trim() + ", ";
													else
													   query += " PORCENTAJE_DESCUENTO = 0, ";

													if(arrCamposLinea.GetValue(15).ToString().Trim()!="")
													   query += " MONTO_TOTAL_ITEM = " + arrCamposLinea.GetValue(15).ToString().Trim() + ", ";
													else
													   query += " MONTO_TOTAL_ITEM = 0, ";

													if(arrCamposLinea.GetValue(10).ToString().Trim()!="")
													   query += " UNIDADES_BULTO = " + arrCamposLinea.GetValue(10).ToString().Trim() + ", ";
													else
													   query += " UNIDADES_BULTO = 0, ";

													if(arrCamposLinea.GetValue(12).ToString().Trim()!="")													
													   query += " COSTO_UNIDAD_RETAIL = " + arrCamposLinea.GetValue(12).ToString().Trim();
													else
													   query += " COSTO_UNIDAD_RETAIL =0 ";

													query += " WHERE NUMERO_DOC ='" + strNumero_OC + "' ";
													query += " AND COD_PROV_HUB_TP = '" + strCodigoProveedorHub + "' ";													
													query += " AND SKU_FARMATODO = '" + arrCamposLinea.GetValue(7).ToString().Replace("'","").Trim() + "'";

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
													// UPDATE DEL REGISTRO DE DETALLE
													mySqlCommand = objBDatos.Comando(query, mySqlConnection);
													mySqlCommand.ExecuteNonQuery();	
												   //objTextFile.EscribirLog("  ---> detalle actualizado SKU: " + arrCamposLinea.GetValue(7).ToString());
													//objTextFile.EscribirLog(" QUERY: " + query);
													strTempNroDoc = strNumeroDoc;

												}
												catch (Exception ex2) 
												{
													Class1.strCodError = ex2.Message;
								
													objTextFile.EscribirLogError("  ---> ERROR -- ERROR REALIZANDO EL UPDATE DEL REGISTRO DE DETALLE REGISTRO DE DETALLE: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
													objTextFile.EscribirLogError(" QUERY: " + query);
												}

											}
												// EL ERROR AL INSERTAR EL REGISTRO DE DETALLE EN LA BASE DE DATOS --> NO ES POR DUPLICIDAD
											else
											{
												objTextFile.EscribirLogError("  ---> ERROR -- ERROR INSERTANDO EL REGISTRO DE DETALLE: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
												objTextFile.EscribirLogError("  ---> QUERY:" + query);

												// LLAMAR PROCESO DE DELETE DE DOCUMENTO
												DeleteOrdenCompra (strNumeroDoc, strCodigoProveedorHub, mySqlConnection);

											}
										} // CATCH								
							
									}
									else
									{
										objTextFile.EscribirLogError("  ---> ERROR -- EL CAMPO NÚMERO DOC, COD PROVEEDOR O SKU NO PUEDE SER UN VALOR NULO.  EL NUMERO DE DETALLE DENTRO DEL ARCHIVO ES: " + intNumeroLinea);
										strErrorDetalles = "SI";
									}
								} // IF DE strErrorDetalles = "SI";
					
	
						}						
						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						// ERROR DE TIPO DE REGISTRO INVALIDO NO ES NI '01' NI '02'
						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						else
						{
				
							// INCREMENTA EL NUMERO DE LA LINEA
							intNumeroLinea += 1;
				
							objTextFile.EscribirLogError("  ---> ERROR -- ERROR EN EL CONTENIDO DEL ARCHIVO DE ENTRADA.  TIPO DE REGISTRO INVALIDO EN LA LINEA: " + intNumeroLinea);

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

				objTextFile.EscribirLogError("  ---> ERROR -- ERROR LEYENDO EL ARCHIVO DE ENTRADA: " + strFileName + "  LINEA ARCHIVO: " + intNumeroLinea + "  CODIGO: " + Class1.strCodError);
				
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
								
				objTextFile.EscribirLogError("  ---> ERROR -- ERROR ELIMINANDO EL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
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
								
				objTextFile.EscribirLogError("  ---> ERROR -- ERROR ELIMINANDO LOS REGISTROS DE DETALLE DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
			}

		}
		#endregion
	
	}
}
