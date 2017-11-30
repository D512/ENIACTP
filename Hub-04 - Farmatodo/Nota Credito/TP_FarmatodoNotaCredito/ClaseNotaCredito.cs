using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace NotaCredito
{
	/// <summary>
	/// Summary description for ClaseOrdenCompra.
	/// </summary>
	public class ClaseNotaCredito
	{		
		// INSTANCIA LA CONEXION DE BASE DE DATOS
		ClaseBaseDatos objDatos=new ClaseBaseDatos();
		SqlConnection conexion = new SqlConnection();
		SqlCommand comando = new SqlCommand();
		SqlDataReader cursor;
		
		int intentos_conexion=4;
		string query = "";
		
		public ClaseNotaCredito()
		{
			// CONSTRUCTOR POR DEFECTO
		}

		public void InsertNotaCredito(string strFileName)
		{
			string cod_prov_hub_tp="";
			string numero_doc="";
			string codigo_proveedor="";
			string strFecha_doc = "";
			string strFecha_venc = "";
			string strDecimal = System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator;
			string strLineaLeida="";
			string strErrorConex="";
			string codigo_comprador1="";
			string codigo_comprador2="";
			string hub_tp="";
			string tabla="";
			bool abierta=false;
			bool error_detalle=true;
			bool error_encabezado=true;
			int i=1;
			int cant_det=0;
			int lineainterna=0;
			Array arrCamposLinea;

				// CREA UNA INSTANCIA DE StreamReader PARA LEER DEL ARCHIVO.
				using (StreamReader sr = new StreamReader(strFileName, System.Text.Encoding.Default)) 
				{					
					while((i<=intentos_conexion)&&(!abierta))
					{
						try
						{												
							conexion = objDatos.Conexion();
							conexion.Open();
							Class1.objTextFile.EscribirLog("CONEXION A LA BASE DE DATOS ESTABLECIDA EXITOSAMENTE");
							abierta=true;
						}
						catch(Exception e)
						{
							strErrorConex=e.Message;
							Class1.objTextFile.EscribirLog("INTENTO NUMERO: "+i.ToString()+" FALLIDO DE CONEXION A LA BASE DE DATOS. ERROR: "+strErrorConex+". SE HARÁ UN NUEVO INTENTO.");
							i+=1;
							Thread.Sleep(15000);
						}
					}
					
					if(abierta)
					{
						// LEE CADA UNA DE LAS LINEAS DEL ARCHIVO
						while ((strLineaLeida = sr.ReadLine()) != null) 
						{
						
							//SEPARA CADA UNO DE LOS CAMPOS DE LA LINEA LEIDA
							arrCamposLinea = strLineaLeida.Split(';');						

							// -------------------------------------------------------------------------------------
							// -------------------------------------------------------------------------------------
							//DETERMINA EL TIPO DE REGISTRO ENCABEZADO
							// -------------------------------------------------------------------------------------
							// -------------------------------------------------------------------------------------
						
							if (arrCamposLinea.GetValue(0).ToString() == "01")
							{							
								codigo_comprador1="";
								codigo_comprador2="";
																
								try 
								{
									//VERIFICACION LA EXISTENCIA DEL COMPRADOR DE ESTE ENCABEZADO DE FACTURA
									codigo_comprador1 = arrCamposLinea.GetValue(4).ToString().Trim();
									query = "SELECT * FROM NOTACREDITO_AUTOMATICA WHERE CODIGO_COMPRADOR = '" + codigo_comprador1 + "'";
									try
									{
										comando=objDatos.Comando(query,conexion);
										cursor=comando.ExecuteReader();
										while(cursor.Read())
										{
											hub_tp = cursor["hub_tp"].ToString().Trim();
											codigo_comprador2 = cursor["codigo_comprador"].ToString().Trim();
											tabla = cursor["tabla"].ToString().Trim();
										}
										cursor.Close();
									}
									catch(Exception e)
									{
										Class1.objTextFile.EscribirLog("  ---> ERROR REALIZANDO EL QUERY QUE VERIFICA LA EXISTENCIA DEL CODIGO_COMPRADOR EN LA BD");
										Class1.objTextFile.EscribirLog("     ---> ERROR :" + e.Message);
									}

									if((codigo_comprador2 == "")||(hub_tp == "")||(tabla == ""))
									{
										Class1.objTextFile.EscribirLog("NO SE PUEDE HACER LA INSERCION DEL REGISTRO DE ENCABEZADO DE NOTA CREDITO DEL PROVEEDOR: " + arrCamposLinea.GetValue(15).ToString() + " DEL NUMERO DE NOTA DE CREDITO: " + arrCamposLinea.GetValue(1).ToString()+", PORQUE NO EXISTE EL COMPRADOR ESPECIFICADO");
										error_encabezado=true;
									}
									else
									{								
										//SE VERIFICA LA CANTIDAD DE DETALLES DEL ENCABEZADO ANTERIOR
										if (cant_det>0)
										{
											if (!error_detalle)
												Class1.objTextFile.EscribirLog(" ---->SE INSERTARON: "+cant_det.ToString()+" DETALLES DE NOTA DE CREDITO DEL PROVEEDOR: " + cod_prov_hub_tp + " DEL N° NOTA CREDITO: " + numero_doc);
										
											else
											{
												Class1.objTextFile.EscribirLogError("  ---> ERROR -- SE HAN ENCONTRADO ERRORES AL REGISTRAR DETALLES DE NOTA DE CREDITO DEL PROVEEDOR: " + cod_prov_hub_tp + " DEL N° NOTA CREDITO:" + numero_doc + ", SE PROCEDERA A ELIMINAR EL ENCABEZADO Y REGISTROS HERMANOS");
												eliminarNotaCredito(cod_prov_hub_tp, numero_doc, tabla);
											}	
											cant_det=0;
											lineainterna=0;
										}
										else if (!error_encabezado)
										{
											Class1.objTextFile.EscribirLogError("  ---> ERROR -- REGISTRO DE ENCABEZADO DE NOTA DE CREDITO DEL PROVEEDOR: " + cod_prov_hub_tp + " DEL N° NOTA CREDITO: " + numero_doc + " NO POSEE DETALLES, SE PROCEDERA A ELIMINARLO");
											eliminarNotaCredito(cod_prov_hub_tp, numero_doc, tabla);
										}			
													
										//VALIDACION DE LOS CAMPOS REQUERIDOS

										if((arrCamposLinea.GetValue(15).ToString().Trim()!="")&&(arrCamposLinea.GetValue(1).ToString().Trim()!=""))
										{
										
											if(arrCamposLinea.GetValue(2).ToString().TrimStart().TrimEnd() == "") 
											{
												strFecha_doc = "NULL";
											}
											else
											{
												strFecha_doc = arrCamposLinea.GetValue(2).ToString().Trim().Substring(0,4) + arrCamposLinea.GetValue(2).ToString().Trim().Substring(4,2) + arrCamposLinea.GetValue(2).ToString().Trim().Substring(6,2);
											}

											if(arrCamposLinea.GetValue(3).ToString().TrimStart().TrimEnd() == "") 
											{
												strFecha_venc = "NULL";
											}
											else
											{
												strFecha_venc = arrCamposLinea.GetValue(3).ToString().Trim().Substring(0,4) + arrCamposLinea.GetValue(3).ToString().Trim().Substring(4,2) + arrCamposLinea.GetValue(3).ToString().Trim().Substring(6,2);
											}									
										
										
											// -------------------------------------------------------------------------------------
											// INSERCION DE LOS ENCABEZADOS EN LA TABLA
											// -------------------------------------------------------------------------------------
											query = "INSERT INTO " + tabla + "_ENCABEZADO_NOTACREDITO (hub_tp,cod_prov_hub_tp,status_doc,fecha_status,numero_doc,fecha_doc,fecha_vencimiento,numero_oc,nro_control,nro_factura,codigo_comprador,nombre_comprador,codigo_despacho,nombre_despacho,total_unidades,monto_total_descuento,monto_total_impuesto,monto_total_factura,nombre_proveedor)";
											query += "VALUES";
											query += " ('" + hub_tp + "','";
											query += arrCamposLinea.GetValue(15).ToString().Trim().Replace("'"," ") + "','120',";
											query += " CONVERT(DATETIME, GetDate()),'";
											query += arrCamposLinea.GetValue(1).ToString().Trim().Replace("'"," ")+ "',";
																		
											if(strFecha_doc == "NULL")
												query += " CONVERT(DATETIME, " + strFecha_doc + "), ";
											else
												query += "CONVERT(DATETIME, '" + strFecha_doc + "'), ";

											if(strFecha_venc == "NULL")
												query += " CONVERT(DATETIME, " + strFecha_venc + "),'";
											else
												query += "CONVERT(DATETIME, '" + strFecha_venc + "'),'";

											query += arrCamposLinea.GetValue(8).ToString().Trim().Replace("'"," ")+ "','";
											query += arrCamposLinea.GetValue(10).ToString().Trim().Replace("'"," ")+ "','";
											query += arrCamposLinea.GetValue(9).ToString().Trim().Replace("'"," ")+ "','";
											query += arrCamposLinea.GetValue(4).ToString().Trim().Replace("'"," ")+ "','";
											query += arrCamposLinea.GetValue(5).ToString().Trim().Replace("'"," ")+ "','";
											query += arrCamposLinea.GetValue(6).ToString().Trim().Replace("'"," ")+ "','";
											query += arrCamposLinea.GetValue(7).ToString().Trim().Replace("'"," ")+ "', ";
								
											if(arrCamposLinea.GetValue(11).ToString().Trim()!="")
												query += arrCamposLinea.GetValue(11).ToString()+ ", ";
											else
												query += "0, ";

											if(arrCamposLinea.GetValue(12).ToString().Trim()!="")
												query += arrCamposLinea.GetValue(12).ToString()+ ", ";
											else
												query += "0, ";				
								  
											if(arrCamposLinea.GetValue(13).ToString().Trim()!="")
												query += arrCamposLinea.GetValue(13)+ ", ";
											else
												query += "0, ";
								   
											if(arrCamposLinea.GetValue(14).ToString().Trim()!="")
												query += arrCamposLinea.GetValue(14)+ ",'";
											else
												query += "0,'";		

								
											query += arrCamposLinea.GetValue(16).ToString().Trim().Replace("'"," ")+ "');";													
						
											cod_prov_hub_tp = arrCamposLinea.GetValue(15).ToString().Trim();
											numero_doc= arrCamposLinea.GetValue(1).ToString().Trim();
										
											try
											{
												// INSERTA EL REGISTRO DE ENCABEZADO
												comando = objDatos.Comando(query, conexion);
												comando.ExecuteNonQuery();
												Class1.objTextFile.EscribirLog("INSERTADO EL ENCABEZADO DE NOTA CREDITO DEL PROVEEDOR: " + arrCamposLinea.GetValue(15).ToString() + " DEL N° NC: " + arrCamposLinea.GetValue(1).ToString());
												error_encabezado=false;
											}
											catch (Exception ex1)
											{
												Class1.objTextFile.EscribirLogError("  ---> ERROR -- ERROR REALIZANDO LA INSERCION DEL REGISTRO DE ENCABEZADO DE NC DEL PROVEEDOR: " + arrCamposLinea.GetValue(15).ToString() + " DEL N° NC: " + arrCamposLinea.GetValue(1).ToString());
												Class1.objTextFile.EscribirLogError("  ---> ERROR -- CODIGO: " + ex1.Message);
												Class1.objTextFile.EscribirLogError("  ---> ERROR -- QUERY: " + query);
												if((ex1.Message.Substring(0,24) != "Violation of PRIMARY KEY")||(ex1.Message.ToString().Substring(0,24)!="Violation of PRIMARY KEY"))
													error_encabezado=true;
											}
										}
										else
										{
											Class1.objTextFile.EscribirLogError(" --->ERROR. NO SE PUEDE HACER LA CARGA DEL ENCABEZADO DE NC DEL PROVEEDOR: " + arrCamposLinea.GetValue(15).ToString() + " DEL N° NC: " + arrCamposLinea.GetValue(1).ToString()+" PORQUE HAY CAMPOS OBLIGATORIOS VACIOS (Codigo Proveedor o Numero Documento)");
											error_encabezado=true;
										}
															
									}
								}
								catch (Exception ex1) 
								{
									Class1.objTextFile.EscribirLogError("  ---> ERROR -- SE HA ENCONTRADO ERRORES EN LA LECTURA DE ENCABEZADO"); 
									Class1.objTextFile.EscribirLogError("  ---> ERROR -- CODIGO: " + ex1.Message);
									Class1.objTextFile.EscribirLogError("  ---> ERROR -- QUERY: " + query);
									error_encabezado=true;												
								}
							}
							else if((arrCamposLinea.GetValue(0).ToString() == "02"))
							{
								if (!error_encabezado)
								{	
									try 
									{	
										//VALIDACION DE LOS CAMPOS OBLIGATORIOS
										if((arrCamposLinea.GetValue(12).ToString().Trim()!="")&&(arrCamposLinea.GetValue(1).ToString().Trim()!="")&&(arrCamposLinea.GetValue(3).ToString().Trim()!=""))
										{							
																				
										// -------------------------------------------------------------------------------------
										// INSERCION DE LOS DETALLES EN LA TABLA
										// -------------------------------------------------------------------------------------
							
										lineainterna += 1;
										query = "INSERT INTO " + tabla + "_DETALLES_NOTACREDITO (linea_interna,hub_tp,cod_prov_hub_tp,numero_doc,status_item,fecha_status,codigo_barra,codigo_proveedor,codigo_prod_comprador,descripcion_unidad,cantidad_facturada,precio_unidad,monto_linea,tasa_iva,monto_impuesto,monto_descuento)";
										query += " VALUES ";
										query += "("+ lineainterna + ",'" + hub_tp + "','";
										query += arrCamposLinea.GetValue(12).ToString().Trim().Replace("'"," ") + "','";
										query += arrCamposLinea.GetValue(1).ToString().Trim().Replace("'"," ")+ "','100',";
										query += " CONVERT(DATETIME, GetDate()),'";
										query += arrCamposLinea.GetValue(2).ToString().Trim().Replace("'"," ")+ "','";
										query += arrCamposLinea.GetValue(3).ToString().Trim().Replace("'"," ")+ "','";
										query += arrCamposLinea.GetValue(4).ToString().Trim().Replace("'"," ")+ "','";
										query += arrCamposLinea.GetValue(5).ToString().Trim().Replace("'"," ")+ "',";

										if(arrCamposLinea.GetValue(6).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(6).ToString()+ ",";
										else
											query += "0,";
  
										if(arrCamposLinea.GetValue(7).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(7).ToString()+ ",";
										else
											query += "0,";
  
										if(arrCamposLinea.GetValue(8).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(8).ToString()+ ", ";
										else
											query += "0, ";
								
										if(arrCamposLinea.GetValue(9).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(9).ToString()+ ",";
										else
											query += "0,";

										if(arrCamposLinea.GetValue(10).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(10).ToString()+ ",";
										else
											query += "0,";

										if(arrCamposLinea.GetValue(11).ToString().Trim()!="")
											query += arrCamposLinea.GetValue(11).ToString()+ ")";
										else
											query += "0)";
																									
										codigo_proveedor= arrCamposLinea.GetValue(3).ToString().Trim();
								  
										try
										{
											// INSERTA EL REGISTRO DE DETALLE
											comando = objDatos.Comando(query, conexion);
											comando.ExecuteNonQuery();
											cant_det+=1;
											error_detalle=false;
										}
										catch (Exception ex1)
										{
											Class1.objTextFile.EscribirLogError("  ---> ERROR -- ERROR REALIZANDO LA INSERCION DEL DETALLE DEL DOCUMENTO N°: " + arrCamposLinea.GetValue(1).ToString() + " DEL PROVEEDOR: " + arrCamposLinea.GetValue(12).ToString());
											Class1.objTextFile.EscribirLogError("  ---> ERROR -- CODIGO: " + ex1.Message);
											Class1.objTextFile.EscribirLogError("  ---> ERROR -- QUERY: " + query);
											if((ex1.Message.Substring(0,24) != "Violation of PRIMARY KEY")||(ex1.Message.ToString().Substring(0,24)!="Violation of PRIMARY KEY"))
											   error_detalle=true;
										}
										}
										else
										{
											Class1.objTextFile.EscribirLogError(" --->ERROR. NO SE PUEDE HACER LA CARGA DEL DETALLE DEL PROVEEDOR: " + arrCamposLinea.GetValue(12).ToString() + " DEL N° NC: " + arrCamposLinea.GetValue(1).ToString()+" DEL CODIGO PRODUCTO :"+arrCamposLinea.GetValue(3).ToString()+" PORQUE HAY CAMPOS OBLIGATORIOS VACIOS (Codigo Proveedor o Numero Documento o Codigo Producto)");
											error_detalle=true;
										}
									}
									catch (Exception ex1) 
									{
										Class1.objTextFile.EscribirLogError("  ---> ERROR -- SE HA ENCONTRADO ERRORES EN LA LECTURA DE DETALLES"); 
										Class1.objTextFile.EscribirLogError("  ---> ERROR -- CODIGO: " + ex1.Message);
										Class1.objTextFile.EscribirLogError("  ---> ERROR -- QUERY: " + query);
										error_detalle=true;
												
									}
									//SE VERIFICA SI SE HA GENERADO UN ERROR EN LA INSERCION DE ALGUN DETALLE PARA HACER EL DELETE DE SU ENCABEZADO Y DETALLES HERMANOS
									if(error_detalle)
									{
										Class1.objTextFile.EscribirLogError("  ---> ERROR -- SE HAN ENCONTRADO ERRORES AL REGISTRAR DETALLES DE NOTA DE CREDITO DEL PROVEEDOR: " + cod_prov_hub_tp + " DEL N° NOTA CREDITO: " + numero_doc + ", SE PROCEDERA A ELIMINAR EL ENCABEZADO Y REGISTROS HERMANOS");
										eliminarNotaCredito(cod_prov_hub_tp,numero_doc,tabla);
										cant_det=0;
										error_encabezado=true;
									}
								}
								else 
									Class1.objTextFile.EscribirLogError("  ---> ERROR NO EXISTE UN ENCABEZADO EN EL ARCHIVO PLANO DE ENTRADA PARA EL DETALLE DE NOTA DE CREDITO DEL PROVEEDOR:" + arrCamposLinea.GetValue(12).ToString() + " DEL N° NOTA CREDITO: " + arrCamposLinea.GetValue(1).ToString());
							}
							else
								Class1.objTextFile.EscribirLogError("  ---> ERROR EN LA LINEA LEÍDA DEL ARCHIVO PLANO DE ENTRADA, NO ES NI ENCABEZADO(01) NI ES DETALLE(02).");
						}

						//SE VERIFICA LA CANTIDAD DE DETALLES DEL ENCABEZADO ANTERIOR
						if(cant_det>0)
						{
							if (!error_detalle)
								Class1.objTextFile.EscribirLog(" ---->SE INSERTARON: "+cant_det.ToString()+" DETALLES DE NC DEL PROVEEDOR: " + cod_prov_hub_tp + " DEL N° NC: " + numero_doc);
							else
							{
								Class1.objTextFile.EscribirLogError("  ---> ERROR -- SE HAN ENCONTRADO ERRORES AL REGISTRAR DETALLES DE NOTA DE CREDITO DEL PROVEEDOR: " + cod_prov_hub_tp + " DEL N° NOTA CREDITO:" + numero_doc + ", SE PROCEDERA A ELIMINAR EL ENCABEZADO Y REGISTROS HERMANOS");
								eliminarNotaCredito(cod_prov_hub_tp, numero_doc, tabla);
							}
							cant_det=0;
						}
						else if (!error_encabezado)
						{
							Class1.objTextFile.EscribirLogError("  ---> ERROR -- REGISTRO DE ENCABEZADO DE NOTA DE CREDITO DEL PROVEEDOR: " + cod_prov_hub_tp + " DEL N° NC: " + numero_doc + " NO POSEE DETALLES, SE PROCEDERA A ELIMINARLO");
							eliminarNotaCredito(cod_prov_hub_tp, numero_doc, tabla);
						}

						//CIERRA LA BD
						conexion.Close();
					}
					else
						Class1.objTextFile.EscribirLog("NO SE PUDO ESTABLECER LA CONEXION A LA BASE DE DATOS. ERROR: "+strErrorConex);
				}			
		}//FIN DEL METODO QUE INSERTA NOTA DE CREDITO
		
		
		//METODO QUE ELIMINA UNA NOTA DE CREDITO EN LA BD
		public void eliminarNotaCredito(string cod_prov_hub_tp,string numero_doc,string tabla)
		{
			// ELIMINA EL REGISTRO DE ENCABEZADO
			query = "DELETE FROM "+tabla+"_ENCABEZADO_NOTACREDITO WHERE COD_PROV_HUB_TP = '" + cod_prov_hub_tp + "' ";
            query += "AND NUMERO_DOC = '" + numero_doc + "'";

			try
			{
				comando = objDatos.Comando(query, conexion);
				comando.ExecuteNonQuery();
                Class1.objTextFile.EscribirLog("ELIMINADO DE LA BD EL ENCABEZADO DEL PROVEEDOR: " + cod_prov_hub_tp + ", DEL N° NC: " + numero_doc);
			}
			catch (Exception ex1)
			{
				Class1.objTextFile.EscribirLogError("  ---> ERROR -- ERROR REALIZANDO EL DELETE DEL ENCABEZADO DEL PROVEEDOR: " + cod_prov_hub_tp + ", DEL N° NC: " + numero_doc);
				Class1.objTextFile.EscribirLogError("  ---> ERROR -- CODIGO: " + ex1.Message);
				Class1.objTextFile.EscribirLogError("  ---> ERROR -- QUERY: " + query);
			}

			// ELIMINA EL REGISTRO DE DETALLE
			query = "DELETE FROM "+tabla+"_DETALLES_NOTACREDITO WHERE COD_PROV_HUB_TP = '" + cod_prov_hub_tp + "' ";
			query += "AND NUMERO_DOC = '" + numero_doc + "'";

			try
			{
				comando = objDatos.Comando(query, conexion);
				comando.ExecuteNonQuery();
				Class1.objTextFile.EscribirLog("ELIMINADOS DE LA BD DETALLES DEL PROVEEDOR: " + cod_prov_hub_tp + ", DEL N° NC: " + numero_doc);
			}
			catch (Exception ex1)
			{
				Class1.objTextFile.EscribirLogError("  ---> ERROR -- ERROR REALIZANDO EL DELETE DE LOS DETALLES DEL PROVEEDOR: " + cod_prov_hub_tp + ", DEL N° NC: " + numero_doc);
				Class1.objTextFile.EscribirLogError("  ---> ERROR -- CODIGO: " + ex1.Message);
				Class1.objTextFile.EscribirLogError("  ---> ERROR -- QUERY: " + query);
			}

		}//FIN DEL METODO QUE ELIMINA UNA NOTA DE CREDITO DE LA BD

		//METODO QUE ACTUALIZA EL STATUS DE OC DE UNA FACTURA
		public void actualizarStatus(string cod_prov_hub_tp,string numero_doc,string tabla)
		{
            query = "UPDATE " + tabla + "_ENCABEZADO_TOTALES_OC SET STATUS_DOC = '501'";
            query += " WHERE COD_PROV_HUB_TP = '" + cod_prov_hub_tp + "' AND NUMERO_DOC = '" + numero_doc + "'";
 
			try
			{
				comando = objDatos.Comando(query, conexion);
				comando.ExecuteNonQuery();
			}
			catch (Exception ex1)
			{
				Class1.objTextFile.EscribirLogError("  ---> ERROR -- ERROR REALIZANDO LA ACTUALIZACION EL STATUS DE OC DE LA FACTURA DE ENCABEZADO DEL PROVEEDOR: " + cod_prov_hub_tp + ", DEL NUMERO DE FACTURA: " + numero_doc);
				Class1.objTextFile.EscribirLogError("  ---> ERROR -- CODIGO: " + ex1.Message);
				Class1.objTextFile.EscribirLogError("  ---> ERROR -- QUERY: " + query);
			}
		}//FIN DEL METODO DE ACTUALIZACION DE LOS STATUS OC
	}
}
