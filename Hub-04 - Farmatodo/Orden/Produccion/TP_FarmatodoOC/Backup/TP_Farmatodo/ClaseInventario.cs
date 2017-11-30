using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_FarmatodoOC
{
	/// <summary>
	/// Summary description for ClaseInventario.
	/// </summary>
	public class ClaseInventario
	{


		#region Clases
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		#endregion


		public ClaseInventario()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		public void InsertInventario(string strFileName)
		{


			string query = "";
			string strFecha_Doc;
			string strFecha_Generacion ;
			//string strTempNroDoc = "";
			string strErrorDetalles = "NO";
			string strErrorIntentos = "NO";
			int intReintentos = 4; // REINTENTA UNA VEZ MENOS DE LO ESPECIFICADO
			int intNumeroLinea = 0;
			int intNumeroEncabezado = 0;
			int intReintentosAplicados = 0;

			string strDecimal = System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator;

						
			try
			{
				// CREA UNA INSTANCIA DE StreamReader PARA LEER DEL ARCHIVO.
				using (StreamReader sr = new StreamReader(strFileName)) 
				{
					
					string strLineaLeida;
					Array arrCamposLinea;
					

					// VARIABLES ASIGNADAS PARA TRABAJAR
					string strCodigoProveedorHub = "";
					string strNumeroDoc = "   ";
					string strTempCodigoProveedorHub = "    ";


					// INSTANCIA LA CONEXION DE BASE DE DATOS
					SqlConnection mySqlConnection = new SqlConnection();
					SqlCommand mySqlCommand = new SqlCommand();
					mySqlConnection = objBDatos.Conexion();


					// LEE CADA UNA DE LAS LINEAS DEL ARCHIVO
					while ((strLineaLeida = sr.ReadLine()) != null) 
					{
						
						//SEPARA CADA UNO DE LOS CAMPOS DE LA LINEA LEIDA
						arrCamposLinea = strLineaLeida.Split('|');

						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						//DETERMINA EL TIPO DE REGISTRO ENCABEZADO
						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						if (arrCamposLinea.GetValue(0).ToString() == "H")
						{	
							
							if ((strCodigoProveedorHub == strTempCodigoProveedorHub) && (strErrorDetalles == "NO"))
							{
								// INSERT EN EL LOG DE LA CANTIDAD DE DETALLES INSERTADOS POR EL ENCABEZADO
								objTextFile.EscribirLog("SE INSERTARON " + (intNumeroLinea - 1) + " REGISTROS DE DETALLE PARA EL PROVEEDOR: " + strCodigoProveedorHub);
							}
							
							
							// INCREMENTA NUMERO DE ENCABEZADO
							intNumeroEncabezado += 1;
							
							// INCREMENTA EL NUMERO DE LA LINEA
							intNumeroLinea = 1;

							// ASIGNA VALORES A LAS VARIABLES DEL ENCABEZADO
							strCodigoProveedorHub = arrCamposLinea.GetValue(2).ToString();
							strNumeroDoc = arrCamposLinea.GetValue(1).ToString();

							// VERIDICA QUE EL NUMERO DEL DOCUMENTO O EL CODIGO DEL PROVEEDOR NO SEAN NULOS
							if (strNumeroDoc != "")
							{
								if (strCodigoProveedorHub != "")
								{
							
									// INICIALIZA LAS VARIABLES CONTADORES
									strErrorDetalles = "NO";
							
									strFecha_Doc = arrCamposLinea.GetValue(4).ToString().Substring(0,4) + arrCamposLinea.GetValue(4).ToString().Substring(4,2) + arrCamposLinea.GetValue(4).ToString().Substring(6,2);
									strFecha_Generacion = arrCamposLinea.GetValue(5).ToString().Substring(0,4) + arrCamposLinea.GetValue(5).ToString().Substring(4,2) + arrCamposLinea.GetValue(5).ToString().Substring(6,2);


									// GENERA QUERY DE INSERT EN BASE DE DATOS
									query = "INSERT INTO FARMATODO_ENCABEZADO_INVENTARIO ";
									query += " (NUMERO_DOC, FECHA_DOC, FECHA_GENERACION, CODIGO_COMPRADOR, NOMBRE_COMPRADOR, COD_PROV_HUB_TP, HUB_TP, STATUS_DOC) ";
									query += " VALUES ";
									query += " ('" + arrCamposLinea.GetValue(1).ToString().Replace("'","") + "', CONVERT(DATETIME, '" + strFecha_Doc + "'), CONVERT(DATETIME, '" + strFecha_Generacion + "'), '" + arrCamposLinea.GetValue(6).ToString().Replace("'","") + "', 'Farmatodo S.A.', '" + arrCamposLinea.GetValue(2).ToString().Replace("'","") + "', 'Hub-04', '100') ";
									

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

										strTempCodigoProveedorHub = strCodigoProveedorHub;
										
										
									}
									catch (Exception ex) 
									{
										Class1.strCodError = ex.Message;
										objTextFile.EscribirLog("  ---> ERROR -- ERROR INSERTANDO EL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + "   " + query + "    DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
										strErrorDetalles = "SI";

										
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
						else if(arrCamposLinea.GetValue(0).ToString() == "D")
						{
							
							if (strErrorDetalles == "NO")
							{
							
								// INCREMENTA EL NUMERO DE LA LINEA
								intNumeroLinea += 1;

								// GENERA QUERY DE INSERT EN BASE DE DATOS
								query = "INSERT INTO FARMATODO_DETALLES_INVENTARIO ";
								query += " (NUMERO_DOC, COD_PROV_HUB_TP, CODIGO_PROD_COMPRADOR, CODIGO_PROD_BARRAS, CODIGO_PROD_PROVEEDOR, CANTIDAD_ORDENADA, CANTIDAD_COMPROMETIDA, CANTIDAD_MINIMA, CANTIDAD_MAXIMA, CANTIDAD_EXISTENCIA, CANTIDAD_REORDEN, CANTIDAD_TRANSITO, UNIDAD_MEDIDA, CODIGO_LOCALIZACION, VENTAS_DIARIA, DESCRIPCION, HUB_TP) ";
								query += " VALUES ";
								query += " ('" + arrCamposLinea.GetValue(1).ToString().Replace("'","") + "', '" + arrCamposLinea.GetValue(2).ToString().Replace("'","") + "', '" + arrCamposLinea.GetValue(3).ToString().Replace("'","") + "', '" + arrCamposLinea.GetValue(4).ToString().Replace("'","") + "', '" + arrCamposLinea.GetValue(5).ToString().Replace("'","") + "', " + arrCamposLinea.GetValue(6) + ", " + arrCamposLinea.GetValue(7) + ", " + arrCamposLinea.GetValue(8) + ", " + arrCamposLinea.GetValue(9) + ", " + arrCamposLinea.GetValue(10) + ", " + arrCamposLinea.GetValue(11) + ", ";
								query += " " + arrCamposLinea.GetValue(12) + ", '" + arrCamposLinea.GetValue(13).ToString().Replace("'","") + "', '" + arrCamposLinea.GetValue(14).ToString().Replace("'","") + "', " + arrCamposLinea.GetValue(15) + ", '" + arrCamposLinea.GetValue(16).ToString().Replace("'","") + "', 'Hub-04') ";


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

									objTextFile.EscribirLog("  ---> ERROR -- ERROR INSERTANDO EL REGISTRO DE DETALLE: " + (intNumeroLinea - 1) + "  DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
								}
							} // IF DE strErrorDetalles = "SI";
					
						}

						// ERROR DE TIPO DE REGISTRO INVALIDO NO ES NI 'H' NI 'D'
						else
						{
						
							// INCREMENTA EL NUMERO DE LA LINEA
							intNumeroLinea += 1;
						
							objTextFile.EscribirLog("  ---> ERROR -- ERROR EN EL CONTENIDO DEL ARCHIVO DE ENTRADA.  TIPO DE REGISTRO INVALIDO EN LA LINEA: " + intNumeroLinea);

							// SE SALE DEL WHILE
							break;

						}

					}


					// INSERT EN EL LOG DE LA CANTIDAD DE DETALLES INSERTADOS POR EL ENCABEZADO
					objTextFile.EscribirLog("SE INSERTARON " + (intNumeroLinea - 1) + " REGISTROS DE DETALLE PARA EL PROVEEDOR: " + strCodigoProveedorHub);
					
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

				objTextFile.EscribirLog("  ---> ERROR -- ERROR LEYENDO EL ARCHIVO DE ENTRADA: " + strFileName + "  CODIGO: " + Class1.strCodError);
				
			}

		}

	}
}
