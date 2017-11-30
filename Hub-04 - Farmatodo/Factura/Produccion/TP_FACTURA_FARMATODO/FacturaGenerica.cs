using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_FacturaFarmatodo
{
	/// <summary>
	/// Summary description for ClaseOrdenCompra.
	/// </summary>
	public class ClaseFacturaFarmatodo
	{
		
		#region Clases
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		public static Email objEmail = new Email();
		

		#endregion

		public ClaseFacturaFarmatodo()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		public void InsertFacturaFarmatodo(string strFileName)
		{

			string query = "";
			string strTempNroDoc = "";
			string strErrorDetalles = "NO";
			string strErrorEncabezados = "NO";
			string strErrorIntentos = "NO";
			string strTemporal="NO";
			int intReintentos = 4; // REINTENTA UNA VEZ MENOS DE LO ESPECIFICADO
			int intNumeroLinea = 0;
			int intContDetalle = 0;
			int intNumeroEncabezado = 0;
			int intReintentosAplicados = 0;
			string strTempNumero_doc = "";
			string strTempCod_prov_hub_tp = "";
			string strTempNombreProveedor= "";
			string strNombreProveedor= "";
			string strTempCodProd = "";
			string strTempNumLote= "";
			string strCodigoProveedorHub = "";
			string strTempBonificado= "";


			//FACTURA GENERICA 
			string strFecha_emision = "";
			string strFecha_vencimiento = "";
			double dobDescuento_general = 0;
			double dobDescuento_general_BsF = 0;
			double dobSubTotal_Factura = 0;
			double dobTotal_dcto_adicional = 0;
			double dobTotal_factura = 0;
			double dobImpuesto = 0;
			double dobIva = 0;
			double dobMontoTotal_DescuentoInvolutivos = 0;
			double dobMontoTotal_DescuentoPromocion = 0;
			double dobMontoTotal_DescuentoVolumen = 0;
			double dobMontoTotal_MontoGravable = 0; 
			double dobMontoTotal_ProntoPago = 0;
			double dobDescuento_FOB = 0 ;
			double dobDias_CondicionPago = 0; 
			double dobTotal_Bulto = 0; 
			double dobTotal_Unidad = 0; 

			string strFecha_vencimiento_det = "";
			double dobUnidades_bulto = 0;
			double dobCantidad_facturada = 0;
			double dobUnidades_bonificada = 0;
			double dobPrecio_unidad_neto = 0;
			double dobPrecio_Neto= 0;
			double dobTotal_linea = 0;
			double dobTasa_Iva = 0;
			double dobPorcentaje_Descuento = 0;
			double dobMonto_Descuento = 0;
			double dobPorcentaje_Descuento_indevolutivo = 0;
			double dobMonto_Descuento_Indevolutivo = 0;
			double dobPorcentaje_Descuento_Promocion = 0;
			double dobMonto_Descuento_Promocion = 0;
			double dobPorcentaje_Descuento_Volumen = 0;
			double dobMonto_Descuento_Volumen = 0;
			double dobPorcentaje_Descuento_Global = 0;
			double dobMonto_Descuento_Global = 0;
			double dobPvp_Unidad= 0;

			bool Existe=false;

			string TipoRegistro = "";
			string ErrorTipoRegistro = "";
			string strTempCodigoProveedorHub = "";
			string TempTipoRegistro = "";

			//para el correo
			string strBody="";
			string strEnvioCorreo="NO";
			string strEmail="";
			string strEmailCuerpo="";
			bool strErrorRegD=false;
			bool strCargaEnc=false;
			bool strCargaDet=false;


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

						//SEPARA CADA UNO DE LOS CAMPOS DE LA LINEA LEIDA
						//strLineaLeida = strLineaLeida.Replace(";","|");
                        arrCamposLinea = strLineaLeida.Split(';');


						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						//DETERMINA EL TIPO DE REGISTRO ENCABEZADO
						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						if (arrCamposLinea.GetValue(0).ToString().TrimStart().TrimEnd() == "01")
						{	
/*
							// -------------------------------------------------------------------------------------
							// EVALUO SI EXISTEN ERRORES PARA MANDAR A BORRAR
							// -------------------------------------------------------------------------------------
							if((strErrorDetalles == "SI") && (strErrorEncabezados != "SI"))
							{
								DeleteFacturaGenerica (strTempNroDoc, strTempCodigoProveedorHub, mySqlConnection);
							}
*/
							
							// -------------------------------------------------------------------------------------
							// MENSAJE TOTAL DE DETALLES INSERTADOS POR ENCABEZADO
							// -------------------------------------------------------------------------------------
							if ((strNumeroDoc == strTempNroDoc) && (strErrorDetalles == "NO")&& (strErrorEncabezados=="NO"))
							{
							
								if(intNumeroLinea-1 == 0)
								{
									objTextFile.EscribirLog("");
									objTextFile.EscribirLog("  ---> ERROR -- DOCUMENTO NUMERO: " + strNumeroDoc + " NO CONTIENE DETALLE(S)");
								}
								else
								{
								
									if(strCargaEnc==true && strCargaDet==true)
									{
										//INSERT EN EL LOG DE LA CANTIDAD DE DETALLES INSERTADOS POR EL ENCABEZADO
										objTextFile.EscribirLog("SE INSERTARON Y/O MODIFICARON " + (intContDetalle) + " REGISTROS DE DETALLE PARA EL DOCUMENTO NUMERO: " + strNumeroDoc);
									}
										// REINICIA EL CONTADOR DE DETALLES
									intContDetalle = 0;	
									ErrorTipoRegistro = "NO";
								}
							}


							//AGREGA UNA LINEA ENTRE FACTURAS DE UN MISMO PROVEEDOR 
							if((strNumeroDoc == strTempNroDoc) && (strEnvioCorreo=="SI") && (strErrorDetalles == "SI") || (strErrorEncabezados=="SI"))
							{
									strEmailCuerpo+="<br>";
							}


							// ASIGNA VALORES A LAS VARIABLES DEL ENCABEZADO strNumeroDoc, strCodigoProveedorHub y strNombreProveedor

							try
							{
								strNumeroDoc = arrCamposLinea.GetValue(1).ToString().Replace("'", "").Replace("|","").Trim();
									
								if (strNumeroDoc == "")
								{
									objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(2) NUMERO DE FACTURA " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(2) Número Factura. Viene vacio y es un campo requerido.";
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

							}
							catch
							{

								objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(2) NUMERO DE FACTURA " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
								strEmailCuerpo+= "<br>";
								strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(2) Número Factura. Viene vacio y es un campo requerido.";
								strErrorEncabezados = "SI";
								ErrorTipoRegistro = "SI";
								strEnvioCorreo="SI";

							}

							try
							{
								strCodigoProveedorHub = arrCamposLinea.GetValue(18).ToString().Replace("'", "").Replace("|","").Trim();
							
								if(strCodigoProveedorHub == "")
								{
									objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(19) CODIGO PROVEEDOR " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(19) Codigo Proveedor. Viene vacio y es un campo requerido.";
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

							}
							catch
							{

								objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(19) CODIGO PROVEEDOR " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
								strEmailCuerpo+= "<br>";
								strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(19) Codigo Proveedor. Viene vacio y es un campo requerido.";
								strErrorEncabezados = "SI";
								ErrorTipoRegistro = "SI";
								strEnvioCorreo="SI";

							}

							try
							{

								strNombreProveedor = arrCamposLinea.GetValue(19).ToString().Replace("'", "").Replace("|","").Trim();
								
								if(strNombreProveedor == "")
								{
									objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(20) NOMBRE PROVEEDOR " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(20) Nombre Proveedor. Viene vacio y es un campo requerido.";
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

							}
							catch
							{
								objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(20) NOMBRE PROVEEDOR " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
								strEmailCuerpo+= "<br>";
								strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(20) Nombre Proveedor. Viene vacio y es un campo requerido.";
								strErrorEncabezados = "SI";
								ErrorTipoRegistro = "SI";
								strEnvioCorreo="SI";
							
							}

							
							#region ENVIO DE CORREO DE ERRORES DE CARGA FACTURA ELECTRONICA
							

							if((strEnvioCorreo=="SI") && (strTempCodigoProveedorHub!="") && ( strCodigoProveedorHub != strTempCodigoProveedorHub))
							{

								#region BUSCA LOS CORREOS

								query = " SELECT * FROM errores_carga_factura where hub_tp = '" + Class1.strHub + "' ";
								query += " and cod_prov_hub_tp = '" + strTempCodigoProveedorHub + "' ";


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
									objTextFile.EscribirLog("  ---> ERROR: " + Class1.strCodError);
									objTextFile.EscribirLog(" QUERY: " + query);
									
								}

								while (myReader.Read())
								{
									strEmail = myReader["correo"].ToString();
									break;
								}

								myReader.Close();

                                if (strEmail == "")
                                {
                                    strEmail = Class1.strEmailCC;
                                }
                               

								#endregion 
								if (strEmail != "")
								{
									try
									{

										strBody= "<center><strong>FACTURAS NO CARGADAS</strong></center>";
										strBody+="   <br><br>";
										strBody+="Facturas Enviadas a: " + Class1.strCliente + "<br>" ;  
										strBody+="Nombre del Proveedor: " + strTempNombreProveedor + "<br>";
										strBody+="Código del Proveedor: " +strTempCodigoProveedorHub + "<br>";  
										strBody+="   <br>";
										strBody+= strEmailCuerpo;

                                        bool OK = objEmail.EnviarEmail("mailing@tradeplace.net", strEmail, Class1.strEmailCC, "Errores Carga Factura", strBody, Class1.strServer);
										if (OK)
										{
											objTextFile.EscribirLog("--> EMAIL ERRORES DE CARGA FACTURA ELECTRONICA EXITOSO... ");
																		
										}
							
									}
									catch (Exception ex1)
									{
										Class1.strCodError = ex1.Message;
										objTextFile.EscribirLog("  ---> ERROR -- ERROR ENVIANDO EL EMAIL ERRORES DE CARGA FACTURA ELECTRONICA.  CODIGO: " + Class1.strCodError);								
									}
								}

								//LIMPIA VARIABLES DE CORREO
								strEmailCuerpo="";
								strEmail="";
								strEnvioCorreo="NO";

							}
							else
							{
								if (strTempCodigoProveedorHub=="")
								{
									//LIMPIA VARIABLES DE CORREO
									strEmailCuerpo="";
									strEmail="";
									strEnvioCorreo="NO";
								}
							
							}

							#endregion
						

							string strFecha = Convert.ToString(System.DateTime.Now);
							strErrorDetalles = "NO";
							ErrorTipoRegistro = "NO";
							strErrorEncabezados="NO";
							intContDetalle = 0;	
							strTemporal="NO";
							strErrorRegD=false;
							strCargaEnc=false;
							strCargaDet=false;
							

							#region //VERIFICA LA FACTURA EN BASE DE DATOS					
								
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
								strErrorEncabezados = "SI";
								ErrorTipoRegistro = "SI";
								objTextFile.EscribirLog("  ---> ERROR LEYENDO EL ENCABEZADO CODIGO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
								objTextFile.EscribirLog("  ---> ERROR -- QUERY: " + query);
								strEmailCuerpo+= "<br>";
								strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Error la factura no se pudo cargar. " + Class1.strCodError;
								strEnvioCorreo="SI";

							}

							while (myReader.Read())
							{
								strTempNumero_doc = myReader["numero_doc"].ToString();
								strTempCod_prov_hub_tp = myReader["cod_prov_hub_tp"].ToString();
								break;
							}

							myReader.Close();

							#endregion



							// -------------------------------------------------------------------------------------
							// EVALUA SI EL REGISTRO EXISTE
							// -------------------------------------------------------------------------------------
							if ((strTempNumero_doc == strNumeroDoc) && (strTempCod_prov_hub_tp == strCodigoProveedorHub))
							{

								// INSERT EN EL LOG LA ACTUALIZACION DE TOTALES DEL ENCABEZADO
								objTextFile.EscribirLog("REGISTRO DE ENCABEZADO YA EXISTE PARA EL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);
								strEmailCuerpo+= "<br>";
								strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc +": Registro ya existe.";
								strErrorEncabezados = "SI";
								ErrorTipoRegistro = "SI";
								strEnvioCorreo="SI";
								strTemporal="SI";

							}//FIN IF UPDATE
							else
							{

								#region VALIDA CAMPOS REQUERIDOS DEL ENCABEZADO

								try
								{
									if(arrCamposLinea.GetValue(2).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(3) FECHA EMISION: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(3) Fecha Emision. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										strFecha_emision = arrCamposLinea.GetValue(2).ToString().Trim().Substring(0,4) + arrCamposLinea.GetValue(2).ToString().Trim().Substring(4,2) + arrCamposLinea.GetValue(2).ToString().Trim().Substring(6,2);
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(3) FECHA EMISION: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(3) Fecha Emision. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if (arrCamposLinea.GetValue(3).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(4) CODIGO GLN " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(4) Código GLN. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
								}
								catch(Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(4) CODIGO GLN " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc+ ": Campo(4) Código GLN. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if (arrCamposLinea.GetValue(4).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(5) NOMBRE COMPRADOR " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(5) Nombre Comprador. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}

								}
								catch(Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(5) NOMBRE COMPRADOR " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(5) Nombre Comprador. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if (arrCamposLinea.GetValue(5).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(6) NUMERO DE ORDEN ASOCIADO A LA FACTURA " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(6) Numero O/C asociado a la factura. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
								
								}
								catch(Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(6) NUMERO DE ORDEN ASOCIADO A LA FACTURA " + "DOCUMENTO NUMERO: " + strNumeroDoc + " ,LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(6) Numero O/C asociado a la factura. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(6).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(7) FECHA VENCIMIENTO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(7) Fecha Vencimiento. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										strFecha_vencimiento = arrCamposLinea.GetValue(6).ToString().Trim().Substring(0,4) + arrCamposLinea.GetValue(6).ToString().Trim().Substring(4,2) + arrCamposLinea.GetValue(6).ToString().Trim().Substring(6,2);
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(7) FECHA VENCIMIENTO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(7) Fecha Vencimiento. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(7).ToString().Trim() == "")
									{
                                        dobDescuento_general = 0.0;
									}
									else
									{
										dobDescuento_general = Convert.ToDouble(arrCamposLinea.GetValue(7).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(8) DESCUENTO GENERAL %: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(8) Descuento General (%). " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(8).ToString().Trim() == "")
									{
                                        dobDescuento_general_BsF = 0.0;
									}
									else
									{
										dobDescuento_general_BsF = Convert.ToDouble(arrCamposLinea.GetValue(8).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(9) DESCUENTO GENERAL Bs. F: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(9) Descuento General (Bs.F.). " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}


								try
								{
									if (arrCamposLinea.GetValue(9).ToString().Trim()== "")
									{
										objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(10) CONDICION DE PAGO " + "DOCUMENTO NUMERO: " + strNumeroDoc + " .LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(10) Condición de Pago. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
								}
								catch(Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("ENCABEZADO - SE REQUIERE VALOR PARA EL CAMPO(10) CONDICION DE PAGO " + "DOCUMENTO NUMERO: " + strNumeroDoc + " .LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc+ ": Campo(10) Condición de Pago. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(10).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(11) DIAS CONDICION DE PAGO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(11) Días Condición d/Pago. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobDias_CondicionPago = Convert.ToDouble(arrCamposLinea.GetValue(10).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(11) DIAS CONDICION DE PAGO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(11) Días Condición d/Pago. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(12).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(13) TOTAL BULTOS: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(13) Total Bultos Facturados. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobTotal_Bulto = Convert.ToDouble(arrCamposLinea.GetValue(12).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(13) TOTAL BULTOS: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(13) Total Bultos Facturados. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
								
								try
								{
									if(arrCamposLinea.GetValue(13).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(14) TOTAL UNIDAD: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(14) Total Unidades Facturados. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobTotal_Unidad = Convert.ToDouble(arrCamposLinea.GetValue(13).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(14) TOTAL UNIDAD: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(14) Total Unidades Facturados. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(14).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(15) SUB-TOTAL FACTURA: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(15) Sub-Total Factura. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobSubTotal_Factura = Convert.ToDouble(arrCamposLinea.GetValue(14).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(15) SUB-TOTAL FACTURA: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(15) Sub-Total Factura. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
							
								try
								{
									if(arrCamposLinea.GetValue(15).ToString().Trim() == "")
									{
                                        dobTotal_dcto_adicional = 0.0;
									}
									else
									{
										dobTotal_dcto_adicional = Convert.ToDouble(arrCamposLinea.GetValue(15).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(16) TOTAL DESCUENTO ADICIONAL: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(16) Monto Total Descuento Adicional. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
							

								try
								{
									if(arrCamposLinea.GetValue(16).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(17) IMPUESTO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(17) Monto Total Impuesto. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobImpuesto = Convert.ToDouble(arrCamposLinea.GetValue(16).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(17) IMPUESTO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(17) Monto Total Impuesto. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(17).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(18) TOTAL FACTURA: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(18) Monto Total Factura. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobTotal_factura = Convert.ToDouble(arrCamposLinea.GetValue(17).ToString().Trim().Replace(",","."));
									
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(18) TOTAL FACTURA: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(18) Monto Total Factura. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
	
								try
								{
									if(arrCamposLinea.GetValue(22).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(23) IVA: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(23) % IVA. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobIva = Convert.ToDouble(arrCamposLinea.GetValue(22).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(23) IVA: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(23) % IVA. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
							
								try
								{
									if(arrCamposLinea.GetValue(23).ToString().Trim() == "")
									{
										dobMontoTotal_DescuentoInvolutivos = 0.0;
									}
									else
									{
										dobMontoTotal_DescuentoInvolutivos = Convert.ToDouble(arrCamposLinea.GetValue(23).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(24) TOTAL DESCUENTOS INDEVOLUTIVOS: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(24) Monto Total Descuento Indevolutivos. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(24).ToString().Trim() == "")
									{
										dobMontoTotal_DescuentoPromocion = 0.0;
									}
									else
									{
										dobMontoTotal_DescuentoPromocion = Convert.ToDouble(arrCamposLinea.GetValue(24).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(25) DESCUENTO POR PROMOCION: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(25) Monto Total Descuento por Promociones. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(25).ToString().Trim() == "")
									{
										dobMontoTotal_DescuentoVolumen = 0.0;
									}
									else
									{
										dobMontoTotal_DescuentoVolumen = Convert.ToDouble(arrCamposLinea.GetValue(25).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(26) DESCUENTO POR VOLUMEN: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(26) Monto Total Descuento por Volumen. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
						
							
								try
								{
									if(arrCamposLinea.GetValue(26).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(27) MONTO GRAVABLE: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Campo(27) Monto Gravable. Viene vacio y es un campo requerido.";
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobMontoTotal_MontoGravable = Convert.ToDouble(arrCamposLinea.GetValue(26).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(27) MONTO GRAVABLE: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(27) Monto Gravable. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(27).ToString().Trim() == "")
									{
										dobMontoTotal_ProntoPago = 0.0;
									}
									else
									{
										dobMontoTotal_ProntoPago = Convert.ToDouble(arrCamposLinea.GetValue(27).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(28) DESCUENTO PRONTO PAGO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(28) Descuento por Pronto Pago. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}


							
								try
								{
									if(arrCamposLinea.GetValue(28).ToString().Trim() == "")
									{
										dobDescuento_FOB = 0.0;
									}
									else
									{
										dobDescuento_FOB = Convert.ToDouble(arrCamposLinea.GetValue(28).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(29) DESCUENTO FOB: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(29) Descuento FOB. " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}


								try
								{
									if(arrCamposLinea.GetValue(29).ToString().Trim() == "")
									{
										dobPorcentaje_Descuento_Global = 0.0;
									}
									else
									{
										dobPorcentaje_Descuento_Global = Convert.ToDouble(arrCamposLinea.GetValue(29).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(30) Descuento Global (%): " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(30) Descuento Global (%). " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(30).ToString().Trim() == "")
									{
										dobMonto_Descuento_Global = 0.0;
									}
									else
									{
										dobMonto_Descuento_Global = Convert.ToDouble(arrCamposLinea.GetValue(30).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DE ENCABEZADO EN EL CAMPO(31) Descuento Global (Bs.F.): " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-ENCABEZADO Factura N°" + strNumeroDoc+ ": Campo(31) Descuento Global (Bs.F.). " + Class1.strCodError;
									strErrorEncabezados = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
						
		
								#endregion						
							


								#region  // EVALUA QUE NO EXISTA ERROR EN EL ENCABEZADO

								if(strErrorEncabezados == "NO")

								{
									// INCREMENTA EL NUMERO DE ENCABEZADO
									intNumeroEncabezado += 1;	
						
									query = " INSERT INTO " + Class1.strTableName01 + " (hub_tp, cod_prov_hub_tp,  status_doc, fecha_status, numero_doc, "; 
									query += " fecha_doc, codigo_comprador,nombre_comprador,numero_oc,  fecha_vencimiento, descuento_general,descuento_general_bs, condicion_pago,dias_condicion_pago,  "; 
									query += " observacion, total_bultos, total_unidades, monto_total_lineas, monto_total_impuesto, monto_total_factura, "; 
									query += " nombre_proveedor, nro_control, nro_notadespacho, iva, monto_total_dcto_indevolutivos, "; 
									query += " monto_total_dcto_promocion, monto_total_dcto_volumen, monto_total_descuento, monto_gravable, dcto_pronto_pago, dcto_fob,dcto_global,dcto_global_bs) "; 
									query += " VALUES "; 
									query += " ('" + Class1.strHub + "', '" + strCodigoProveedorHub + "', '120', CONVERT(DATETIME, GETDATE()),'" + strNumeroDoc + "',"; 
											
									if(strFecha_emision.Trim() == "NULL")
									{
										query += " CONVERT(DATETIME, " + strFecha_emision + "), ";
									}
									else
									{
										query += " CONVERT(DATETIME, '" + strFecha_emision + "'), ";
									}
											
									query += " '" + arrCamposLinea.GetValue(3).ToString().Replace ("'", "").Replace("|","").Trim() + "',";
									query += " '" + arrCamposLinea.GetValue(4).ToString().Replace ("'", "").Replace("|","").Trim() + "',";
									query += " '" + arrCamposLinea.GetValue(5).ToString().Replace ("'", "").Replace("|","").Trim() + "',";

									if(strFecha_vencimiento.Trim() == "NULL")
									{
										query += " CONVERT(DATETIME, " + strFecha_vencimiento + "), ";
									}
									else
									{
										query += " CONVERT(DATETIME, '" + strFecha_vencimiento + "'), ";
									}

									query += " " + dobDescuento_general.ToString().Replace(",", ".") + ", " + dobDescuento_general_BsF.ToString().Replace(",", ".") + ", " ;
									query += " '" + arrCamposLinea.GetValue(9).ToString().Replace ("'", "").Replace("|","").Trim() + "',"; 
									query += " " + dobDias_CondicionPago.ToString().Trim() + ", ";
									query += " '" + arrCamposLinea.GetValue(11).ToString().Replace ("'", "").Replace("|","").Trim() + "',";
									query +="  " + dobTotal_Bulto.ToString().Trim().Replace(",", ".") + ", " + dobTotal_Unidad + ", " +  dobSubTotal_Factura.ToString().Replace(",", ".") + ", " + dobImpuesto.ToString().Replace(",", ".") + ", " + dobTotal_factura.ToString().Replace(",", ".") + ", ";
									query += " '" + strNombreProveedor + "', ";
									query += " '" + arrCamposLinea.GetValue(20).ToString().Replace ("'", "").Replace("|","").Trim() + "', ";
									query += " '" + arrCamposLinea.GetValue(21).ToString().Replace ("'", "").Replace("|","").Trim() + "', ";
									query += " " + dobIva.ToString().Trim().Replace(",", ".") + ", " + dobMontoTotal_DescuentoInvolutivos.ToString().Replace(",", ".") + ", ";
									query += " " + dobMontoTotal_DescuentoPromocion.ToString().Trim().Replace(",", ".") + ", " + dobMontoTotal_DescuentoVolumen.ToString().Replace(",", ".") + ", ";
									query += " " + dobTotal_dcto_adicional.ToString().Trim().Replace(",", ".") + ", " + dobMontoTotal_MontoGravable.ToString().Replace(",", ".") + ", " + dobMontoTotal_ProntoPago.ToString().Replace(",", ".") + ", " ;
									query += " " + dobDescuento_FOB.ToString().Trim().Replace(",", ".") + ", " + dobPorcentaje_Descuento_Global.ToString().Trim().Replace(",", ".") + ", " ;
									query += " " + dobMonto_Descuento_Global.ToString().Replace(",", ".") + " )";


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
									#endregion

									try
									{
										// INSERTA EL REGISTRO DE ENCABEZADO
										mySqlCommand = objBDatos.Comando(query, mySqlConnection);
										mySqlCommand.ExecuteNonQuery();		

										objTextFile.EscribirLog("INSERTANDO ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + arrCamposLinea.GetValue(18).ToString().TrimStart().TrimEnd());
										strCargaEnc=true;

										TempTipoRegistro = TipoRegistro;
										TipoRegistro = arrCamposLinea.GetValue(0).ToString();

										if(TempTipoRegistro == TipoRegistro)
										{
											//error H==H
											DeleteFacturaGenerica(strTempNroDoc, strTempCodigoProveedorHub, mySqlConnection);
											TipoRegistro = "";
											TempTipoRegistro = "";
														
										}
													

									}
									catch (Exception ex) 
									{
										Class1.strCodError = ex.Message;
										objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
										objTextFile.EscribirLog("  ---> ERROR -- QUERY: " + query);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-ENCABEZADO Factura N° " + strNumeroDoc + ": Error cuando intentaba insertar el registro. " + Class1.strCodError;
										strErrorEncabezados = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";

									}
									

								} 
								#endregion

							}// fin de registro existe
					


							strTempNroDoc = strNumeroDoc;
							strTempCodigoProveedorHub = strCodigoProveedorHub;
							strTempNombreProveedor= strNombreProveedor;


						}
						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						//DETERMINA EL TIPO DE REGISTRO DETALLE
						// -------------------------------------------------------------------------------------
						// -------------------------------------------------------------------------------------
						else if(arrCamposLinea.GetValue(0).ToString() == "02")
						{

							if(strTemporal=="NO")
							{

								intContDetalle += 1;

								// ASIGNA VALORES A LAS VARIABLES DEL DETALLE NUMERO FACTURA Y CODIGO PROVEEDOR
									
								try
								{
									strNumeroDoc = arrCamposLinea.GetValue(1).ToString().Replace("'", "").Replace("|","").Trim();

									if (strNumeroDoc == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(2) NUMERO DE FACTURA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(2) Número Factura. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
								}
								catch
								{
								
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(2) NUMERO DE FACTURA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(2) Número Factura. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";

								}

								try
								{
									
									strCodigoProveedorHub = arrCamposLinea.GetValue(19).ToString().Replace("'", "").Replace("|","").Trim();
									
									if (strCodigoProveedorHub == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(20) CODIGO PROVEEDOR: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(20) Código Proveedor. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
								
								}
								catch
								{
								
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(20) CODIGO PROVEEDOR: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Hay campos que son obligatorios y vienen vacio. Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
								

								if(intNumeroLinea == 1)
								{
									TipoRegistro = arrCamposLinea.GetValue(0).ToString();
									if(TipoRegistro != "01")
									{
										
										if (strErrorEncabezados=="SI" )
										{
											
											objTextFile.EscribirLog("  ---> ERROR -- NO ES POSIBLE INSERTAR DETALLES PORQUE FALTAN CAMPOS EN EL ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc );
										
										}
										else
										{
											if((ErrorTipoRegistro == "SI") || (ErrorTipoRegistro == ""))
											{
												objTextFile.EscribirLog("  ---> ERROR -- DOCUMENTO(S) NUMERO(S): " + strNumeroDoc + " NO CONTIENE ENCABEZADO" + " LINEA:" + intNumeroLinea);
												strEmailCuerpo+= "<br>";
												strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Error el documento no contiene encabezado. " + Class1.strCodError + ". Detalle " + intContDetalle;
												strErrorEncabezados = "SI";
												ErrorTipoRegistro = "SI";
												strEnvioCorreo="SI";
											}
										}

									}
									else
									{
										ErrorTipoRegistro = "NO";
									}
								}
								else
								{
									if((strNumeroDoc != strTempNroDoc) || (strCodigoProveedorHub != strTempCodigoProveedorHub)) 
									{
	
											objTextFile.EscribirLog("-DETALLE Factura N° " + strNumeroDoc + " El Número Factura o el Código Provedor no coinciden a los enviados en el encabezado" + " ,LINEA:" + intNumeroLinea);
											strEmailCuerpo+= "<br>";
											strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": El Número Factura o el Código Provedor no coinciden a los enviados en el encabezado. Detalle " + intContDetalle;
											ErrorTipoRegistro = "SI";
											strEnvioCorreo="SI";
											//error D!=D
											DeleteFacturaGenerica(strTempNroDoc, strTempCodigoProveedorHub, mySqlConnection);
											strErrorDetalles = "SI";
											strErrorRegD=true;
									}
									else
									{
										//ErrorTipoRegistro = "NO";
									}
								}


							#region SE VALIDAN TODOS LOS CAMPOS OBLIGATORIOS DEL DETALLE
								
								try
								{
									if (arrCamposLinea.GetValue(2).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("DETALLE - SE REQUIERE VALOR PARA EL CAMPO(3) CODIGO DE BARRA, LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(3) Código Barra Unidad. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
								
								}
								catch
								{
								
									objTextFile.EscribirLog("DETALLE - SE REQUIERE VALOR PARA EL CAMPO(3) CODIGO DE BARRA, LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(3) Código Barra Unidad. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";

								}

								try
								{
									if (arrCamposLinea.GetValue(3).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("DETALLE - SE REQUIERE VALOR PARA EL CAMPO(4) CODIGO INTERNO, LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(4) Código Interno Producto Proveedor. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
								}
								catch
								{
								
									objTextFile.EscribirLog("DETALLE - SE REQUIERE VALOR PARA EL CAMPO(4) CODIGO INTERNO, LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(4) Código Interno Producto Proveedor. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";

								}

								try
								{
									if (arrCamposLinea.GetValue(5).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(6) DESCRIPCION PRODUCTO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(6) Descripción Producto. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}

								}
								catch
								{
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(6) DESCRIPCION PRODUCTO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(6) Descripción Producto. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(6).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(7) UNIDADES POR BULTO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(7) Unidades x Bulto. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobUnidades_bulto = Convert.ToDouble(arrCamposLinea.GetValue(6).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(7) UNIDADES POR BULTO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(7) Unidades x Bulto. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(7).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(8) CANTIDAD FACTURADA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(8) Cantidad Facturada. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobCantidad_facturada = Convert.ToDouble(arrCamposLinea.GetValue(7).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(8) CANTIDAD FACTURADA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(8) Cantidad Facturada. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(8).ToString().Trim() == "")
									{
										dobUnidades_bonificada = 0;
									}
									else
									{
										dobUnidades_bonificada = Convert.ToDouble(arrCamposLinea.GetValue(8).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;								
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(9) UNIDADES BONIFICADAS: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(9) Unidades Bonificadas. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";

								}

								try
								{
									if (arrCamposLinea.GetValue(9).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(10) TIPO DE UNIDAD FACTURADA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(10) Tipo Unidad Facturada. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
								}
								catch
								{
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(10) TIPO DE UNIDAD FACTURADA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(10) Tipo Unidad Facturada. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(10).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(11) PRECIO UNIDAD NETO, DEL DETALLE: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(11) Precio Unitario Neto. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobPrecio_unidad_neto = Convert.ToDouble(arrCamposLinea.GetValue(10).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(11) PRECIO UNIDAD NETO, DEL DETALLE: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(11) Precio Unitario Neto. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(11).ToString().Trim() == "")
									{
										dobPvp_Unidad = 0.0; 
									}
									else
									{
										dobPvp_Unidad = Convert.ToDouble(arrCamposLinea.GetValue(11).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(12) PVP UNIDAD: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(12) PVP Unidad. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}


								try
								{
									if(arrCamposLinea.GetValue(12).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(13) TOTAL LINEA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(13) Total Linea. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobTotal_linea = Convert.ToDouble(arrCamposLinea.GetValue(12).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(13) TOTAL LINEA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(13) Total Linea. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}


								try
								{
									if(arrCamposLinea.GetValue(13).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMP(14) TASA IVA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(14) Tasa IVA. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobTasa_Iva = Convert.ToDouble(arrCamposLinea.GetValue(13).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(14) TASA IVA: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(14) Tasa IVA. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(14).ToString().Trim() == "")
									{
										objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(15) MONTO IMPUESTO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
										strEmailCuerpo+= "<br>";
										strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Campo(15) Monto Impuesto. Viene vacio y es un campo requerido. Detalle " + intContDetalle;
										strErrorDetalles = "SI";
										ErrorTipoRegistro = "SI";
										strEnvioCorreo="SI";
									}
									else
									{
										dobImpuesto = Convert.ToDouble(arrCamposLinea.GetValue(14).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(15) MONTO IMPUESTO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(15) Monto Impuesto. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								
								try
								{
									if(arrCamposLinea.GetValue(15).ToString().Trim() == "")
									{
										dobPorcentaje_Descuento = 0.0;
									}
									else
									{
										dobPorcentaje_Descuento = Convert.ToDouble(arrCamposLinea.GetValue(15).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(16) % DESCUENTO ADICIONAL: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(16) % Descuento Adicional. " + Class1.strCodError+ " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
								
								try
								{
									if(arrCamposLinea.GetValue(16).ToString().Trim() == "")
									{
										dobMonto_Descuento = 0.0;
									}
									else
									{
										dobMonto_Descuento = Convert.ToDouble(arrCamposLinea.GetValue(16).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(17) DESCUENTO ADICIONAL: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(17) Monto Descuento Adicional. " + Class1.strCodError+ " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{			
									if(arrCamposLinea.GetValue(18).ToString().Trim() == "")
									{
										strFecha_vencimiento_det = "NULL";
									}
									else
									{
										strFecha_vencimiento_det = arrCamposLinea.GetValue(18).ToString().Trim().Substring(0,4) + arrCamposLinea.GetValue(18).ToString().Trim().Substring(4,2) + arrCamposLinea.GetValue(18).ToString().Trim().Substring(6,2);
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(19) FECHA VENCIMIENTO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(19) Fecha Vencimiento. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
								
								
								try
								{
									if(arrCamposLinea.GetValue(20).ToString().Trim() == "")
									{
										dobPorcentaje_Descuento_indevolutivo = 0.0;
									}
									else
									{
										dobPorcentaje_Descuento_indevolutivo = Convert.ToDouble(arrCamposLinea.GetValue(20).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(21) % DESCUENTO INDEVOLUTIVO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(21) % Descuento Indevolutivos. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
								
										
								try
								{
									if(arrCamposLinea.GetValue(21).ToString().Trim() == "")
									{
										dobMonto_Descuento_Indevolutivo = 0.0;
									}
									else
									{
										dobMonto_Descuento_Indevolutivo = Convert.ToDouble(arrCamposLinea.GetValue(21).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(22) MONTO DESCUENTO INDEVOLUTIVO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(22) Monto Total Descuento Indevolutivos. " + Class1.strCodError+ " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(22).ToString().TrimStart().TrimEnd() == "")
									{
										dobPorcentaje_Descuento_Promocion = 0.0;
									}
									else
									{
										dobPorcentaje_Descuento_Promocion = Convert.ToDouble(arrCamposLinea.GetValue(22).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(23) % DESCUENTO PROMOCION: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(23) % Descuento por Promociones. " + Class1.strCodError+ " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
									
									
								try
								{
									if(arrCamposLinea.GetValue(23).ToString().TrimStart().TrimEnd() == "")
									{
										dobMonto_Descuento_Promocion = 0.0;
									}
									else
									{
										dobMonto_Descuento_Promocion = Convert.ToDouble(arrCamposLinea.GetValue(23).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(24) MONTO DESCUENTO PROMOCION: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(24) Monto Total Descuento por Promociones. " + Class1.strCodError+ " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}

								try
								{
									if(arrCamposLinea.GetValue(24).ToString().TrimStart().TrimEnd() == "")
									{
										dobPorcentaje_Descuento_Volumen = 0.0;
									}
									else
									{
										dobPorcentaje_Descuento_Volumen = Convert.ToDouble(arrCamposLinea.GetValue(24).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(25) % DESCUENTO VOLUMEN: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(25) % Descuento por Volumen. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
										
								
								try
								{
									if(arrCamposLinea.GetValue(25).ToString().TrimStart().TrimEnd() == "")
									{
										dobMonto_Descuento_Volumen = 0.0;
									}
									else
									{
										dobMonto_Descuento_Volumen = Convert.ToDouble(arrCamposLinea.GetValue(25).ToString().Trim().Replace(",","."));
									}
								}
								catch (Exception ex) 
								{
									Class1.strCodError = ex.Message;
									objTextFile.EscribirLog("  ---> ERROR DETALLE EN EL CAMPO(26) MONTO DESCUENTO VOLUMEN: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
									strEmailCuerpo+= "<br>";
									strEmailCuerpo+= "-DETALLE Factura N°" + strNumeroDoc+ ": Campo(26) Monto Total Descuento por Volumen. " + Class1.strCodError + " . Detalle " + intContDetalle;
									strErrorDetalles = "SI";
									ErrorTipoRegistro = "SI";
									strEnvioCorreo="SI";
								}
				
														
							#endregion
							
							
							strTempNumero_doc = "";
							strTempCod_prov_hub_tp = "";
							TipoRegistro = "";
							TempTipoRegistro = "";
								


							if(strErrorEncabezados == "NO")
								{

									if (strErrorDetalles == "NO")
									{

									#region //VERIFICA SI YA EXISTE LA FACTURA EN BASE DE DATOS 
 
										query = " SELECT * FROM " + Class1.strTableName02 + " WHERE numero_doc = '" + strNumeroDoc + "' ";
										query += " and cod_prov_hub_tp = '" + strCodigoProveedorHub + "' ";
										query += " and codigo_barra = '" + arrCamposLinea.GetValue(2).ToString().TrimStart().TrimEnd() + "' ";
										query += " and nro_lote = '" + arrCamposLinea.GetValue(17).ToString().TrimStart().TrimEnd()+ "'";
										if (dobPrecio_unidad_neto == 0.0 )
										{
											query += " and bonificado = 'SI'";
										}
										else
										{
											query += " and bonificado = 'NO'";
										}


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
										#endregion
								
										try
										{
											// VERIFICA SI EXISTE EL DETALLE 
											mySqlCommand = objBDatos.Comando(query, mySqlConnection);
											myReader = mySqlCommand.ExecuteReader();
										}
										catch (Exception ex) 
										{
											Class1.strCodError = ex.Message;
											strErrorDetalles = "SI";
											ErrorTipoRegistro = "SI";
											objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError);
											objTextFile.EscribirLog("  ---> ERROR -- QUERY: " + query);
											strEmailCuerpo+= "<br>";
											strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Error la factura no se pudo cargar. " + Class1.strCodError + " . Detalle " + intContDetalle;
											strEnvioCorreo="SI";
										}

										while (myReader.Read())
										{
											strTempNumero_doc = myReader["numero_doc"].ToString();
											strTempCod_prov_hub_tp = myReader["cod_prov_hub_tp"].ToString();
											strTempCodProd = myReader["codigo_barra"].ToString();
											strTempNumLote = myReader["nro_lote"].ToString();
											strTempBonificado=myReader["bonificado"].ToString();
									
									
											if (dobPrecio_unidad_neto == 0.0 )
											{
												if(strTempBonificado=="SI")
												{
													Existe=true;
												}
												else
												{
													Existe=false;
												}
											}
											else
											{
												if(strTempBonificado=="SI")
												{
													Existe=false;
												}
												else
												{
													Existe=true;
												}
											}
										}

										myReader.Close();
										#endregion


										
										/*if((strTempNumero_doc == arrCamposLinea.GetValue(1).ToString().Trim()) && (strTempCod_prov_hub_tp == arrCamposLinea.GetValue(19).ToString().Trim())&& (strTempCodProd == arrCamposLinea.GetValue(02).ToString().Trim()) && Existe )
										{

													
											strNumeroDoc = arrCamposLinea.GetValue(1).ToString().TrimStart().TrimEnd();
											strTempNroDoc = strNumeroDoc;

											//BORRA LA FACTURA POR EXISTIR DUPLICIDAD DE LAS CLAVES
											objTextFile.EscribirLog("  ---> ERROR -- ERROR SE ENCONTRARON REGISTROS DUPLICADOS, LINEA: "+intNumeroLinea);
											strEmailCuerpo+= "<br>";
											strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Error la factura no se cargo porque se encontraron registros duplicados. Detalle " + intContDetalle;
											strErrorDetalles = "SI";
											ErrorTipoRegistro = "SI";
											strEnvioCorreo="SI";


										//}
										//else
										//{*/

											#region //INSERTA EL REGISTRO DE DETALLE

											query = " INSERT INTO " + Class1.strTableName02 + " (hub_tp, status_item, fecha_status, cod_prov_hub_tp, numero_doc, codigo_barra, ";
											query += "  codigo_proveedor,codigo_prod_comprador, descripcion_unidad, unidades_bulto, cantidad_facturada, ";
											query += " cantidad_bonificada, tipo_unidad_facturada,  precio_unidad, pvp_unidad,   ";
											query += " monto_linea,tasa_iva, monto_impuesto, dcto_adicional,monto_descuento, nro_lote,fecha_vencimiento, dcto_indevolutivo, total_dcto_indevolutivo, ";
											query += " dcto_promocion, total_dcto_promocion, dcto_volumen, total_dcto_volumen, linea_interna, bonificado ) ";
											query += " VALUES ";
											query += " ('" + Class1.strHub + "', '100', CONVERT(DATETIME, GETDATE()),  '" + strCodigoProveedorHub + "', ";
											query += " '" + strNumeroDoc + "', ";
											query += " '" + arrCamposLinea.GetValue(2).ToString().Replace("'", "").Replace("|","").Trim() + "', ";
											query += " '" + arrCamposLinea.GetValue(3).ToString().Replace("'", "").Replace("|","").Trim() + "', ";
											query += " '" + arrCamposLinea.GetValue(4).ToString().Replace("'", "").Replace("|","").Trim() + "', ";
											query += " '" + arrCamposLinea.GetValue(5).ToString().Replace("'", "").Replace("|","").Trim() + "', ";
											query += " " + dobUnidades_bulto + ", " + dobCantidad_facturada + ", " + dobUnidades_bonificada + ", "; 
											query += " '" + arrCamposLinea.GetValue(9).ToString().Replace("'", "").Replace("|","").Trim() + "', ";
											query += " " + dobPrecio_unidad_neto.ToString().Replace(",", ".") + ", " + dobPrecio_Neto.ToString().Replace(",", ".") + ", " + dobTotal_linea.ToString().Replace(",", ".") + ", " + dobTasa_Iva.ToString().Replace(",", ".") + ", ";
											query += " " + dobImpuesto.ToString().Replace(",", ".") + ", " + dobPorcentaje_Descuento.ToString().Replace(",", ".") + ", " + dobMonto_Descuento.ToString().Replace(",", ".") + ", '" + arrCamposLinea.GetValue(17).ToString().Replace("'", "").Replace("|","").Trim() + "', ";
											if(strFecha_vencimiento_det.Trim() == "NULL")
											{
												query += " CONVERT(DATETIME, " + strFecha_vencimiento_det.TrimStart().TrimEnd() + "), ";
											}
											else
											{
												query += " CONVERT(DATETIME, '" + strFecha_vencimiento_det.TrimStart().TrimEnd() + "'), ";
											}
											query += " " + dobPorcentaje_Descuento_indevolutivo.ToString().Replace(",", ".") + ", " + dobMonto_Descuento_Indevolutivo.ToString().Replace(",", ".") + ", ";
											query += " " + dobPorcentaje_Descuento_Promocion.ToString().Replace(",", ".") + ", " + dobMonto_Descuento_Promocion.ToString().Replace(",", ".") + ", ";
											query += " " + dobPorcentaje_Descuento_Volumen.ToString().Replace(",", ".") + ", " + dobMonto_Descuento_Volumen.ToString().Replace(",", ".") + ", " ;
											if (dobPrecio_unidad_neto==0.0)
											{
												query += " "+ intContDetalle + ",'SI') ";
											}
											else
											{
												query += " "+ intContDetalle + ",'NO') ";
											}

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
											#endregion
															
											// INSERTA EL REGISTRO DE DETALLE
											try
											{
												mySqlCommand = objBDatos.Comando(query, mySqlConnection);
												mySqlCommand.ExecuteNonQuery();	
												strTempNroDoc = strNumeroDoc;
												strCargaDet=true;
											}
											catch (Exception ex) 
											{
												Class1.strCodError = ex.Message;
												objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError + " DOCUMENTO NUMERO: " + strNumeroDoc + " LINEA: " + intNumeroLinea);
												objTextFile.EscribirLog("  ---> ERROR -- QUERY: " + query);
												strEmailCuerpo+= "<br>";
												strEmailCuerpo+= "-DETALLE Factura N° " + strNumeroDoc + ": Error cuando intentaba insertar el registro. " + Class1.strCodError + " . Detalle " + intContDetalle;
												strErrorDetalles = "SI";
												ErrorTipoRegistro = "SI";
												strEnvioCorreo="SI";
												DeleteFacturaGenerica (strNumeroDoc, strCodigoProveedorHub, mySqlConnection);
												strErrorRegD=true;
												
											}

											#endregion

										//}
									}
									else
									{
									
										if (strErrorDetalles =="SI")
										{
											if(strErrorRegD!=true)
											{
												DeleteFacturaGenerica (strNumeroDoc, strCodigoProveedorHub, mySqlConnection);
												strErrorRegD=true;
											}
										}

									}// fin (strErrorDetalles == "NO")

								
								}
								else
								{

									if (strErrorEncabezados == "SI" && strErrorDetalles =="SI" )
									{
										if(strErrorRegD!=true)
										{
											DeleteFacturaGenerica (strNumeroDoc, strCodigoProveedorHub, mySqlConnection);
											strErrorRegD=true;
										}
									}

								}// fin (strErrorEncabezados == "NO")

							}//fin (strTemporal=="NO")
						
						}//fin registro 02
							
						else // ERROR DE TIPO DE REGISTRO INVALIDO NO ES NI '01' NI '02'
						{						
											
							//objTextFile.EscribirLog("  ---> ERROR -- ERROR EN EL CONTENIDO DEL ARCHIVO DE ENTRADA.  TIPO DE REGISTRO INVALIDO EN LA LINEA: " + intNumeroLinea);
							// SE SALE DEL WHILE
							//break;
						}



					}// fin while



					// -------------------------------------------------------------------------------------
					// EVALUO SI EXISTEN ERRORES PARA MANDAR A BORRAR
					// -------------------------------------------------------------------------------------
					/*if((strErrorDetalles == "SI") && (strErrorEncabezados != "SI"))
					{
						DeleteFacturaGenerica (strTempNroDoc, strTempCodigoProveedorHub, mySqlConnection);
					}*/

					// -------------------------------------------------------------------------------------
					// ULTIMO LOG DE ENCABEZADO
					// -------------------------------------------------------------------------------------

					if(	ErrorTipoRegistro != "SI")
					{
						if(intNumeroLinea-1 == 0)
						{
							objTextFile.EscribirLog("");
							objTextFile.EscribirLog("  ---> ERROR -- DOCUMENTO NUMERO: " + strNumeroDoc + " NO CONTIENE DETALLE(S)");
						}
						else
						{
							if(strCargaEnc==true && strCargaDet==true)
							{
								// INSERT EN EL LOG DE LA CANTIDAD DE DETALLES INSERTADOS POR EL ENCABEZADO
								objTextFile.EscribirLog("SE INSERTARON Y/O MODIFICARON " + (intContDetalle) + " REGISTROS DE DETALLE PARA EL DOCUMENTO NUMERO: " + strNumeroDoc);
						
							}
                            else
                            {
                                DeleteFacturaGenerica(strTempNroDoc, strTempCodigoProveedorHub, mySqlConnection);
                                strEmailCuerpo += "-La Factura N° " + strNumeroDoc + ": No Contiene Detalle(s).";
                                objTextFile.EscribirLog("  ---> ERROR -- DOCUMENTO NUMERO: " + strNumeroDoc + " NO CONTIENE DETALLE(S)");
                                strEnvioCorreo = "SI";

                            }
						}

					}

					#region ULTIMO CORREO DE ERRORES DE CARGA FACTURA ELECTRONICA
							

					if((strEnvioCorreo=="SI"))
					{

						#region BUSCA LOS CORREOS

						query = " SELECT * FROM errores_carga_factura where hub_tp = '" + Class1.strHub + "' ";
						query += " and cod_prov_hub_tp = '" + strCodigoProveedorHub + "' ";


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
							objTextFile.EscribirLog("  ---> ERROR: " + Class1.strCodError);
							objTextFile.EscribirLog(" QUERY: " + query);
							//strErrorRegistro = "SI";

						}

						while (myReader.Read())
						{
							strEmail = myReader["correo"].ToString();
							break;
						}

						myReader.Close();	

						#endregion 

						if (strEmail!="" && strEmailCuerpo!="")
						{
							try
							{
								strBody= "<center><strong>FACTURAS NO CARGADAS</strong></center>";
								strBody+="   <br><br>";					
								strBody+="Facturas Enviadas a: " + Class1.strCliente + "<br>";  
								strBody+="Nombre del Proveedor: " + strNombreProveedor + "<br>";
								strBody+="Código del Proveedor: " + strCodigoProveedorHub + "<br>";  
								strBody+="   <br>";
								strBody+= strEmailCuerpo;

                                bool OK = objEmail.EnviarEmail("mailing@tradeplace.net", strEmail, Class1.strEmailCC, "Errores Carga Factura", strBody, Class1.strServer);
								if (OK)
								{
									objTextFile.EscribirLog("--> EMAIL ERRORES DE CARGA FACTURA ELECTRONICA EXITOSO... ");
																		
								}
							
							}
							catch (Exception ex1)
							{
								Class1.strCodError = ex1.Message;
								objTextFile.EscribirLog("  ---> ERROR -- ERROR ENVIANDO EL EMAIL ERRORES DE CARGA FACTURA ELECTRONICA.  CODIGO: " + Class1.strCodError);								
							}
						}
					}

					#endregion


					

					if(intNumeroLinea == 1)
					{
						//TipoRegistro = arrCamposLinea.GetValue(0).ToString();
						if(TipoRegistro == "01")
						{
							DeleteFacturaGenerica (strTempNroDoc, strTempCodigoProveedorHub, mySqlConnection);
							TipoRegistro = "";
							TempTipoRegistro = "";
						}
					}

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

		public void DeleteFacturaGenerica (string strNumeroDoc, string strCodigoProveedorHub, SqlConnection mySqlConnection)
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
			query = " DELETE " + Class1.strTableName01 + " ";
			query += " WHERE numero_doc ='" + strNumeroDoc + "' ";
			query += " AND cod_prov_hub_tp = '" + strCodigoProveedorHub + "' ";

			// INSERT EN EL LOG LA ELIMINACION DEL ENCABEZADO
			//objTextFile.EscribirLog("ELIMINANDO REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);

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
						
				//objTextFile.EscribirLog("  ---> ERROR -- ERROR ELIMINANDO EL REGISTRO DE ENCABEZADO DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
			}

			// -------------------------------------------------------------------------------------------
			// GENERA EL QUERY PARA HACER EL DELETE DE LOS REGISTROS DE BASE DE DATOS DE DETALLES
			// -------------------------------------------------------------------------------------------
			query = " DELETE " + Class1.strTableName02 + " ";
			query += " WHERE numero_doc = '" + strNumeroDoc + "' ";
			query += " AND cod_prov_hub_tp = '" + strCodigoProveedorHub + "' ";

			// INSERT EN EL LOG LA ELIMINACION DE LOS DETALLES
			//objTextFile.EscribirLog("ELIMINANDO LOS REGISTROS DE DETALLES DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);
			
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
				// INSERT EN EL LOG LA ELIMINACION DE LOS DETALLES
				objTextFile.EscribirLog("--->ELIMINANDO ENCABEZADO Y REGISTROS DE DETALLE DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub);
			}
			catch (Exception ex) 
			{
				Class1.strCodError = ex.Message;
								
				//objTextFile.EscribirLog("  ---> ERROR -- ERROR ELIMINANDO LOS REGISTROS DE DETALLE DEL DOCUMENTO NUMERO: " + strNumeroDoc + " DEL PROVEEDOR: " + strCodigoProveedorHub + "  CODIGO: " + Class1.strCodError);
			}
		}
	}
}
