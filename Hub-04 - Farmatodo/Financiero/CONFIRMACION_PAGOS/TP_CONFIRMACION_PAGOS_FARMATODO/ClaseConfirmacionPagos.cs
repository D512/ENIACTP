
using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_CONFIRMACION_PAGOS
{
	/// <summary>
	/// Clase Pagos
	/// </summary>
	public class ClaseConfirmacionPagos
	{
		
		#region Clases
		
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		
		#endregion
		
		#region Variables Globales

        int intNumeroLinea = 0;
        int intReintentosAplicados = 0;
        int intReintentos = 4;
        int cantDet = 0;
       

        bool error, saltarRegistro, registroDetalle;
        bool primeraLinea = false;

        string strNumeroItem = "";
        string querySql = String.Empty;

        #endregion

        #region CAMPOS ENCABEZADO

		//Tipo Registro	
		String tipoReg = String.Empty;
		const int LENGTHtipoReg = 2;
		
		//Nro.  De Comprobante	
		String numDoc = String.Empty;	
		const int LENGTHnumDoc = 35;

        //Código Proveedor	
        String codProv = String.Empty;
        const int LENGTHcodProv = 35;

        //Nombre Proveedor
        String proveedor = String.Empty;
        const int LENGTHproveedor = 150;

        //RIF del Proveedor	
        String rifProv = String.Empty;
        const int LENGTHrifProv = 15;

		//Fecha Documento
		String fechaDoc = String.Empty;	
		const int LENGTHfechaDoc = 8;

        // Monto Pago
        String montoPagos = String.Empty;
        double montoPago = 0;
		
		//Nombre Comprador
		String comprador = String.Empty;	
		const int LENGTHcomprador = 150;
		
		//RIF Comprador
		String rifComprador = String.Empty;	
		const int LENGTHrifComprador = 15;
		
		//Estatus Documento	
		String statusDoc = String.Empty;	
		const int LENGTHstatusDoc = 3;
		
		//Fecha Estatus	
		String fechaStatus = String.Empty;	
		const int LENGTHfechaStatus = 8;
       
        #endregion

        #region CAMPOS DETALLE
	
		//Nro. De Comprobante	
		String numDocDet = String.Empty;	
		const int LENGTHnumDocDet = 35;

		//Código Proveedor	
		String codProvDet = String.Empty;	
		const int LENGTHcodProvDet = 35;

        //Número de Factura	
        String numFactura = String.Empty;
        const int LENGTHnumFactura = 35;

		//Fecha de la Factura	
		String fechaFactura = String.Empty;	
		const int LENGTHfechaFactura = 8;

        //Monto Factura
        String montoFacturas = String.Empty;
        double montoFactura = 0;

        //Decripción
        String descripcion = String.Empty;
        const int LENGTHdescripcion = 150;

        //Código Localización
        String codLocalizacion = String.Empty;
        const int LENGTHcodLocalizacion = 35;

        //Nombre Tienda
        String nombreTienda = String.Empty;
        const int LENGTHnombreTienda = 150;

        //Banco Emisor
        String bancoEmisor = String.Empty;
        const int LENGTHbancoEmisor = 150;

        //Código Banco Emisor
        String codEmisor = String.Empty;
        const int LENGTHcodEmisor = 35;

        //Banco Receptor
        String bancoReceptor = String.Empty;
        const int LENGTHbancoReceptor = 150;

        //Código Banco Receptor
        String codReceptor = String.Empty;
        const int LENGTHcodReceptor = 35;

        //Cuenta Receptora
        String cuentaReceptora= String.Empty;
        const int LENGTHcuentaReceptora = 35;

        //Tipo de Pago
        String tipoPago = String.Empty;
        const int LENGTHtipoPago = 35;

        //Monto Gravable
        String gravables = String.Empty;
        double gravable= 0;

		//Monto Excento
        String exentos = String.Empty;
        double exento = 0;

        //Monto Impuesto	
        String impuestos = String.Empty;
        double impuesto = 0;

        //IVA Retenido	
        String ivaRets = String.Empty;
        double ivaRet = 0;

        //ISLR Retenido
        String islrRets = String.Empty;
        double islrRet = 0;

        //Descuento
        String descuentos = String.Empty;
        double descuento = 0;

        //Monto Pagado Factura
        String montoPagFacts = String.Empty;
        double montoPagFact = 0;

        //Importe Pagado Anteriores
        String impPagAnts = String.Empty;
        double impPagAnt = 0;

		//Estatus Item	
		String statusItem = String.Empty;	
		const int LENGTHstatusItem = 3;
		
		//Fecha Estatus	
		String fechaStatuDets = String.Empty;	
		const int LENGTHfechaStatuDets = 8;
	
        #endregion

        /// <summary>
        /// Builder
        /// </summary>
		public ClaseConfirmacionPagos()
		{
		}

		public void InsertConfirmacionPagos (string strFileName)
		{
	
			try
			{
				// CREA UNA INSTANCIA DE StreamReader PARA LEER DEL ARCHIVO.
                using (StreamReader sr = new StreamReader(strFileName, System.Text.Encoding.Default))
                {
                    string lineaLeida;
                    cantDet = 0;
                    int cantEnc = 0;
                    registroDetalle = false;

                    // LEE CADA UNA DE LAS LINEAS DEL ARCHIVO
                    while ((lineaLeida = sr.ReadLine()) != null)
                    {
                        intNumeroLinea++;
                        error = false;
                        error = Validar(lineaLeida);

						if (error)
                        {
                            saltarRegistro = true;
                            continue; //Salta iteracion si consigue error
                        }
					    
						//Valida si viene nuevo registro para no saltar mas
                        if ((tipoReg == "01") && (primeraLinea))
                        {
                            if ((saltarRegistro) && (!(error)))
                                saltarRegistro = false;
                        }
						
						if (saltarRegistro)
                        {
                            continue;
                        }

                        //Aun faltan datos por leer guardo lo leido en primera linea, y paso a leer 2da de encabezado
                        if (primeraLinea)
                        {
                            continue;
                        }

                        try
                        {
                            error = false;
                            querySql = String.Empty;

                            if (tipoReg == "01")
                            {
                                if ((cantDet != 0) && (!(registroDetalle)))
                                {
                                    objTextFile.EscribirLog("DETALLES ASOCIADOS: " + cantDet);
                                }

                                registroDetalle = false;
                                cantDet = 0;
                                cantEnc++;
								
                                //Se comprueba si retencion existe
								
                                if (ExecuteQuery(numDoc, codProv, querySql, 1, 1))
                                {
                                    //Si existe se borra y luego se reinserta, esto para evitar duplicidad detalles
                                    if (ExecuteQuery(numDoc, codProv, querySql, 3, 1))
                                    {
                                        objTextFile.EscribirLog("RETENCION ELIMINADA. DOCUMENTO: " + numDoc + " PROVEEDOR: " + codProv + " LINEA: " + intNumeroLinea);

                                        querySql = "INSERT INTO " + Class1.strTableName01 + " ("+ Class1.strCampoEnc01 + ", " + Class1.strCampoEnc02 + ", " + Class1.strCampoEnc03+ ", ";
                                        querySql = querySql + Class1.strCampoEnc04 + ", " + Class1.strCampoEnc05 + ", " + Class1.strCampoEnc06 + ", " + Class1.strCampoEnc07 + ", ";
                                        querySql = querySql + Class1.strCampoEnc08 + ", " + Class1.strCampoEnc09 + ", " + Class1.strCampoEnc10 + ", " + Class1.strCampoEnc11;
                                        querySql = querySql + ") VALUES ";
                                        querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";
                                        querySql = querySql + numDoc + "','";
                                        querySql = querySql + codProv + "','";
                                        querySql = querySql + proveedor + "','";
                                        querySql = querySql + rifProv + "',";
                                        querySql = querySql + "convert(datetime,'" + fechaDoc + "',112),";
                                        querySql = querySql + montoPagos.Replace(",", ".") + ",'";
                                        querySql = querySql + comprador + "','";
                                        querySql = querySql + rifComprador + "');";

                                        if (ExecuteQuery(numDoc, codProv, querySql, 2, 1))
                                        {
                                            objTextFile.EscribirLog("CONFIMACIÓN DE PAGO INSERTADA. DOCUMENTO: " + numDoc + " PROVEEDOR: " + proveedor + " CODIGO: " + codProv + " LINEA: " + intNumeroLinea);

                                            //PREPARA INSERT TRACKING
                                            querySql = "INSERT INTO " + Class1.strTableName03 + "(" + Class1.strCampoTra01 + ", " + Class1.strCampoTra02 + ", " + Class1.strCampoTra03 + ", ";
                                            querySql = querySql + Class1.strCampoTra04 + ", " + Class1.strCampoTra05 + ", " + Class1.strCampoTra06 + ", " + Class1.strCampoTra07 + " ) VALUES ";
                                            querySql = querySql + " ('" + Class1.strHub + "','";
                                            querySql = querySql + codProv + "','";
                                            querySql = querySql + numDoc + "','";
                                            querySql = querySql + "Proceso Carga',";
                                            querySql = querySql + "GETDATE(), '";
                                            querySql = querySql + "Carga', '";
                                            querySql = querySql + "Nueva');";

                                            //INSERTA TRACKING
                                            ExecuteQuery(numDoc, codProv, querySql, 2, 1);
                                        }
                                        else
                                        {
                                            objTextFile.EscribirLog(" --> ERROR - EJECUTANDO INSERCION EN BD. LINEA: " + intNumeroLinea);
                                            error = true;
                                            saltarRegistro = true;
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                        objTextFile.EscribirLog(" --> ERROR - EJECUTANDO ELIMINACION EN BD. LINEA: " + intNumeroLinea);
                                        error = true;
                                        saltarRegistro = true;
                                        continue;
                                    }
                                }
                                else
                                {
                                    //Sino existe se inserta

                                    querySql = "INSERT INTO " + Class1.strTableName01 + " (" + Class1.strCampoEnc01 + ", " + Class1.strCampoEnc02 + ", " + Class1.strCampoEnc03 + ", ";
                                    querySql = querySql + Class1.strCampoEnc04 + ", " + Class1.strCampoEnc05 + ", " + Class1.strCampoEnc06 + ", " + Class1.strCampoEnc07 + ", ";
                                    querySql = querySql + Class1.strCampoEnc08 + ", " + Class1.strCampoEnc09 + ", " + Class1.strCampoEnc10 + ", " + Class1.strCampoEnc11;
                                    querySql = querySql + ") VALUES ";
                                    querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";
                                    querySql = querySql + numDoc + "','";
                                    querySql = querySql + codProv + "','";
                                    querySql = querySql + proveedor + "','";
                                    querySql = querySql + rifProv + "',";
                                    querySql = querySql + "convert(datetime,'" + fechaDoc + "',112),";
                                    querySql = querySql + montoPagos.Replace(",", ".") + ",'";
                                    querySql = querySql + comprador + "','";
                                    querySql = querySql + rifComprador + "');";

                                    if (ExecuteQuery(numDoc, codProv, querySql, 2, 1))
                                    {
                                        objTextFile.EscribirLog("CONFIRMACION DE PAGO INSERTADA. DOCUMENTO: " + numDoc + " PROVEEDOR: " + proveedor + " CODIGO: " + codProv + " LINEA: " + intNumeroLinea);

                                        //PREPARA INSERT TRACKING
                                        querySql = "INSERT INTO " + Class1.strTableName03 + "(" + Class1.strCampoTra01 + ", " + Class1.strCampoTra02 + ", " + Class1.strCampoTra03 + ", ";
                                        querySql = querySql + Class1.strCampoTra04 + ", " + Class1.strCampoTra05 + ", " + Class1.strCampoTra06 + ", " + Class1.strCampoTra07 + " ) VALUES ";
                                        querySql = querySql + " ('" + Class1.strHub + "','";
                                        querySql = querySql + codProv + "','";
                                        querySql = querySql + numDoc + "','";
                                        querySql = querySql + "Proceso Carga',";
                                        querySql = querySql + "GETDATE(), '";
                                        querySql = querySql + "Carga', '";
                                        querySql = querySql + "Nueva');";

                                        //INSERTA TRACKING
                                        ExecuteQuery(numDoc, codProv, querySql, 2, 1);
                                    }
                                    else
                                    {
                                        objTextFile.EscribirLog(" --> ERROR - EJECUTANDO INSERCION EN BD. LINEA: " + intNumeroLinea);
                                        error = true;
                                        saltarRegistro = true;

                                        continue;
                                    }
                                }
                            }
                            else if (tipoReg == "02")
                            {
                                int cantDetSum = cantDet + 1;
                                String cantDets = Convert.ToString(cantDetSum);

                                querySql = "INSERT INTO " + Class1.strTableName02 + "(" + Class1.strCampoDet01 + ", " + Class1.strCampoDet02 + ", " + Class1.strCampoDet03 + ", ";
								querySql = querySql + Class1.strCampoDet04 + ", " + Class1.strCampoDet05 + ", " + Class1.strCampoDet06 + ", " + Class1.strCampoDet07 + ", ";
                                querySql = querySql + Class1.strCampoDet08 + ", " + Class1.strCampoDet09 + ", " + Class1.strCampoDet10 + ", " + Class1.strCampoDet11 + ", ";
                                querySql = querySql + Class1.strCampoDet12 + ", " + Class1.strCampoDet13 + ", " + Class1.strCampoDet14 + ", " + Class1.strCampoDet15 + ", ";
								querySql = querySql + Class1.strCampoDet16 + ", " + Class1.strCampoDet17 + ", " + Class1.strCampoDet18 + ", " + Class1.strCampoDet19 + ", ";
                                querySql = querySql + Class1.strCampoDet20 + ", " + Class1.strCampoDet21 + ", " + Class1.strCampoDet22 + ", " + Class1.strCampoDet23 + ", ";
                                querySql = querySql + Class1.strCampoDet24 + ", " + Class1.strCampoDet25;
                                querySql = querySql + ") VALUES ";
                                querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";                                        
                                querySql = querySql + numDocDet + "','";
                                querySql = querySql + codProvDet + "','";
                                querySql = querySql + numFactura + "',";
								querySql = querySql + "convert(datetime,'" + fechaFactura + "',112),";
                                querySql = querySql + montoFacturas.Replace(",", ".") + ",'";
                                querySql = querySql + descripcion + "','";
                                querySql = querySql + codLocalizacion + "','";
                                querySql = querySql + nombreTienda + "','";
                                querySql = querySql + bancoEmisor + "','";
                                querySql = querySql + codEmisor + "','";
                                querySql = querySql + bancoReceptor + "','";
                                querySql = querySql + codReceptor + "','";
                                querySql = querySql + cuentaReceptora + "','";
                                querySql = querySql + tipoPago + "',";
                                querySql = querySql + gravables.Replace(",", ".") + ",";
								querySql = querySql + exentos.Replace(",", ".") + ",";
                                querySql = querySql + impuestos.Replace(",", ".") + ",";
								querySql = querySql + ivaRets.Replace(",", ".") + ",";
                                querySql = querySql + islrRets.Replace(",", ".") + ",";
                                querySql = querySql + descuentos.Replace(",", ".") + ",";
                                querySql = querySql + montoPagFacts.Replace(",", ".") + ",";
                                querySql = querySql + impPagAnts.Replace(",", ".") + ");";

                                if (ExecuteQuery(numDocDet, codProvDet, querySql, 1, 2))
                                {
                                    cantDet++;
                                }
                                else
                                {
                                    objTextFile.EscribirLog(" --> ERROR - EJECUTANDO INSERCION EN BD. LINEA: " + intNumeroLinea);
                                    error = true;
                                    saltarRegistro = true;
                                    continue;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Class1.strCodError = ex.Message;
                            objTextFile.EscribirLog(" --> ERROR - EJECUTANDO TRANSACCIONES EN BD: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
                            error = true;
                            continue;
                        }
                    }

                    if ((cantDet != 0) && (!(registroDetalle)))
                    {
                        objTextFile.EscribirLog("DETALLES ASOCIADOS: " + cantDet);
                        registroDetalle = true;
                    }

                    if (cantEnc != 0)
                    {
                        objTextFile.EscribirLog("TOTAL REGISTROS INSERTADOS: " + cantEnc);
                    }
                }
            }
            catch (Exception ex)
            {
                Class1.strCodError = ex.Message;
                objTextFile.EscribirLog("--> ERROR - ACTUALIZANDO REGISTROS: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
                error = true;
            }
			
		}

        /// <summary>
        /// Valida campos
        /// </summary>
        /// <param name="lineaLeida">Linea leida</param>
        /// <returns></returns>
        public bool Validar(string lineaLeida)
        {

            #region Variables Validar

            bool error = false;
            Array arrCamposLinea;

            string msjError = "--> ERROR - ";

            #endregion

            //Array Campos
            arrCamposLinea = lineaLeida.Split(Class1.caracter);

			// Tipo Registro	
			tipoReg = arrCamposLinea.GetValue(0).ToString().Trim();	
			tipoReg = Caracteres(tipoReg);

            if (tipoReg == "01")
            {
                try
                {
                        //Nro.  De Comprobante	
                        numDoc = arrCamposLinea.GetValue(1).ToString().Trim();
                        numDoc = Caracteres(numDoc);

                        //Código Proveedor	
                        codProv = arrCamposLinea.GetValue(2).ToString().Trim();
                        codProv = Caracteres(codProv);

                        //Nombre Proveedor
                        proveedor = arrCamposLinea.GetValue(3).ToString().Trim();
                        proveedor = Caracteres(proveedor);

                        //RIF del Proveedor	
                        rifProv = arrCamposLinea.GetValue(4).ToString().Trim();
                        rifProv = Caracteres(rifProv);

                        //Fecha Documento
                        fechaDoc = arrCamposLinea.GetValue(5).ToString().Trim();
                        fechaDoc = Caracteres(fechaDoc);

                        //Monto Pago	
                        montoPagos = arrCamposLinea.GetValue(6).ToString().Trim();
                        montoPagos = Caracteres(montoPagos);

                        //Nombre Comprador
                        comprador = arrCamposLinea.GetValue(7).ToString().Trim();
                        comprador = Caracteres(comprador);

                        //Rif Comprador
                        rifComprador = arrCamposLinea.GetValue(8).ToString().Trim();
                        rifComprador = Caracteres(rifComprador);

                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION LECTURA CAMPOS ENCABEZADO - ";
                }

                //NUMERO DOCUMENTO

                try
                {
                    if (numDoc == "")
                    {
                        error = true;
                        msjError = msjError + "NUMERO DOCUMENTO VACIO - ";
                    }
                    else
                    {
                        if (numDoc.Length > LENGTHnumDoc)
                        {
                            numDoc = numDoc.Substring(0, LENGTHnumDoc);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO DOCUMENTO - ";
                }

                //CODIGO PROVEEDOR

                try
                {
                    if (codProv == "")
                    {
                        error = true;
                        msjError = msjError + "CODIGO PROVEEDOR VACIO - ";
                    }
                    else
                    {
                        if (codProv.Length > LENGTHcodProv)
                        {
                            codProv = codProv.Substring(0, LENGTHcodProv);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION PROVEEDOR - ";
                }

                //PROVEEDOR

                try
                {
                    if (proveedor == "")
                    {
                        error = true;
                        msjError = msjError + "PROVEEDOR VACIO - ";
                    }
                    else
                    {
                        if (proveedor.Length > LENGTHproveedor)
                        {
                            proveedor = proveedor.Substring(0, LENGTHproveedor);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION PROVEEDOR - ";
                }

                //RIF PROVEEDOR

                try
                {
                    if (rifProv == "")
                    {
                        error = true;
                        msjError = msjError + "RIF PROVEEDOR VACIO - ";
                    }
                    else
                    {
                        if (rifProv.Length > LENGTHrifProv)
                        {
                            rifProv = rifProv.Substring(0, LENGTHrifProv);
                        }
                        //Corto RIF PROVEEDOR y aplica formato J-0000000-0
                        rifProv = rifProv.Insert(1, "-");
                        rifProv = rifProv.Insert(rifProv.Length - 1, "-");
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION RIF PROVEEDOR - ";
                }			

				//FECHA DOCUMENTO

                try
                {
                    if (fechaDoc == "")
                    {
                        error = true;
                        msjError = msjError + "FECHA DOCUMENTO VACIO - ";
                    }
                    else
                    {
                        if (fechaDoc.Length > LENGTHfechaDoc)
                        {
                            fechaDoc = fechaDoc.Substring(0, LENGTHfechaDoc);
                        }

                        string dia, mes, ano;

                        //Separa fecha
                        dia = fechaDoc.Substring(6, 2);
                        mes = fechaDoc.Substring(4, 2);
                        ano = fechaDoc.Substring(0, 4);
                        fechaDoc = ano + mes + dia;

                        int idia = Int32.Parse(dia);
                        int imes = Int32.Parse(mes);
                        int iano = Int32.Parse(ano);

                        if ((idia < 1) || (idia > 31))
                        {
                            msjError = msjError + "FECHA DOCUMENTO DIA INVALIDO -- ";
                            error = true;
                        }
                        if ((imes < 1) || (imes > 12))
                        {
                            msjError = msjError + "FECHA DOCUMENTO MES INVALIDO -- ";
                            error = true;
                        }
                        if ((iano < 1) || (iano > 3000))
                        {
                            msjError = msjError + "FECHA DOCUMENTO ANO INVALIDO -- ";
                            error = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION FECHA DOCUMENTO - ";
                }
                
                //MONTO PAGO

                try
                {
                    if (montoPagos == "")
                    {
                        error = true;
                        msjError = msjError + " MONTO PAGO VACIO - ";
                    }
                    else
                    {

                        //Fix Caso .01
                        if (montoPagos.Substring(0, 1) == ".")
                        {
                            montoPagos = "0" + montoPagos;
                        } 
                        montoPago = Convert.ToDouble(montoPagos);
                        montoPago = Math.Round(montoPago, 2);
                        montoPagos = Convert.ToString(montoPago);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MONTO PAGO - ";
                }

                //NOMBRE COMPRADOR

                try
                {
                    if (comprador == "")
                    {
                        comprador = "";
                    }
                    else
                    {
                        if (comprador.Length > LENGTHcomprador)
                        {
                            comprador = comprador.Substring(0, LENGTHcomprador);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION COMPRADOR - ";
                }

                //RIF COMPRADOR

                try
                {
                    if (rifComprador == "")
                    {
                        rifComprador = "";
                    }
                    else
                    {
                        if (rifComprador.Length > LENGTHrifComprador)
                        {
                            rifComprador = rifComprador.Substring(0, LENGTHrifComprador);
                        }
                        //Corto RIF PROVEEDOR y aplica formato J-0000000-0
                        rifComprador = rifComprador.Insert(1, "-");
                        rifComprador = rifComprador.Insert(rifProv.Length - 1, "-");
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION RIF COMPRADOR - ";
                }			
            }

            if (tipoReg == "02")
            {
                try
                {

                    //Nro. De Documento
					numDocDet = arrCamposLinea.GetValue(1).ToString().Trim();	
					numDocDet = Caracteres(numDocDet);
					
					//Código Proveedor	
					codProvDet = arrCamposLinea.GetValue(2).ToString().Trim();	
					codProvDet = Caracteres(codProvDet);

                    //Número de Factura	
                    numFactura = arrCamposLinea.GetValue(3).ToString().Trim();
                    numFactura = Caracteres(numFactura);
					
					//Fecha de la Factura	
					fechaFactura = arrCamposLinea.GetValue(4).ToString().Trim();	
					fechaFactura = Caracteres(fechaFactura);
					
					//Monto Factura
                    montoFacturas = arrCamposLinea.GetValue(5).ToString().Trim();
                    montoFacturas = Caracteres(montoFacturas);

                    //Descripción
                    descripcion = arrCamposLinea.GetValue(6).ToString().Trim();
                    descripcion = Caracteres(descripcion);

					//Código Localización
                    codLocalizacion= arrCamposLinea.GetValue(7).ToString().Trim();
                    codLocalizacion = Caracteres(codLocalizacion);

                    //Nombre Tienda	
                    nombreTienda = arrCamposLinea.GetValue(8).ToString().Trim();
                    nombreTienda = Caracteres(nombreTienda);

                    //Banco Emisor
                    bancoEmisor = arrCamposLinea.GetValue(9).ToString().Trim();
                    bancoEmisor = Caracteres(bancoEmisor);

                    //Código Banco Emisor
                    codEmisor = arrCamposLinea.GetValue(10).ToString().Trim();
                    codEmisor = Caracteres(codEmisor);

                    //Banco Receptor
                    bancoReceptor = arrCamposLinea.GetValue(11).ToString().Trim();
                    bancoReceptor = Caracteres(bancoReceptor);

                    //Código Banco Emisor
                    codReceptor = arrCamposLinea.GetValue(12).ToString().Trim();
                    codReceptor = Caracteres(codReceptor);

                    //Cuenta Receptora	
                    cuentaReceptora = arrCamposLinea.GetValue(13).ToString().Trim();
                    cuentaReceptora = Caracteres(cuentaReceptora);

                    //Tipo Pago
                    tipoPago = arrCamposLinea.GetValue(14).ToString().Trim();
                    tipoPago = Caracteres(tipoPago);

                    //Monto Gravable
                    gravables = arrCamposLinea.GetValue(15).ToString().Trim();
                    gravables = Caracteres(gravables);

                    //Monto Excento
                    exentos = arrCamposLinea.GetValue(16).ToString().Trim();
                    exentos = Caracteres(exentos);

                    //Monto Impuesto
                    impuestos = arrCamposLinea.GetValue(17).ToString().Trim();
                    impuestos = Caracteres(impuestos);

                    //Iva Retenido
                    ivaRets = arrCamposLinea.GetValue(18).ToString().Trim();
                    ivaRets = Caracteres(ivaRets);

                    //Islr Retenido
                    islrRets = arrCamposLinea.GetValue(19).ToString().Trim();
                    islrRets = Caracteres(islrRets);

                    //Descuento
                    descuentos = arrCamposLinea.GetValue(20).ToString().Trim();
                    descuentos = Caracteres(descuentos);

                    //Monto Pagado Factura
                    montoPagFacts= arrCamposLinea.GetValue(21).ToString().Trim();
                    montoPagFacts = Caracteres(montoPagFacts);

                    //Importe Pagado Anteriores
                    impPagAnts = arrCamposLinea.GetValue(22).ToString().Trim();
                    impPagAnts = Caracteres(impPagAnts);
					
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION LECTURA CAMPOS DETALLE - ";
                }
                
				//NUMERO DOCUMENTO DETALLE

                try
                {
                    if (numDocDet == "")
                    {
                        error = true;
                        msjError = msjError + "NUMERO DOCUMENTO DETALLE VACIO - ";
                    }
                    else
                    {
                        if (numDocDet.Length > LENGTHnumDocDet)
                        {
                            numDocDet = numDocDet.Substring(0, LENGTHnumDocDet);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO DOCUMENTO DETALLE - ";
                } 
				
				//CODIGO PROVEEDOR

                try
                {
                    if (codProvDet == "")
                    {
                        error = true;
                        msjError = msjError + "CODIGO PROVEEDOR VACIO - ";
                    }
                    else
                    {
                        if (codProvDet.Length > LENGTHcodProvDet)
                        {
                            codProvDet = codProvDet.Substring(0, LENGTHcodProvDet);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION CODIGO PROVEEDOR - ";
                }

                //NUMERO FACTURA

                try
                {
                    if (numFactura == "")
                    {
                        error = true;
                        msjError = msjError + "NUMERO FACTURA VACIO - ";
                    }
                    else
                    {
                        if (numFactura.Length > LENGTHnumFactura)
                        {
                            numFactura = numFactura.Substring(0, LENGTHnumFactura);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO FACTURA - ";
                } 

				//FECHA FACTURA

                try
                {
                    if (fechaFactura == "")
                    {
                        error = true;
                        msjError = msjError + "FECHA FACTURA VACIO - ";
                    }
                    else
                    {
                        if (fechaFactura.Length > LENGTHfechaFactura)
                        {
                            fechaFactura = fechaFactura.Substring(0, LENGTHfechaFactura);
                        }

                        string dia, mes, ano;

                        //Separa fecha
                        dia = fechaFactura.Substring(6, 2);
                        mes = fechaFactura.Substring(4, 2);
                        ano = fechaFactura.Substring(0, 4);
                        fechaFactura = ano + mes + dia;

                        int idia = Int32.Parse(dia);
                        int imes = Int32.Parse(mes);
                        int iano = Int32.Parse(ano);

                        if ((idia < 1) || (idia > 31))
                        {
                            msjError = msjError + "FECHA FACTURA DIA INVALIDO -- ";
                            error = true;
                        }
                        if ((imes < 1) || (imes > 12))
                        {
                            msjError = msjError + "FECHA FACTURA MES INVALIDO -- ";
                            error = true;
                        }
                        if ((iano < 1) || (iano > 3000))
                        {
                            msjError = msjError + "FECHA FACTURA ANO INVALIDO -- ";
                            error = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION FECHA FACTURA - ";
                }

                //MONTO FACTURA

                try
                {
                    if (montoFacturas == "")
                    {
                        error = true;
                        msjError = msjError + "MONTO FACTURA VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (montoFacturas.Substring(0, 1) == ".")
                        {
                            montoFacturas = "0" + montoFacturas;
                        } 
                        montoFactura = Convert.ToDouble(montoFacturas);
                        montoFactura = Math.Round(montoFactura, 2);
                        montoFacturas = Convert.ToString(montoFactura);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MONTO FACTURA - ";
                }
				
				
				//DESCRIPCIÓN

                try
                {
                    if (descripcion == "")
                    {
                        descripcion = "";
                    }
                    else
                    {
                        if (descripcion.Length > LENGTHdescripcion)
                        {
                            descripcion = descripcion.Substring(0, LENGTHdescripcion);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION DESCRIPCIÓN - ";
                } 
					
				//CÓDIGO DE LOCALIZACIÓN

                try
                {
                    if (codLocalizacion == "")
                    {
                        codLocalizacion = "";
                    }
                    else
                    {
                        if (codLocalizacion.Length > LENGTHcodLocalizacion)
                        {
                            codLocalizacion = codLocalizacion.Substring(0, LENGTHcodLocalizacion);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION CÓDIGO DE LOCALIZACIÓN - ";
                } 

				//NOMBRE TIENDA

                try
                {
                    if (nombreTienda == "")
                    {
                        nombreTienda = "";
                    }
                    else
                    {
                        if (nombreTienda.Length > LENGTHnombreTienda)
                        {
                            nombreTienda = nombreTienda.Substring(0, LENGTHnombreTienda);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NOMBRE TIENDA - ";
                }

                //BANCO EMISOR

                try
                {
                    if (bancoEmisor == "")
                    {
                        error = true;
                        msjError = msjError + "BANCO EMISOR VACIO - ";
                    }
                    else
                    {
                        if (bancoEmisor.Length > LENGTHbancoEmisor)
                        {
                            bancoEmisor = bancoEmisor.Substring(0, LENGTHbancoEmisor);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION BANCO EMISOR - ";
                } 
				
				//CODIGO DE BANCO EMISOR

                try
                {
                    if (codEmisor == "")
                    {
                        codEmisor = "";
                    }
                    else
                    {
                        if (codEmisor.Length > LENGTHcodEmisor)
                        {
                            codEmisor = codEmisor.Substring(0, LENGTHcodEmisor);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION CODIGO DE BANCO EMISOR - ";
                }

                //BANCO RECEPTOR

                try
                {
                    if (bancoReceptor == "")
                    {
                        error = true;
                        msjError = msjError + "BANCO RECEPTOR VACIO - ";
                    }
                    else
                    {
                        if (bancoReceptor.Length > LENGTHbancoReceptor)
                        {
                            bancoReceptor = bancoReceptor.Substring(0, LENGTHbancoReceptor);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION BANCO RECEPTOR - ";
                }

                //CODIGO DE BANCO RECEPTOR

                try
                {
                    if (codReceptor == "")
                    {
                        codReceptor = "";
                    }
                    else
                    {
                        if (codReceptor.Length > LENGTHcodReceptor)
                        {
                            codReceptor = codReceptor.Substring(0, LENGTHcodReceptor);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION CODIGO DE BANCO RECEPTOR - ";
                } 
				
				//CUENTA RECEPTORA

                try
                {
                    if (cuentaReceptora == "")
                    {
                        cuentaReceptora = "";
                    }
                    else
                    {
                        if (cuentaReceptora.Length > LENGTHcuentaReceptora)
                        {
                            cuentaReceptora = cuentaReceptora.Substring(0, LENGTHcuentaReceptora);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION CUENTA RECEPTORA - ";
                }

                //TIPO DE PAGO

                try
                {
                    if (tipoPago == "")
                    {
                        error = true;
                        msjError = msjError + "TIPO DE PAGO VACIO - ";
                    }
                    else
                    {
                        if (tipoPago.Length > LENGTHtipoPago)
                        {
                            tipoPago = tipoPago.Substring(0, LENGTHtipoPago);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TIPO DE PAGO - ";
                } 
								
				//MONTO GRAVABLE

                try
                {
                    if (gravables == "")
                    {
                        error = true;
                        msjError = msjError + "MONTO GRAVABLE VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (gravables.Substring(0, 1) == ".")
                        {
                            gravables = "0" + gravables;
                        } 
                        gravable = Convert.ToDouble(gravables);
                        gravable = Math.Round(gravable, 2);
                        gravables = Convert.ToString(gravable);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MONTO GRAVABLE - ";
                }
				
				//MONTO EXCENTO

                try
                {
                    if (exentos == "")
                    {
                        error = true;
                        msjError = msjError + "MONTO EXCENTO VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (gravables.Substring(0, 1) == ".")
                        {
                            gravables = "0" + gravables;
                        } 
                        exento = Convert.ToDouble(exentos);
                        exento = Math.Round(exento, 2);
                        exentos = Convert.ToString(exento);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MONTO EXCENTO - ";
                }

				//MONTO IMPUESTO

                try
                {
                    if (impuestos == "")
                    {
                        error = true;
                        msjError = msjError + "MONTO IMPUESTO VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (impuestos.Substring(0, 1) == ".")
                        {
                            impuestos = "0" + impuestos;
                        } 
                        impuesto = Convert.ToDouble(impuestos);
                        impuesto = Math.Round(impuesto, 2);
                        impuestos = Convert.ToString(impuesto);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MONTO IMPUESTO - ";
                }

                //MONTO IVA RETENIDO

                try
                {
                    if (ivaRets == "")
                    {
                        error = true;
                        msjError = msjError + "MONTO IVA RETENIDO VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (ivaRets.Substring(0, 1) == ".")
                        {
                            ivaRets = "0" + ivaRets;
                        } 
                        ivaRet = Convert.ToDouble(ivaRets);
                        ivaRet = Math.Round(ivaRet, 2);
                        ivaRets = Convert.ToString(ivaRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MONTO IVA RETENIDO - ";
                }

				
				//MONTO ISLR RETENIDO

                try
                {
                    if (islrRets == "")
                    {
                        error = true;
                        msjError = msjError + "MONTO ISLR RETENIDO VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (islrRets.Substring(0, 1) == ".")
                        {
                            islrRets = "0" + islrRets;
                        } 
                        islrRet = Convert.ToDouble(islrRets);
                        islrRet = Math.Round(islrRet, 2);
                        islrRets = Convert.ToString(islrRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MONTO ISLR RETENIDO - ";
                }
				
				//DESCUENTO

                try
                {
                    if (descuentos == "")
                    {
                        error = true;
                        msjError = msjError + "DESCUENTO VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (descuentos.Substring(0, 1) == ".")
                        {
                            descuentos = "0" + descuentos;
                        } 
                        descuento = Convert.ToDouble(descuentos);
                        descuento = Math.Round(descuento, 2);
                        descuentos = Convert.ToString(descuento);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION DESCUENTO - ";
                }

                //MONTO PAGADO FACTURA

                try
                {
                    if (montoPagFacts == "")
                    {
                        error = true;
                        msjError = msjError + "MONTO PAGADO FACTURA VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (montoPagFacts.Substring(0, 1) == ".")
                        {
                            montoPagFacts = "0" + montoPagFacts;
                        } 
                        montoPagFact = Convert.ToDouble(montoPagFacts);
                        montoPagFact = Math.Round(montoPagFact, 2);
                        montoPagFacts = Convert.ToString(montoPagFact);
                    }

                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MONTO PAGADO FACTURA - ";
                }

                //IMPORTE PAGOS ANTERIORES

                try
                {
                    if (impPagAnts == "")
                    {
                        error = true;
                        msjError = msjError + "IMPORTE PAGOS ANTERIORES VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (impPagAnts.Substring(0, 1) == ".")
                        {
                            impPagAnts = "0" + impPagAnts;
                        } 
                        impPagAnt = Convert.ToDouble(impPagAnts);
                        impPagAnt = Math.Round(impPagAnt, 2);
                        impPagAnts = Convert.ToString(impPagAnt);
                    }

                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION IMPORTE PAGOS ANTERIORES - ";
                }

            }

            if ((tipoReg != "01") && (tipoReg != "02"))
            {
                error = true;
                msjError = msjError + "TIPO REGISTRO INVALIDO - ";
            }

            //Si hubo error se registra

            if (error)
            {

                if ((cantDet != 0) && (tipoReg == "01") && (!(registroDetalle)))
                {
                    objTextFile.EscribirLog("DETALLES ASOCIADOS: " + cantDet);
                    cantDet = 0;
                    registroDetalle = true;
                }

                objTextFile.EscribirLog(msjError + " DOCUMENTO: " + numDoc + " LINEA: " + intNumeroLinea);
            }

            return error;
        }

        /// <summary>
        /// Ejecuta Querys en BD
        /// </summary>
        /// <param name="tpBD">Tipo de BD</param>
        /// <param name="rif">Rif Prov</param>
        /// <param name="codprov">Codigo Prov</param>
        /// <param name="strQueryInsert">Query Insercion</param>
        /// <param name="codOp">Codigo Operacion</param>
        /// <returns>Resultado</returns>
        public bool ExecuteQuery(String numeroDoc, String codprov, String strQueryInsert, int codOp, int tipo)
        {
            bool result = false;
            bool res1 = false;
            bool res2 = false;
            String strQueryDelete = String.Empty;
            String strQuerySelect = String.Empty;

            // INSTANCIA LA CONEXION DE BASE DE DATOS
            //Crea conexión
            SqlConnection mySqlConnection = new SqlConnection();
            SqlCommand mySqlCommand = new SqlCommand();
            SqlDataReader myReader = null;

            if (tipo == 1)
            {
                switch (codOp)
                {
                    //Caso Consulta
                    case 1:
                        strQuerySelect = " SELECT " + Class1.strCampoEnc05 + ", " + Class1.strCampoEnc04 + " FROM " + Class1.strTableName01;
                        strQuerySelect = strQuerySelect + " WHERE ((" + Class1.strCampoEnc05 + " = '" + codprov + "') AND (" + Class1.strCampoEnc04 + " = '" + numeroDoc + "'))";

                        try
                        {
                            mySqlConnection = objBDatos.Conexion();
                            ValidarConexion(mySqlConnection);
                            mySqlCommand = objBDatos.Comando(strQuerySelect, mySqlConnection);
                            myReader = mySqlCommand.ExecuteReader();
                            //Reader tiene datos de retencion, luego de realizar consulta

                            //Limpieza de Variables Proveedor
                            String tempDoc = String.Empty;
                            String tempCod = String.Empty;

                            //Ciclo lectura resultado consulta
                            while (myReader.Read())
                            {
                                //Extrae Codigo Prov
                                tempCod = myReader[Class1.strCampoEnc05].ToString().Trim();
                                //Extrae Numero Doc
                                tempDoc = myReader[Class1.strCampoEnc04].ToString().Trim();
                                break;
                            }

                            myReader.Close();

                            if ((tempDoc == numeroDoc) && (tempCod == codprov))
                            {
                                objTextFile.EscribirLog("CONFIRMACION DE PAGO YA EXISTENTE. CONFIRMACION DE PAGO NUMERO: " + numeroDoc + " PROVEEDOR: " + codprov + " SE ACTUALIZARA.");
                                result = true;
                            }
                            else
                            {
                                result = false;
                            }
                        }
                        catch (Exception ex)
                        {
                            objTextFile.EscribirLog("--> ERROR BUSCANDO EN BD: " + ex.Message + ". LINEA: " + intNumeroLinea);
                            objTextFile.EscribirLog("QUERY: " + strQuerySelect + ". LINEA: " + intNumeroLinea); //PARA PRUEBAS
                            result = false;
                        }

                        break;

                    //Caso Insercion
                    case 2:

                        try
                        {
                            //Inserta Nueva Retencion Info
                            mySqlConnection = objBDatos.Conexion();
                            ValidarConexion(mySqlConnection);
                            mySqlCommand = objBDatos.Comando(strQueryInsert, mySqlConnection);
                            //objTextFile.EscribirLog("QUERY: " + strQueryInsert);
                            mySqlCommand.ExecuteNonQuery();
                            result = true;
                        }
                        catch (Exception ex)
                        {
                            objTextFile.EscribirLog("--> ERROR INSERTANDO EN BD: " + ex.Message);
                            objTextFile.EscribirLog("QUERY: " + strQueryInsert + ". LINEA: " + intNumeroLinea); //PARA PRUEBAS
                            result = false;
                        }

                        break;

                    //Caso Eliminacion
                    case 3:

                        res1 = false;
                        res2 = false;
                        //Prepara Query borrado retencion encabezado
                        strQueryDelete = "DELETE FROM " + Class1.strTableName01;
                        strQueryDelete = strQueryDelete + " WHERE ((" + Class1.strCampoEnc05 + " = '" + codprov + "') AND (" + Class1.strCampoEnc04 + " = '" + numeroDoc + "'))";

                        try
                        {
                            mySqlConnection = objBDatos.Conexion();
                            ValidarConexion(mySqlConnection);
                            //Borra retencion en caso que exista
                            mySqlCommand = objBDatos.Comando(strQueryDelete, mySqlConnection);
                            mySqlCommand.ExecuteNonQuery();
                            res1 = true;
                            //objTextFile.EscribirLog("ELIMINADA CONFIRMACION DE PAGO. DOC: " + numeroDoc + " CODIGO: " + codprov + " LINEA: " + intNumeroLinea);
                        }
                        catch (Exception ex)
                        {
                            objTextFile.EscribirLog("--> ERROR ELIMINANDO EN BD: " + ex.Message + ". LINEA: " + intNumeroLinea);
                            objTextFile.EscribirLog("QUERY: " + strQueryDelete + ". LINEA: " + intNumeroLinea); //PARA PRUEBAS
                            res1 = false;
                        }

                        //Prepara Query borrado retencion detalles
                        strQueryDelete = "DELETE FROM " + Class1.strTableName02;
                        strQueryDelete = strQueryDelete + " WHERE ((" + Class1.strCampoDet04 + " = '" + numeroDoc + "') AND (" + Class1.strCampoDet05 + " = '" + codprov + "'))";

                        try
                        {
                            mySqlConnection = objBDatos.Conexion();
                            ValidarConexion(mySqlConnection);
                            //Borra retencion en caso que exista
                            mySqlCommand = objBDatos.Comando(strQueryDelete, mySqlConnection);
                            mySqlCommand.ExecuteNonQuery();
                            res2 = true;
                        }
                        catch (Exception ex)
                        {
                            objTextFile.EscribirLog("--> ERROR ELIMINANDO EN BD: " + ex.Message + ". LINEA: " + intNumeroLinea);
                            objTextFile.EscribirLog("QUERY: " + strQueryDelete + ". LINEA: " + intNumeroLinea); //PARA PRUEBAS
                            res2 = false;
                        }

                        if ((res1) && (res2))
                        {
                            result = true;
                        }

                        break;
                }
            }
            else if (tipo == 2)
            {
                switch (codOp)
                {
                    //Caso Insercion Detalle
                    case 1:

                        try
                        {
                            //Inserta Nueva Retencion Detalle Info
                            mySqlConnection = objBDatos.Conexion();
                            ValidarConexion(mySqlConnection);
                            mySqlCommand = objBDatos.Comando(strQueryInsert, mySqlConnection);
                            //objTextFile.EscribirLog("QUERY: " + strQueryInsert);
                            mySqlCommand.ExecuteNonQuery();
                            result = true;
                        }
                        catch (Exception ex)
                        {
                            objTextFile.EscribirLog("--> ERROR INSERTANDO EN BD: " + ex.Message);
                            objTextFile.EscribirLog("QUERY: " + strQueryInsert + ". LINEA: " + intNumeroLinea); //PARA PRUEBAS
                            result = false;
                        }

                        break;
                }
            }

            mySqlConnection.Close();

            return result;
        }


        /// <summary>
        /// Elimina Caracteres Especiales
        /// </summary>
        /// <param name="texto">Texto a reemplazar caracteres</param>
        /// <returns>Texto sin caracteres especiales</returns>
        public string Caracteres(string texto)
        {
            try
            {
                texto = texto.Replace("'", "");
                texto = texto.Replace("\"", "");
                texto = texto.Replace("´", "");
                texto = texto.Replace("*", "");
                texto = texto.Replace("+", "");
                texto = texto.Replace("~", "");
                texto = texto.Replace("[", "");
                texto = texto.Replace("]", "");
                texto = texto.Replace("{", "");
                texto = texto.Replace("}", "");
                texto = texto.Replace("^", "");
                texto = texto.Replace("`", "");
                texto = texto.Replace("!", "");
                texto = texto.Replace("¡", "");
                texto = texto.Replace("?", "");
                texto = texto.Replace("¿", "");
                texto = texto.Replace("#", "");
                texto = texto.Replace("$", "");
                texto = texto.Replace("%", "");
                texto = texto.Replace("&", "");
                texto = texto.Replace("/", "");
                texto = texto.Replace("(", "");
                texto = texto.Replace(")", "");
                texto = texto.Replace("=", "");
                texto = texto.Replace("á", "a");
                texto = texto.Replace("é", "e");
                texto = texto.Replace("í", "i");
                texto = texto.Replace("ó", "o");
                texto = texto.Replace("ú", "u");
                texto = texto.Replace("Á", "A");
                texto = texto.Replace("É", "E");
                texto = texto.Replace("Í", "I");
                texto = texto.Replace("Ó", "O");
                texto = texto.Replace("Ú", "U");
                texto = texto.Replace("ñ", "n");
                texto = texto.Replace("Ñ", "N");
                texto = texto.Replace("Ã³", "O");
                
            }
            catch (Exception e)
            {
                objTextFile.EscribirLog(" --> ERROR -- ELIMINANDO CARACTERES ESPECIALES: " + Class1.strCodError + " VALOR: " + texto);
                objTextFile.EscribirLog(" EXCEPCION: " + e.Message.ToString());
            }
            return texto;
        }

        /// <summary>
        /// Valida si la conexion a la BD está abierta
        /// </summary>
        /// <param name="mySqlConnection">Conexion BD a validar</param>
        public void ValidarConexion(SqlConnection mySqlConnection)
        {
            int intReintentosAplicados = 0;
            int intReintentos = 4;

            string strErrorIntentos = String.Empty;


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
                        strErrorIntentos = "SI";
                    }
                    catch (Exception ex)
                    {
                        Class1.strCodError = ex.Message;
                        objTextFile.EscribirLog(" --> ERROR - ABRIENDO LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados + " CODIGO: " + Class1.strCodError);
                        intReintentosAplicados += intReintentosAplicados;
                    }
                } // FIN DEL WHILE
            } // FIN DEL IF DE LA CONEXION CERRADA
        }
    }
}
