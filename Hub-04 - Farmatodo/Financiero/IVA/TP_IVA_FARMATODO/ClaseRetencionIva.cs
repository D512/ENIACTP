
using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_RETENCIONIVA
{
	/// <summary>
	/// Clase Pagos
	/// </summary>
	public class ClaseRetencionIva
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
		const int LENGTHnumDoc = 20;

		//Fecha Emisión	
		String fechaEmision = String.Empty;	
		const int LENGTHfechaEmision = 8;

		//Fecha Entrega	
		String fechaEntrega = String.Empty;	
		const int LENGTHfechaEntrega = 8;
		
		//Nombre o Razón Social del Agente de Retención	
		String agente = String.Empty;	
		const int LENGTHagente = 240;
		
		//RIF del Agente de Retención	
		String rifAgente = String.Empty;	
		const int LENGTHrifAgente = 15;

        //Período Fiscal	
        String periodoFiscal = String.Empty;
        const int LENGTHperiodoFiscal = 6;

        //Dirección Fiscal del Agente de Retención	
        String direccionAg = String.Empty;
        const int LENGTHdireccionAg = 150;
		
		//Nombre o Razón Social del Sujeto Retenido (Proveedor)	
		String proveedor = String.Empty;	
		const int LENGTHproveedor = 240;

        //Código Proveedor	
        String codProv = String.Empty;
        const int LENGTHcodProv = 35;

		//RIF del Proveedor	
		String rifProv = String.Empty;	
		const int LENGTHrifProv = 15;

        // Total Factura
        String totalFacturas = String.Empty;
        double totalFactura = 0;
		
		//Total Compras Sin Derecho A Crédito IVA	
		String totalSinIvas = String.Empty;	
		double totalSinIva = 0;
		
		//Total Base Imponible	
		String totalBaseImps = String.Empty;	
		double totalBaseImp = 0;
		
		//Total Impuesto IVA	
		String totalIvas = String.Empty;	
		double totalIva = 0;
		
		//Total IVA Retenido	
		String totalIvaRets = String.Empty;	
		double totalIvaRet = 0;

        //Periodo Impositivo
        String periodoImpositivo = String.Empty;
        const int LENGTHperiodoImpositivo = 35;

        //Total Compras Incluye Iva	
        String totalCompIvas = String.Empty;
        double totalCompIva = 0;
		
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
		const int LENGTHnumDocDet = 20;

		//Código Proveedor	
		String codProvDet = String.Empty;	
		const int LENGTHcodProvDet = 35;

		//Número Operación	
		String numOp = String.Empty;	
		const int LENGTHnumOp = 15;

		//Fecha de la Factura	
		String fechaFactura = String.Empty;	
		const int LENGTHfechaFactura = 8;

		//Número de Factura	
		String numFactura = String.Empty;	
		const int LENGTHnumFactura = 20;

		//Número Control Factura	
		String numCtrl = String.Empty;	
		const int LENGTHnumCtrl = 20 ;

		//Número Nota de Débito	
		String numND = String.Empty;	
		const int LENGTHnumND = 20;

		//Número Control N. Debito	
		String numCtrlND = String.Empty;	
		const int LENGTHnumCtrlND = 20;

		//Número Nota de Crédito	
		String numNC = String.Empty;	
		const int LENGTHnumNC = 20;

		//Número Control N. Crédito	
		String numCtrlNC = String.Empty;	
		const int LENGTHnumCtrlNC = 20;

        //Tipo de Transaccion
        String tipoTransaccion = String.Empty;
        const int LENGTHtipoTransaccion = 15;

        // Factura Afectada	
        String facturaAfec = String.Empty;
        const int LENGTHfacturaAfec = 20;

        //Total Compras Incluye IVA
        String totalCompIvasDet = String.Empty;
        double totalCompIvaDet = 0;

        //Total compras sin IVA
        String totalSinIvasDet = String.Empty;
        double totalSinIvaDet = 0;

		//Base Imponible	
        String baseImps = String.Empty;
        double baseImp = 0;

        //% Alícuota	
        String alicuotas = String.Empty;
        double alicuota = 0;

        //Impuesto IVA	
        String impuestoIvas = String.Empty;
        double impuestoIva = 0;

        //IVA Retenido	
        String ivaRets = String.Empty;
        double ivaRet = 0;

        //Tasa
        String tasas = String.Empty;
        double tasa = 0;

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
		public ClaseRetencionIva()
		{
		}

        /// <summary>
        /// Insercion Retencion IVA
        /// </summary>
        /// <param name="strFileName">Nombre Archivo</param>
		public void InsertRetencionIva(string strFileName)
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
                                        querySql = querySql + Class1.strCampoEnc08 + ", " + Class1.strCampoEnc09 + ", " + Class1.strCampoEnc10 + ", " + Class1.strCampoEnc11 + ", ";
                                        querySql = querySql + Class1.strCampoEnc12 + ", " + Class1.strCampoEnc13 + ", " + Class1.strCampoEnc14 + ", " + Class1.strCampoEnc15 + ", ";
                                        querySql = querySql + Class1.strCampoEnc16 + ", " + Class1.strCampoEnc17 + ", " + Class1.strCampoEnc18 + ", " + Class1.strCampoEnc19 + ", ";
                                        querySql = querySql + Class1.strCampoEnc20;
                                        querySql = querySql + ") VALUES ";
                                        querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";
                                        querySql = querySql + numDoc + "','";
                                        querySql = querySql + codProv + "',";
                                        querySql = querySql + "convert(datetime,'" + fechaEmision + "',112),";
										querySql = querySql + "convert(datetime,'" + fechaEntrega + "',112),'";
                                        querySql = querySql + agente + "','";
										querySql = querySql + rifAgente + "','";
                                        querySql = querySql + periodoFiscal + "','";
                                        querySql = querySql + direccionAg + "','";
                                        querySql = querySql + proveedor + "','";                                        
										querySql = querySql + rifProv + "',";
                                        querySql = querySql + totalFacturas.Replace(",", ".") + ",";
                                        querySql = querySql + totalSinIvas.Replace(",", ".") + ",";
                                        querySql = querySql + totalBaseImps.Replace(",", ".") + ",";
                                        querySql = querySql + totalIvas.Replace(",", ".") + ",";
                                        querySql = querySql + totalIvaRets.Replace(",", ".") + ",'";
                                        querySql = querySql + periodoImpositivo + "',";
                                        querySql = querySql + totalCompIvas.Replace(",", ".") + ");";

                                        if (ExecuteQuery(numDoc, codProv, querySql, 2, 1))
                                        {
                                            objTextFile.EscribirLog("RETENCION IVA INSERTADA. DOCUMENTO: " + numDoc + " PROVEEDOR: " + proveedor + " CODIGO: " + codProv + " LINEA: " + intNumeroLinea);


                                            //PREPARA INSERT TRACKING
                                            querySql = "INSERT INTO " + Class1.strTableName03 + " (" + Class1.strCampoTra01 + ", " + Class1.strCampoTra02 + ", " + Class1.strCampoTra03 + ", ";
                                            querySql = querySql + Class1.strCampoTra04 + ", " + Class1.strCampoTra05 + ", " + Class1.strCampoTra06 + ", " + Class1.strCampoTra07 + ") VALUES ";
                                            querySql = querySql + " ('" + Class1.strHub + "','";
                                            querySql = querySql + codProv + "','";
                                            querySql = querySql + numDoc + "','";
                                            querySql = querySql + "Proceso Carga',";
                                            querySql = querySql + "GETDATE(),'";
                                            querySql = querySql + "Proceso Carga','";
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
                                    querySql = querySql + Class1.strCampoEnc08 + ", " + Class1.strCampoEnc09 + ", " + Class1.strCampoEnc10 + ", " + Class1.strCampoEnc11 + ", ";
                                    querySql = querySql + Class1.strCampoEnc12 + ", " + Class1.strCampoEnc13 + ", " + Class1.strCampoEnc14 + ", " + Class1.strCampoEnc15 + ", ";
                                    querySql = querySql + Class1.strCampoEnc16 + ", " + Class1.strCampoEnc17 + ", " + Class1.strCampoEnc18 + ", " + Class1.strCampoEnc19 + ", ";
                                    querySql = querySql + Class1.strCampoEnc20;
                                    querySql = querySql + ") VALUES ";
                                    querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";
                                    querySql = querySql + numDoc + "','";
                                    querySql = querySql + codProv + "',";
                                    querySql = querySql + "convert(datetime,'" + fechaEmision + "',112),";
                                    querySql = querySql + "convert(datetime,'" + fechaEntrega + "',112),'";
                                    querySql = querySql + agente + "','";
                                    querySql = querySql + rifAgente + "','";
                                    querySql = querySql + periodoFiscal + "','";
                                    querySql = querySql + direccionAg + "','";
                                    querySql = querySql + proveedor + "','";
                                    querySql = querySql + rifProv + "',";
                                    querySql = querySql + totalFacturas.Replace(",", ".") + ",";
                                    querySql = querySql + totalSinIvas.Replace(",", ".") + ",";
                                    querySql = querySql + totalBaseImps.Replace(",", ".") + ",";
                                    querySql = querySql + totalIvas.Replace(",", ".") + ",";
                                    querySql = querySql + totalIvaRets.Replace(",", ".") + ",'";
                                    querySql = querySql + periodoImpositivo + "',";
                                    querySql = querySql + totalCompIvas.Replace(",", ".") + ");";

                                    if (ExecuteQuery(numDoc, codProv, querySql, 2, 1))
                                    {
                                        objTextFile.EscribirLog("RETENCION IVA INSERTADA. DOCUMENTO: " + numDoc + " PROVEEDOR: " + proveedor + " CODIGO: " + codProv + " LINEA: " + intNumeroLinea);

                                        //PREPARA INSERT TRACKING
                                        querySql = "INSERT INTO " + Class1.strTableName03 + " (" + Class1.strCampoTra01 + ", " + Class1.strCampoTra02 + ", " + Class1.strCampoTra03 + ", ";
                                        querySql = querySql + Class1.strCampoTra04 + ", " + Class1.strCampoTra05 + ", " + Class1.strCampoTra06 + ", " + Class1.strCampoTra07 + ") VALUES ";
                                        querySql = querySql + " ('" + Class1.strHub + "','";
                                        querySql = querySql + codProv + "','";
                                        querySql = querySql + numDoc + "','";
                                        querySql = querySql + "Proceso Carga',";
                                        querySql = querySql + "GETDATE(),'";
                                        querySql = querySql + "Proceso Carga','";
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
                                querySql = querySql + Class1.strCampoDet20 + ", " + Class1.strCampoDet21 + ", " + Class1.strCampoDet22;
                                querySql = querySql + ") VALUES ";
                                querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";                                        
                                querySql = querySql + numDocDet + "','";
                                querySql = querySql + codProvDet + "','";
								querySql = querySql + numOp + "',";
								querySql = querySql + "convert(datetime,'" + fechaFactura + "',112),'";
                                querySql = querySql + numFactura + "','";
								querySql = querySql + numCtrl + "','";
								querySql = querySql + numND + "','";
								querySql = querySql + numCtrlND + "','";
								querySql = querySql + numNC + "','";
								querySql = querySql + numCtrlNC + "','";
								querySql = querySql + tipoTransaccion + "','";
								querySql = querySql + facturaAfec + "',";
                                querySql = querySql + totalCompIvasDet.Replace(",", ".") + ",";
                                querySql = querySql + totalSinIvasDet.Replace(",", ".") + ",";
								querySql = querySql + baseImps.Replace(",", ".") + ",";
                                querySql = querySql + alicuotas.Replace(",", ".") + ",";
								querySql = querySql + impuestoIvas.Replace(",", ".") + ",";
                                querySql = querySql + ivaRets.Replace(",", ".") + ",";
                                querySql = querySql + tasa + ");";

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

                        //Fecha Emisión	
                        fechaEmision = arrCamposLinea.GetValue(2).ToString().Trim();
                        fechaEmision = Caracteres(fechaEmision);

                        //Fecha Entrega	
                        fechaEntrega = arrCamposLinea.GetValue(3).ToString().Trim();
                        fechaEntrega = Caracteres(fechaEntrega);

                        //Nombre o Razón Social del Agente de Retención	
                        agente = arrCamposLinea.GetValue(4).ToString().Trim();
                        agente = Caracteres(agente);

                        //RIF del Agente de Retención	
                        rifAgente = arrCamposLinea.GetValue(5).ToString().Trim();
                        rifAgente = Caracteres(rifAgente);

                        //Período Fiscal	
                        periodoFiscal = arrCamposLinea.GetValue(6).ToString().Trim();
                        periodoFiscal = Caracteres(periodoFiscal);

                        //Dirección Fiscal del Agente de Retención	
                        direccionAg = arrCamposLinea.GetValue(7).ToString().Trim();
                        direccionAg = Caracteres(direccionAg);

                        //Nombre o Razón Social del Sujeto Retenido (Proveedor)	
                        proveedor = arrCamposLinea.GetValue(8).ToString().Trim();
                        proveedor = Caracteres(proveedor);

                        //Código Proveedor	
                        codProv = arrCamposLinea.GetValue(9).ToString().Trim();
                        codProv = Caracteres(codProv);

                        //RIF del Proveedor	
                        rifProv = arrCamposLinea.GetValue(10).ToString().Trim();
                        rifProv = Caracteres(rifProv);

                        //Total Monto Factura o N. Débito	
                        totalFacturas = arrCamposLinea.GetValue(11).ToString().Trim();
                        totalFacturas = Caracteres(totalFacturas);

                        //Total Compras Sin Derecho A Crédito IVA	
                        totalSinIvas = arrCamposLinea.GetValue(12).ToString().Trim();
                        totalSinIvas = Caracteres(totalSinIvas);

                        //Total Base Imponible	
                        totalBaseImps = arrCamposLinea.GetValue(13).ToString().Trim();
                        totalBaseImps = Caracteres(totalBaseImps);

                        //Total Impuesto IVA	
                        totalIvas = arrCamposLinea.GetValue(14).ToString().Trim();
                        totalIvas = Caracteres(totalIvas);

                        //Total IVA Retenido	
                        totalIvaRets = arrCamposLinea.GetValue(15).ToString().Trim();
                        totalIvaRets = Caracteres(totalIvaRets);

                        //Periodo Impositivo
                        periodoImpositivo = arrCamposLinea.GetValue(16).ToString().Trim();
                        periodoImpositivo = Caracteres(periodoImpositivo);

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

				//FECHA EMISION

                try
                {
                    if (fechaEmision == "")
                    {
                        error = true;
                        msjError = msjError + "FECHA EMISION VACIO - ";
                    }
                    else
                    {
                        if (fechaEmision.Length > LENGTHfechaEmision)
                        {
                            fechaEmision = fechaEmision.Substring(0, LENGTHfechaEmision);
                        }

                        string dia, mes, ano;

                        //Separa fecha
                        dia = fechaEmision.Substring(6, 2);
                        mes = fechaEmision.Substring(4, 2);
                        ano = fechaEmision.Substring(0, 4);
                        fechaEmision = ano + mes + dia;

                        int idia = Int32.Parse(dia);
                        int imes = Int32.Parse(mes);
                        int iano = Int32.Parse(ano);

                        if ((idia < 1) || (idia > 31))
                        {
                            msjError = msjError + "FECHA EMISION DIA INVALIDO -- ";
                            error = true;
                        }
                        if ((imes < 1) || (imes > 12))
                        {
                            msjError = msjError + "FECHA EMISION MES INVALIDO -- ";
                            error = true;
                        }
                        if ((iano < 1) || (iano > 3000))
                        {
                            msjError = msjError + "FECHA EMISION ANO INVALIDO -- ";
                            error = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION FECHA EMISION - ";
                }

				//FECHA ENTREGA

                try
                {
                    if (fechaEntrega == "")
                    {
                        error = true;
                        msjError = msjError + "FECHA ENTREGA VACIO - ";
                    }
                    else
                    {
                        if (fechaEntrega.Length > LENGTHfechaEntrega)
                        {
                            fechaEntrega = fechaEntrega.Substring(0, LENGTHfechaEntrega);
                        }

                        string dia, mes, ano;

                        //Separa fecha
                        dia = fechaEntrega.Substring(6, 2);
                        mes = fechaEntrega.Substring(4, 2);
                        ano = fechaEntrega.Substring(0, 4);
                        fechaEntrega = ano + mes + dia;

                        int idia = Int32.Parse(dia);
                        int imes = Int32.Parse(mes);
                        int iano = Int32.Parse(ano);

                        if ((idia < 1) || (idia > 31))
                        {
                            msjError = msjError + "FECHA ENTREGA DIA INVALIDO -- ";
                            error = true;
                        }
                        if ((imes < 1) || (imes > 12))
                        {
                            msjError = msjError + "FECHA ENTREGA MES INVALIDO -- ";
                            error = true;
                        }
                        if ((iano < 1) || (iano > 3000))
                        {
                            msjError = msjError + "FECHA ENTREGA ANO INVALIDO -- ";
                            error = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION FECHA EMISION - ";
                }
				
				//NOMBRE AGENTE RETENCION

                try
                {
                    if (agente == "")
                    {
                        error = true;
                        msjError = msjError + "NOMBRE AGENTE RETENCION VACIO - ";
                    }
                    else
                    {
                        if (agente.Length > LENGTHagente)
                        {
                            agente = agente.Substring(0, LENGTHagente);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NOMBRE AGENTE RETENCION - ";
                }
				
				//RIF AGENTE

                try
                {
                    if (rifAgente == "")
                    {
                        error = true;
                        msjError = msjError + "RIF AGENTE VACIO - ";
                    }
                    else
                    {
                        if (rifAgente.Length > LENGTHrifAgente)
                        {
                            rifAgente = rifAgente.Substring(0, LENGTHrifAgente);
                        }
                        //Corto RIF AGENTE y aplica formato J-0000000-0
                        rifAgente = rifAgente.Replace("-", "");
                        rifAgente = rifAgente.Insert(1, "-");
                        rifAgente = rifAgente.Insert(rifAgente.Length - 1, "-");
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION RIF AGENTE - ";
                }

                //PERIODO FISCAL

                try
                {
                    if (periodoFiscal == "")
                    {
                        error = true;
                        msjError = msjError + "PERIODO FISCAL VACIO - ";
                    }
                    else
                    {
                        if (periodoFiscal.Length > LENGTHperiodoFiscal)
                        {
                            periodoFiscal = periodoFiscal.Substring(0, LENGTHperiodoFiscal);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION PERIODO FISCAL - ";
                }			
				
				//DIRECCION AGENTE RETENCION

                try
                {
                    if (direccionAg == "")
                    {
                        error = true;
                        msjError = msjError + "DIRECCION AGENTE RETENCION VACIO - ";
                    }
                    else
                    {
                        if (direccionAg.Length > LENGTHdireccionAg)
                        {
                            direccionAg = direccionAg.Substring(0, LENGTHdireccionAg);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION DIRECCION AGENTE RETENCION - ";
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
                
                //TOTAL FACTURA 

                try
                {
                    if (totalFacturas == "")
                    {
                        error = true;
                        msjError = msjError + "TOTAL FACTURA VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalFacturas.Substring(0, 1) == ".")
                        {
                            totalFacturas = "0" + totalFacturas;
                        }
                        
                        totalFactura = Convert.ToDouble(totalFacturas);
                        totalFactura = Math.Round(totalFactura, 2);
                        totalFacturas = Convert.ToString(totalFactura);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL FACTURA - ";
                }
                    
                    
                //TOTAL COMPRAS SIN DERECHO A IVA

                try
                {
                    if (totalSinIvas == "")
                    {
                        totalSinIvas = "0.00";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalSinIvas.Substring(0, 1) == ".")
                        {
                            totalSinIvas = "0" + totalSinIvas;
                        }

                        totalSinIva = Convert.ToDouble(totalSinIvas);
                        totalSinIva = Math.Round(totalSinIva, 2);
                        totalSinIvas = Convert.ToString(totalSinIva);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL COMPRAS SIN DERECHO A IVA - ";
                }

                //TOTAL BASE IMPONIBLE

                try
                {
                    if (totalBaseImps == "")
                    {
                        error = true;
                        msjError = msjError + "TOTAL BASE IMPONIBLE VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalBaseImps.Substring(0, 1) == ".")
                        {
                            totalBaseImps = "0" + totalBaseImps;
                        }
                        
                        totalBaseImp = Convert.ToDouble(totalBaseImps);
                        totalBaseImp = Math.Round(totalBaseImp, 2);
                        totalBaseImps = Convert.ToString(totalBaseImp);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL BASE IMPONIBLE - ";
                }

                //TOTAL IMPUESTO IVA

                try
                {
                    if (totalIvas == "")
                    {
                        error = true;
                        msjError = msjError + "TOTAL IMPUESTO IVA VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalIvas.Substring(0, 1) == ".")
                        {
                            totalIvas = "0" + totalIvas;
                        }
                        
                        totalIva = Convert.ToDouble(totalIvas);
                        totalIva = Math.Round(totalIva, 2);
                        totalIvas = Convert.ToString(totalIva);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL IMPUESTO IVA  - ";
                }

                //TOTAL IVA RETENIDO

                try
                {
                    if (totalIvaRets == "")
                    {
                        error = true;
                        msjError = msjError + "TOTAL IVA RETENIDO VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalIvaRets.Substring(0, 1) == ".")
                        {
                            totalIvaRets = "0" + totalIvaRets;
                        }
                        
                        totalIvaRet = Convert.ToDouble(totalIvaRets);
                        totalIvaRet = Math.Round(totalIvaRet, 2);
                        totalIvaRets = Convert.ToString(totalIvaRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL IVA RETENIDO - ";
                }

                //PERIODO IMPOSITIVO

                try
                {
                    if (periodoImpositivo == "")
                    {
                        error = true;
                        msjError = msjError + "PERIODO IMPOSITIVO VACIO - ";
                    }
                    else
                    {
                        if (periodoImpositivo.Length > LENGTHperiodoImpositivo)
                        {
                            periodoImpositivo = periodoImpositivo.Substring(0, LENGTHperiodoImpositivo);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION PERIODO IMPOSITIVO  - ";
                }	

                //TOTAL COMPRAS INCLUYE IVA

                try
                {
                    if (totalCompIvas == "")
                    {
                        totalCompIvas = totalFacturas;
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalCompIvas.Substring(0, 1) == ".")
                        {
                            totalCompIvas = "0" + totalCompIvas;
                        }
                        
                        totalCompIva = Convert.ToDouble(totalCompIvas);
                        totalCompIva = Math.Round(totalCompIva, 2);
                        totalCompIvas = Convert.ToString(totalCompIva);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TTOTAL COMPRAS INCLUYE IVA - ";
                }                
            }

            if (tipoReg == "02")
            {
                try
                {

                    //Nro. De Comprobante	
					numDocDet = arrCamposLinea.GetValue(1).ToString().Trim();	
					numDocDet = Caracteres(numDocDet);
					
					//Código Proveedor	
					codProvDet = arrCamposLinea.GetValue(2).ToString().Trim();	
					codProvDet = Caracteres(codProvDet);

					//Número Operación	
					numOp = arrCamposLinea.GetValue(3).ToString().Trim();	
					numOp = Caracteres(numOp);
					
					//Fecha de la Factura	
					fechaFactura = arrCamposLinea.GetValue(4).ToString().Trim();	
					fechaFactura = Caracteres(fechaFactura);
					
					//Número de Factura	
					numFactura = arrCamposLinea.GetValue(5).ToString().Trim();	
					numFactura = Caracteres(numFactura);
					
					//Número Control Factura	
					numCtrl = arrCamposLinea.GetValue(6).ToString().Trim();	
					numCtrl = Caracteres(numCtrl);
					
					//Número Nota de Débito	
					numND = arrCamposLinea.GetValue(7).ToString().Trim();	
					numND = Caracteres(numND);
					
					//Número Control N. Debito	
					numCtrlND = arrCamposLinea.GetValue(8).ToString().Trim();	
					numCtrlND = Caracteres(numCtrlND);
					
					//Número Nota de Crédito	
					numNC = arrCamposLinea.GetValue(9).ToString().Trim();	
					numNC = Caracteres(numNC);
					
					//Número Control N. Crédito	
					numCtrlNC = arrCamposLinea.GetValue(10).ToString().Trim();	
					numCtrlNC = Caracteres(numCtrlNC);

                    //Tipo de Transaccion
                    tipoTransaccion = arrCamposLinea.GetValue(11).ToString().Trim();
                    tipoTransaccion = Caracteres(tipoTransaccion);

                    //Factura Afectada	
                    facturaAfec = arrCamposLinea.GetValue(12).ToString().Trim();
                    facturaAfec = Caracteres(facturaAfec);
					
					//Total Compras Incluye Iva
                    totalCompIvasDet = arrCamposLinea.GetValue(13).ToString().Trim();
                    totalCompIvasDet = Caracteres(totalCompIvasDet);

                    //Total compras sin IVA
                    totalSinIvasDet = arrCamposLinea.GetValue(14).ToString().Trim();
                    totalSinIvasDet = Caracteres(totalSinIvasDet);

					//Base Imponible	
                    baseImps = arrCamposLinea.GetValue(15).ToString().Trim();
                    baseImps = Caracteres(baseImps);

                    //% Alícuota	
                    alicuotas = arrCamposLinea.GetValue(16).ToString().Trim();
                    alicuotas = Caracteres(alicuotas);

                    //Impuesto IVA	
                    impuestoIvas = arrCamposLinea.GetValue(17).ToString().Trim();
                    impuestoIvas = Caracteres(impuestoIvas);

                    //IVA Retenido	
                    ivaRets = arrCamposLinea.GetValue(18).ToString().Trim();
                    ivaRets = Caracteres(ivaRets);

                    //Tasa	
                    tasas = arrCamposLinea.GetValue(19).ToString().Trim();
                    tasas = Caracteres(tasas);
					
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

				//NUMERO OPERACION

                try
                {
                    if (numOp == "")
                    {
                        numOp = "";
                    }
                    else
                    {
                        if (numOp.Length > LENGTHnumOp)
                        {
                            numOp = numOp.Substring(0, LENGTHnumOp);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO OPERACION - ";
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
				
				//NUMERO FACTURA

                try
                {
                    if (numFactura == "")
                    {
                        numFactura = "";
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
				
				//NUMERO CONTROL FACTURA

                try
                {
                    if (numCtrl == "")
                    {
                        numCtrl = "";
                    }
                    else
                    {
                        if (numCtrl.Length > LENGTHnumCtrl)
                        {
                            numCtrl = numCtrl.Substring(0, LENGTHnumCtrl);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO CONTROL FACTURA - ";
                } 
					
				//NUMERO NOTA DEBITO

                try
                {
                    if (numND == "")
                    {
                        numND = "";
                    }
                    else
                    {
                        if (numND.Length > LENGTHnumND)
                        {
                            numND = numND.Substring(0, LENGTHnumND);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO NOTA DEBITO - ";
                } 

				//NUMERO CONTROL NOTA DEBITO

                try
                {
                    if (numCtrlND == "")
                    {
                        numCtrlND = "";
                    }
                    else
                    {
                        if (numCtrlND.Length > LENGTHnumCtrlND)
                        {
                            numCtrlND = numCtrlND.Substring(0, LENGTHnumCtrlND);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO CONTROL NOTA DEBITO - ";
                }
				
				//NUMERO NOTA CREDITO

                try
                {
                    if (numNC == "")
                    {
                        numNC = "";
                    }
                    else
                    {
                        if (numNC.Length > LENGTHnumNC)
                        {
                            numNC = numNC.Substring(0, LENGTHnumNC);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO NOTA CREDITO - ";
                } 
				
				//NUMERO CONTROL NOTA CREDITO

                try
                {
                    if (numCtrlNC == "")
                    {
                        numCtrlNC = "";
                    }
                    else
                    {
                        if (numCtrlNC.Length > LENGTHnumCtrlNC)
                        {
                            numCtrlNC = numCtrlNC.Substring(0, LENGTHnumCtrlNC);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO CONTROL NOTA CREDITO - ";
                }

                //TIPO TRANSACCION

                try
                {
                    if (tipoTransaccion == "")
                    {
                        error = true;
                        msjError = msjError + "TIPO DE TRANSACCION VACIO - ";
                    }
                    else
                    {
                        if (tipoTransaccion.Length > LENGTHtipoTransaccion)
                        {
                            tipoTransaccion = tipoTransaccion.Substring(0, LENGTHtipoTransaccion);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TIPO DE TRANSACCION - ";
                } 
				
				
				//FACTURA AFECTADA

                try
                {
                    if (facturaAfec == "")
                    {
                        facturaAfec = "";
                    }
                    else
                    {
                        if (facturaAfec.Length > LENGTHfacturaAfec)
                        {
                            facturaAfec = facturaAfec.Substring(0, LENGTHfacturaAfec);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION FACTURA AFECTADA - ";
                } 
								
				//TOTAL COMPRAS INCLUYE IVA DETALLE

                try
                {
                    if (totalCompIvasDet == "")
                    {
                        error = true;
                        msjError = msjError + "TOTAL COMPRAS INCLUYE IVA DETALLE VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalCompIvasDet.Substring(0, 1) == ".")
                        {
                            totalCompIvasDet = "0" + totalCompIvasDet;
                        }
                        
                        totalCompIvaDet = Convert.ToDouble(totalCompIvasDet);
                        totalCompIvaDet = Math.Round(totalCompIvaDet, 2);
                        totalCompIvasDet = Convert.ToString(totalCompIvaDet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL MONTO FACTURA - ";
                }
				
				//TOTAL COMPRAS SIN IVA

                try
                {
                    if (totalSinIvasDet == "")
                    {
                        totalSinIvaDet = 0.00;
                        totalSinIvasDet = "0.00";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalSinIvasDet.Substring(0, 1) == ".")
                        {
                            totalSinIvasDet = "0" + totalSinIvasDet;
                        }
                        
                        totalSinIvaDet = Convert.ToDouble(totalSinIvasDet);
                        totalSinIvaDet = Math.Round(totalSinIvaDet, 2);
                        totalSinIvasDet = Convert.ToString(totalSinIvaDet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL COMPRAS SIN IVA DETALLE - ";
                }

				//BASE IMPONIBLE

                try
                {
                    if (baseImps == "")
                    {
                        error = true;
                        msjError = msjError + "BASE IMPONIBLE VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (baseImps.Substring(0, 1) == ".")
                        {
                            baseImps = "0" + baseImps;
                        }
                        
                        baseImp = Convert.ToDouble(baseImps);
                        baseImp = Math.Round(baseImp, 2);
                        baseImps = Convert.ToString(baseImp);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION BASE IMPONIBLE - ";
                }

                //ALICUOTA

                try
                {
                    if (alicuotas == "")
                    {
                        error = true;
                        msjError = msjError + "ALICUOTA VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (alicuotas.Substring(0, 1) == ".")
                        {
                            alicuotas = "0" + alicuotas;
                        }
                        
                        alicuota = Convert.ToDouble(alicuotas);
                        alicuota = Math.Round(alicuota, 2);
                        alicuotas = Convert.ToString(alicuota);
                        alicuotas = alicuotas + ".00";
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION ALICUOTA - ";
                }

				
				//IMPUESTO IVA

                try
                {
                    if (impuestoIvas == "")
                    {
                        error = true;
                        msjError = msjError + "IMPUESTO IVA VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (impuestoIvas.Substring(0, 1) == ".")
                        {
                            impuestoIvas = "0" + impuestoIvas;
                        }
                        
                        impuestoIva = Convert.ToDouble(impuestoIvas);
                        impuestoIva = Math.Round(impuestoIva, 2);
                        impuestoIvas = Convert.ToString(impuestoIva);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION IMPUESTO IVA - ";
                }
				
				//IVA RETENIDO

                try
                {
                    if (ivaRets == "")
                    {
                        error = true;
                        msjError = msjError + "IVA RETENIDO VACIO - ";
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
                    msjError = msjError + "EXCEPCION IVA RETENIDO - ";
                }

                //TASA

                try
                {
                    if (tasas == "")
                    {
                        error = true;
                        msjError = msjError + "TASA VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (tasas.Substring(0, 1) == ".")
                        {
                            tasas = "0" + tasas;
                        }                        
                        
                        tasa = Convert.ToDouble(tasas);
                        tasa = Math.Round(tasa, 2);
                        tasas = Convert.ToString(tasa);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TASA - ";
                }
            }

            if ((tipoReg != "01") && (tipoReg != "02"))
            {
                error = true;
                msjError = msjError + "TIPO REGISTRO INVALIDO - " + tipoReg;
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
                                objTextFile.EscribirLog("RETENCION YA EXISTENTE. RETENCION NUMERO: " + numeroDoc + " PROVEEDOR: " + codprov + " SE ACTUALIZARA.");
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
                            //objTextFile.EscribirLog("ELIMINADA RETENCION. DOC: " + numeroDoc + " CODIGO: " + codprov + " LINEA: " + intNumeroLinea);
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
