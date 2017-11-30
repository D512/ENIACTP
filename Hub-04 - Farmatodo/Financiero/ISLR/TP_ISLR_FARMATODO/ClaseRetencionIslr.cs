
using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_RETENCIONISLR
{
	/// <summary>
	/// Clase Pagos
	/// </summary>
	public class ClaseRetencionIslr
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
		const int LENGTHnumDoc =  20;

        //Código Proveedor	
        String codProv = String.Empty;
        const int LENGTHcodProv = 35;

		//Fecha Emisión	
		String fechaEmision = String.Empty;	
		const int LENGTHfechaEmision = 8;

        //Período Fiscal	
        String periodoFiscal = String.Empty;
        const int LENGTHperiodoFiscal = 6;
		
		//Nombre o Razón Social del Agente de Retención	
		String agente = String.Empty;	
		const int LENGTHagente = 240;
		
		//RIF del Agente de Retención	
		String rifAgente = String.Empty;	
		const int LENGTHrifAgente = 15;

        //Dirección Fiscal del Agente de Retención	
        String direccionAg = String.Empty;
        const int LENGTHdireccionAg = 150;
		
		//Nombre o Razón Social del Sujeto Retenido (Proveedor)	
		String proveedor = String.Empty;	
		const int LENGTHproveedor = 240;

		//RIF del Proveedor	
		String rifProv = String.Empty;	
		const int LENGTHrifProv = 15;

        //Dirección del Proveedor
        String direccionProv = String.Empty;
        const int LENGTHdireccionProv = 150;

        // Total Abonado
        String totalAbonados = String.Empty;
        double totalAbonado = 0;
		
		//Total Cnat Objeto de Retencion
		String totalObjRets = String.Empty;
        double totalObjRet = 0;
		
		//Total Impuesto Retenido
		String totalImpRets = String.Empty;	
		double totalImpRet = 0;
		
		//Total Monto a Pagar	
		String totalPagars = String.Empty;	
		double totalPagar = 0;
		
		//Total Cantidad Compra	
		String totalCantComps = String.Empty;
        double totalCantComp = 0;

        //Tipo Proveedor
        String tipoProv = String.Empty;
        const int LENGTHtipoProv = 30;
		
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

        //Fecha del Documento
        String fechaDoc = String.Empty;
        const int LENGTHfechaDoc = 8;

        //Número de Cobro	
        String numCobro = String.Empty;
        const int LENGTHnumCobro = 15;

        //Tipo de Documento
        String tipoDoc = String.Empty;
        const int LENGTHtipoDoc = 15;

        //Número de Factura	
        String numFactura = String.Empty;
        const int LENGTHnumFactura = 20;

        //Número Control 
        String numCtrl = String.Empty;
        const int LENGTHnumCtrl = 20;

        //Fecha de la Factura	
        String fechaFactura = String.Empty;
        const int LENGTHfechaFactura = 8;

		//Número Operación	
		String numOp = String.Empty;	
		const int LENGTHnumOp = 15;

        //Total Factura
        String totalFacturas = String.Empty;
        double totalFactura = 0;

        //Monto Abonado
        String montoAbonados = String.Empty;
        double montoAbonado = 0;

        //Cantidad Objeto de Retencion
        String cantObjRets = String.Empty;
        double cantObjRet = 0;

        //% Alícuota	
        String alicuotas = String.Empty;
        double alicuota = 0;

        //Impuesto Retenido
        String impRets = String.Empty;
        double impRet = 0;

        //Mínimo	
        String minimos = String.Empty;
        double minimo = 0;

        //Sustraendo
        String sustraendos = String.Empty;
        double sustraendo = 0;

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
		public ClaseRetencionIslr()
		{
		}

        /// <summary>
        /// Metodo Insercion ISLR
        /// </summary>
        /// <param name="strFileName">Archivo a procesar</param>
		public void InsertRetencionIslr(string strFileName)
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

                        if (intNumeroLinea == 84)
                        {
                            if (true) { }
                        }

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
                                        querySql = querySql + Class1.strCampoEnc16 + ", " + Class1.strCampoEnc17 + ", " + Class1.strCampoEnc18 + ", " + Class1.strCampoEnc19;
                                        querySql = querySql + ") VALUES ";
                                        querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";
                                        querySql = querySql + numDoc + "','";
                                        querySql = querySql + codProv + "',";
                                        querySql = querySql + "convert(datetime,'" + fechaEmision + "',112),'";
                                        querySql = querySql + periodoFiscal + "','";
                                        querySql = querySql + agente + "','";
										querySql = querySql + rifAgente + "','";
                                        querySql = querySql + direccionAg + "','";
                                        querySql = querySql + proveedor + "','";                                        
										querySql = querySql + rifProv + "','";
                                        querySql = querySql + direccionProv + "',";
                                        querySql = querySql + totalAbonados.Replace(",", ".") + ",";
                                        querySql = querySql + totalObjRets.Replace(",", ".") + ",";
                                        querySql = querySql + totalImpRets.Replace(",", ".") + ",";
                                        querySql = querySql + totalPagars.Replace(",", ".") + ",";
                                        querySql = querySql + totalCantComps.Replace(",", ".") + ",'";
                                        querySql = querySql + tipoProv + "');";

                                        if (ExecuteQuery(numDoc, codProv, querySql, 2, 1))
                                        {
                                            objTextFile.EscribirLog("RETENCION ISLR INSERTADA. DOCUMENTO: " + numDoc + " PROVEEDOR: " + proveedor + " CODIGO: " + codProv + " LINEA: " + intNumeroLinea);

                                            //PREPARA INSERT TRACKING
                                            querySql = "INSERT INTO " + Class1.strTableName03 + "(" + Class1.strCampoTra01 + ", " + Class1.strCampoTra02 + ", " + Class1.strCampoTra03 + ", ";
                                            querySql = querySql + Class1.strCampoTra04 + ", " + Class1.strCampoTra05 + ", " + Class1.strCampoTra06 + ", " + Class1.strCampoTra07 + ") VALUES ";
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
                                    querySql = querySql + Class1.strCampoEnc08 + ", " + Class1.strCampoEnc09 + ", " + Class1.strCampoEnc10 + ", " + Class1.strCampoEnc11 + ", ";
                                    querySql = querySql + Class1.strCampoEnc12 + ", " + Class1.strCampoEnc13 + ", " + Class1.strCampoEnc14 + ", " + Class1.strCampoEnc15 + ", ";
                                    querySql = querySql + Class1.strCampoEnc16 + ", " + Class1.strCampoEnc17 + ", " + Class1.strCampoEnc18 + ", " + Class1.strCampoEnc19;
                                    querySql = querySql + ") VALUES ";
                                    querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";
                                    querySql = querySql + numDoc + "','";
                                    querySql = querySql + codProv + "',";
                                    querySql = querySql + "convert(datetime,'" + fechaEmision + "',112),'";
                                    querySql = querySql + periodoFiscal + "','";
                                    querySql = querySql + agente + "','";
                                    querySql = querySql + rifAgente + "','";
                                    querySql = querySql + direccionAg + "','";
                                    querySql = querySql + proveedor + "','";
                                    querySql = querySql + rifProv + "','";
                                    querySql = querySql + direccionProv + "',";
                                    querySql = querySql + totalAbonados.Replace(",", ".") + ",";
                                    querySql = querySql + totalObjRets.Replace(",", ".") + ",";
                                    querySql = querySql + totalImpRets.Replace(",", ".") + ",";
                                    querySql = querySql + totalPagars.Replace(",", ".") + ",";
                                    querySql = querySql + totalCantComps.Replace(",", ".") + ",'";
                                    querySql = querySql + tipoProv + "');";

                                    if (ExecuteQuery(numDoc, codProv, querySql, 2, 1))
                                    {
                                        objTextFile.EscribirLog("RETENCION ISLR INSERTADA. DOCUMENTO: " + numDoc + " PROVEEDOR: " + proveedor + " CODIGO: " + codProv + " LINEA: " + intNumeroLinea);

                                        //PREPARA INSERT TRACKING
                                        querySql = "INSERT INTO " + Class1.strTableName03 + "(" + Class1.strCampoTra01 + ", " + Class1.strCampoTra02 + ", " + Class1.strCampoTra03 + ", ";
                                        querySql = querySql + Class1.strCampoTra04 + ", " + Class1.strCampoTra05 + ", " + Class1.strCampoTra06 + ", " + Class1.strCampoTra07 + ") VALUES ";
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
                                querySql = querySql + Class1.strCampoDet16 + ", " + Class1.strCampoDet17 + ", " + Class1.strCampoDet18 + ", " + Class1.strCampoDet19;
                                querySql = querySql + ") VALUES ";
                                querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";                                        
                                querySql = querySql + numDocDet + "','";
                                querySql = querySql + codProvDet + "',";
                                querySql = querySql + "convert(datetime,'" + fechaDoc + "',112),'";
                                querySql = querySql + numCobro + "','";
                                querySql = querySql + tipoDoc + "','";
                                querySql = querySql + numFactura + "','";
                                querySql = querySql + numCtrl + "',";
                                querySql = querySql + "convert(datetime,'" + fechaFactura + "',112),'";
								querySql = querySql + numOp + "',";
                                querySql = querySql + totalFacturas.Replace(",", ".") + ",";
                                querySql = querySql + montoAbonados.Replace(",", ".") + ",";
								querySql = querySql + cantObjRets.Replace(",", ".") + ",";
                                querySql = querySql + alicuotas.Replace(",", ".") + ",";
								querySql = querySql + impRets.Replace(",", ".") + ",";
                                querySql = querySql + minimos.Replace(",", ".") + ",";
                                querySql = querySql + sustraendos + ");";

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
                        codProv = arrCamposLinea.GetValue(9).ToString().Trim();
                        codProv = Caracteres(codProv);

                        //Fecha Emisión	
                        fechaEmision = arrCamposLinea.GetValue(2).ToString().Trim();
                        fechaEmision = Caracteres(fechaEmision);

                        //Período Fiscal	
                        periodoFiscal = arrCamposLinea.GetValue(3).ToString().Trim();
                        periodoFiscal = Caracteres(periodoFiscal);

                        //Nombre o Razón Social del Agente de Retención	
                        agente = arrCamposLinea.GetValue(4).ToString().Trim();
                        agente = Caracteres(agente);

                        //RIF del Agente de Retención	
                        rifAgente = arrCamposLinea.GetValue(5).ToString().Trim();
                        rifAgente = Caracteres(rifAgente);

                        //Dirección Fiscal del Agente de Retención	
                        direccionAg = arrCamposLinea.GetValue(6).ToString().Trim();
                        direccionAg = Caracteres(direccionAg);

                        //Nombre o Razón Social del Sujeto Retenido (Proveedor)	
                        proveedor = arrCamposLinea.GetValue(7).ToString().Trim();
                        proveedor = Caracteres(proveedor);

                        //RIF del Proveedor	
                        rifProv = arrCamposLinea.GetValue(8).ToString().Trim();
                        rifProv = Caracteres(rifProv);

                        //Dirección del Proveedor
                        direccionProv = arrCamposLinea.GetValue(10).ToString().Trim();
                        direccionProv = Caracteres(direccionProv);

                        //Total Monto Abonado	
                        totalAbonados = arrCamposLinea.GetValue(11).ToString().Trim();
                        totalAbonados = Caracteres(totalAbonados);

                        //Total Cant. Objeto Retención
                        totalObjRets = arrCamposLinea.GetValue(12).ToString().Trim();
                        totalObjRets = Caracteres(totalObjRets);

                        //Total Impuesto Retenido
                        totalImpRets = arrCamposLinea.GetValue(13).ToString().Trim();
                        totalImpRets = Caracteres(totalImpRets);

                        //Total Monto Pagar	
                        totalPagars = arrCamposLinea.GetValue(14).ToString().Trim();
                        totalPagars = Caracteres(totalPagars);

                        //Total Cantidad Compra	
                        totalCantComps = arrCamposLinea.GetValue(15).ToString().Trim();
                        totalCantComps = Caracteres(totalCantComps);

                        //Tipo Agente Sujeto Retendido
                        tipoProv = arrCamposLinea.GetValue(16).ToString().Trim();
                        tipoProv = Caracteres(tipoProv);

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

                //DIRECCION PROVEEDOR

                try
                {
                    if (direccionProv == "")
                    {
                        error = true;
                        msjError = msjError + "DIRECCION PROVEEDOR - ";
                    }
                    else
                    {
                        if (direccionProv.Length > LENGTHdireccionProv)
                        {
                            direccionProv = direccionProv.Substring(0, LENGTHdireccionProv);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION DIRECCION PROVEEDOR - ";
                }

                //TOTAL MONTO ABONAD

                try
                {
                    if (totalAbonados == "")
                    {
                        totalAbonados = "";
                    }
                    else
                    {
                        totalAbonado = Convert.ToDouble(totalAbonados);
                        totalAbonado = Math.Round(totalAbonado, 2);
                        totalAbonados = Convert.ToString(totalAbonado);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL MONTO ABONADO - ";
                }
                       
                //TOTAL CANTIDAD OBJETO DE RETENCION

                try
                {
                    if (totalObjRets == "")
                    {
                        totalObjRets = "";
                    }
                    else
                    {
                        totalObjRet = Convert.ToDouble(totalObjRets);
                        totalObjRet = Math.Round(totalObjRet, 2);
                        totalObjRets = Convert.ToString(totalObjRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL CANTIDAD OBJETO DE RETENCION - ";
                }

                //TOTAL IMPUESTO RETENIDO

                try
                {
                    if (totalImpRets == "")
                    {
                        error = true;
                        msjError = msjError + "TOTAL IMPUESTO RETENIDO VACIO - ";
                    }
                    else
                    {
                        totalImpRet = Convert.ToDouble(totalImpRets);
                        totalImpRet = Math.Round(totalImpRet, 2);
                        totalImpRets = Convert.ToString(totalImpRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL IMPUESTO RETENIDO - ";
                }

                //TOTAL MONTO A PAGAR

                try
                {
                    if (totalPagars == "")
                    {
                        error = true;
                        msjError = msjError + "TOTAL MONTO A PAGAR VACIO - ";
                    }
                    else
                    {
                        totalPagar = Convert.ToDouble(totalPagars);
                        totalPagar = Math.Round(totalPagar, 2);
                        totalPagars = Convert.ToString(totalPagar);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL MONTO A PAGAR - ";
                }

                //TOTAL CANTIDAD COMPRA

                try
                {
                    if (totalCantComps == "")
                    {
                        error = true;
                        msjError = msjError + "TOTAL CANTIDAD COMPRA VACIO - ";
                    }
                    else
                    {
                        totalCantComp = Convert.ToDouble(totalCantComps);
                        totalCantComp = Math.Round(totalCantComp, 2);
                        totalCantComps = Convert.ToString(totalCantComp);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL CANTIDAD COMPRA - ";
                }

                //TIPO DE AGENTE SUJETO RETENIDO

                try
                {
                    if (tipoProv != "")
                    {
                        if (tipoProv.Length > LENGTHtipoProv)
                        {
                            tipoProv = tipoProv.Substring(0, LENGTHtipoProv);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TIPO DE AGENTE SUJETO RETENIDO  - ";
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

                    //Fecha del Documento	
                    fechaDoc = arrCamposLinea.GetValue(3).ToString().Trim();
                    fechaDoc = Caracteres(fechaDoc);

                    //Número de Cobro
                    numCobro = arrCamposLinea.GetValue(4).ToString().Trim();
                    numCobro = Caracteres(numCobro);

                    //Tipo de Documento
                    tipoDoc = arrCamposLinea.GetValue(5).ToString().Trim();
                    tipoDoc = Caracteres(tipoDoc);

                    //Número de Factura	
                    numFactura = arrCamposLinea.GetValue(6).ToString().Trim();
                    numFactura = Caracteres(numFactura);

                    //Número Control 
                    numCtrl = arrCamposLinea.GetValue(7).ToString().Trim();
                    numCtrl = Caracteres(numCtrl);

                    //Fecha de la Factura	
                    fechaFactura = arrCamposLinea.GetValue(8).ToString().Trim();
                    fechaFactura = Caracteres(fechaFactura);

					//Número Operación	
					numOp = arrCamposLinea.GetValue(9).ToString().Trim();	
					numOp = Caracteres(numOp);
				
					//Total Factura
                    totalFacturas = arrCamposLinea.GetValue(10).ToString().Trim();
                    totalFacturas = Caracteres(totalFacturas);

                    //Monto Abonado
                    montoAbonados = arrCamposLinea.GetValue(11).ToString().Trim();
                    montoAbonados = Caracteres(montoAbonados);

					//Cantidad Objeto a Retencion	
                    cantObjRets = arrCamposLinea.GetValue(12).ToString().Trim();
                    cantObjRets = Caracteres(cantObjRets);

                    //% Alícuota	
                    alicuotas = arrCamposLinea.GetValue(13).ToString().Trim();
                    alicuotas = Caracteres(alicuotas);

                    //Impuesto Retenido
                    impRets = arrCamposLinea.GetValue(14).ToString().Trim();
                    impRets = Caracteres(impRets);

                    //Minimo
                    minimos = arrCamposLinea.GetValue(15).ToString().Trim();
                    minimos = Caracteres(minimos);

                    //Sustraendo
                    sustraendos = arrCamposLinea.GetValue(16).ToString().Trim();
                    sustraendos = Caracteres(sustraendos);
					
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

                //FECHA DEL DOCUMENTO

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

                //NUMERO DE COBRO

                try
                {
                    if (numCobro == "")
                    {
                        numCobro = "";
                    }
                    else
                    {
                        if (numCobro.Length > LENGTHnumCobro)
                        {
                            numCobro = numCobro.Substring(0, LENGTHnumCobro);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION NUMERO DE COBRO - ";
                }

                //TIPO DE DOCUMENTO

                try
                {
                    if (tipoDoc == "")
                    {
                        tipoDoc = "";
                    }
                    else
                    {
                        if (tipoDoc.Length > LENGTHtipoDoc)
                        {
                            tipoDoc = tipoDoc.Substring(0, LENGTHtipoDoc);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TIPO DE DOCUMENTO - ";
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

                //NUMERO CONTROL 

                try
                {
                    if (numCtrl == "")
                    {
                        error = true;
                        msjError = msjError + "NUMERO CONTROL VACIO - ";
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
                    msjError = msjError + "EXCEPCION NUMERO CONTROL - ";
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
				
				//NUMERO OPERACION

                try
                {
                    if (numOp == "")
                    {
                        error = true;
                        msjError = msjError + "NUMERO OPERACION VACIO - ";
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
				
				//MONTO ABONADO

                try
                {
                    if (montoAbonados == "")
                    {
                        montoAbonado = 0.00;
                        montoAbonados = "0.00";
                    }
                    else
                    {
                        montoAbonado = Convert.ToDouble(montoAbonados);
                        montoAbonado = Math.Round(montoAbonado, 2);
                        montoAbonados = Convert.ToString(montoAbonado);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MONTO ABONADO - ";
                }

                //CANTIDAD OBJETO A RETENCION

                try
                {
                    if (cantObjRets == "")
                    {
                        error = true;
                        msjError = msjError + "CANTIDAD OBJETO A RETENCION VACIO - ";
                    }
                    else
                    {
                        cantObjRet = Convert.ToDouble(cantObjRets);
                        cantObjRet = Math.Round(cantObjRet, 2);
                        cantObjRets = Convert.ToString(cantObjRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION CANTIDAD OBJETO A RETENCION - ";
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

				
				//IMPUESTO RETENIDO

                try
                {
                    if (impRets == "")
                    {
                        error = true;
                        msjError = msjError + "IMPUESTO RETENIDO VACIO - ";
                    }
                    else
                    {
                        impRet = Convert.ToDouble(impRets);
                        impRet = Math.Round(impRet, 2);
                        impRets = Convert.ToString(impRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION IMPUESTO RETENIDO - ";
                }
				
				//MINIMO

                try
                {
                    if (minimos == "")
                    {
                        error = true;
                        msjError = msjError + "MINIMO VACIO - ";
                    }
                    else
                    {
                        minimo = Convert.ToDouble(minimos);
                        minimo = Math.Round(minimo, 2);
                        minimos = Convert.ToString(minimo);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION MINIMO - ";
                }

                //SUSTRAENDO

                try
                {
                    if (sustraendos == "")
                    {
                        error = true;
                        msjError = msjError + "SUSTRAENDO VACIO - ";
                    }
                    else
                    {
                        sustraendo = Convert.ToDouble(sustraendos);
                        sustraendo = Math.Round(sustraendo, 2);
                        sustraendos = Convert.ToString(sustraendo);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION SUSTRAENDO - ";
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
                // TRATAR DE HACER UN REINTENTO DE CONEXION A BASE DE DATOS
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
