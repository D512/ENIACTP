
using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_RETENCIONARCV
{
	/// <summary>
	/// Clase Pagos
	/// </summary>
	public class ClaseRetencionArcv
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

        //Fecha Documento
        String fechaDoc = String.Empty;
        const int LENGTHfechaDoc = 8;

        //Tipo De agente de Retencion
        String tipoPersona = String.Empty;
        const int LENGTHtipoPersona = 10;
		
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
		String fechaDocDet = String.Empty;	
		const int LENGTHfechaDocDet = 8;

        //Fecha del Pago
        String fechaPago = String.Empty;
        const int LENGTHfechaPago = 8;

		//Descripcion
		String descripcion = String.Empty;	
		const int LENGTHdescripcion = 60;

        //Código de retención de impuesto	
		String codRet = String.Empty;	
		const int LENGTHcodRet = 20 ;

        //Total Importe Pagado
        String totalPagados = String.Empty;
        double totalPagado = 0;

        //Cantidad Objeto Retención (Base Retención)
        String cantObjRets = String.Empty;
        double cantObjRet = 0;

        //Tasa Impositiva % Retención	
        String tasas = String.Empty;
        double tasa = 0;

        //Importe Impuesto Retenido	
        String impuestoRets = String.Empty;
        double impuestoRet = 0;

        //Total Cantidad Objeto Retención Acumulada 
        String totalRetAcums = String.Empty;
        double totalRetAcum = 0;

        //Periodo	
        String periodo = String.Empty;
        const int LENGTHperiodo = 6;

        //Importe Impuesto Retenido Total
        String totalImpRets = String.Empty;
        double totalImpRet = 0;

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
		public ClaseRetencionArcv()
		{
		}

        /// <summary>
        /// Metodo Insercion ARCV
        /// </summary>
        /// <param name="strFileName">Archivo a Procesar</param>
		public void InsertRetencionArcv(string strFileName)
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
                                        querySql = querySql + Class1.strCampoEnc12 + ", " + Class1.strCampoEnc13;
                                        querySql = querySql + ") VALUES ";
                                        querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";
                                        querySql = querySql + numDoc + "','";
                                        querySql = querySql + codProv + "',";
                                        querySql = querySql + "convert(datetime,'" + fechaDoc + "',112),'";
                                        querySql = querySql + tipoPersona + "','";
                                        querySql = querySql + agente + "','";
										querySql = querySql + rifAgente + "','";
                                        querySql = querySql + direccionAg + "','";
                                        querySql = querySql + proveedor + "','";                                               
										querySql = querySql + rifProv + "','";
                                        querySql = querySql + direccionProv + "');";

                                        if (ExecuteQuery(numDoc, codProv, querySql, 2, 1))
                                        {
                                            objTextFile.EscribirLog("RETENCION IVA INSERTADA. DOCUMENTO: " + numDoc + " PROVEEDOR: " + proveedor + " CODIGO: " + codProv + " LINEA: " + intNumeroLinea);

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
                                    querySql = querySql + Class1.strCampoEnc12 + ", " + Class1.strCampoEnc13;
                                    querySql = querySql + ") VALUES ";
                                    querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";
                                    querySql = querySql + numDoc + "','";
                                    querySql = querySql + codProv + "','";
                                    querySql = querySql + tipoPersona + "','";
                                    querySql = querySql + agente + "','";
                                    querySql = querySql + rifAgente + "','";
                                    querySql = querySql + direccionAg + "',";
                                    querySql = querySql + "convert(datetime,'" + fechaDoc + "',112),'";
                                    querySql = querySql + proveedor + "','";
                                    querySql = querySql + rifProv + "','";
                                    querySql = querySql + direccionProv + "');";

                                    if (ExecuteQuery(numDoc, codProv, querySql, 2, 1))
                                    {
                                        objTextFile.EscribirLog("RETENCION ARCV INSERTADA. DOCUMENTO: " + numDoc + " PROVEEDOR: " + proveedor + " CODIGO: " + codProv + " LINEA: " + intNumeroLinea);

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
                                querySql = querySql + Class1.strCampoDet16;
                                querySql = querySql + ") VALUES ";
                                querySql = querySql + " ('" + Class1.strHub + "','100',GETDATE(),'";                                        
                                querySql = querySql + numDocDet + "','";
                                querySql = querySql + codProvDet + "',";
                                querySql = querySql + "convert(datetime,'" + fechaDoc + "',112),";
                                querySql = querySql + "convert(datetime,'" + fechaPago + "',112),'";
                                querySql = querySql + descripcion + "','";
								querySql = querySql + codRet + "',";
                                querySql = querySql + totalPagados.Replace(",", ".") + ",";
                                querySql = querySql + cantObjRets.Replace(",", ".") + ",";
								querySql = querySql + tasas.Replace(",", ".") + ",";
                                querySql = querySql + impuestoRets.Replace(",", ".") + ",";
								querySql = querySql + totalRetAcums.Replace(",", ".") + ",'";
                                querySql = querySql + periodo + "',";
                                querySql = querySql + totalImpRets + ");";

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

                        //Fecha Documento	
                        fechaDoc = arrCamposLinea.GetValue(6).ToString().Trim();
                        fechaDoc = Caracteres(fechaDoc);

                        //Código Proveedor	
                        codProv = arrCamposLinea.GetValue(8).ToString().Trim();
                        codProv = Caracteres(codProv);

                        //Tipo De agente de Retencion
                        tipoPersona = arrCamposLinea.GetValue(2).ToString().Trim();
                        tipoPersona = Caracteres(tipoPersona);

                        //Nombre o Razón Social del Agente de Retención	
                        agente = arrCamposLinea.GetValue(3).ToString().Trim();
                        agente = Caracteres(agente);

                        //RIF del Agente de Retención	
                        rifAgente = arrCamposLinea.GetValue(4).ToString().Trim();
                        rifAgente = Caracteres(rifAgente);

                        //Dirección Fiscal del Agente de Retención	
                        direccionAg = arrCamposLinea.GetValue(5).ToString().Trim();
                        direccionAg = Caracteres(direccionAg);

                        //Nombre o Razón Social del Sujeto Retenido (Proveedor)	
                        proveedor = arrCamposLinea.GetValue(7).ToString().Trim();
                        proveedor = Caracteres(proveedor);

                        //RIF del Proveedor	
                        rifProv = arrCamposLinea.GetValue(9).ToString().Trim();
                        rifProv = Caracteres(rifProv);

                        //Dirección Proveedor	
                        direccionProv = arrCamposLinea.GetValue(10).ToString().Trim();
                        direccionProv = Caracteres(direccionProv);

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

				//TIPO DE AGENTE DE RETENCION

                try
                {
                    if (tipoPersona == "")
                    {
                        tipoPersona = "";
                    }
                    else
                    {
                        if (tipoPersona.Length > LENGTHtipoPersona)
                        {
                            tipoPersona = tipoPersona.Substring(0, LENGTHtipoPersona);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TIPO PERSONA - ";
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
                        msjError = msjError + "DIRECCION PROVEEDOR VACIO - ";
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

                    //Fecha del Pago o Abono	
                    fechaPago = arrCamposLinea.GetValue(3).ToString().Trim();
                    fechaPago = Caracteres(fechaPago);

					//Decripcion
					descripcion = arrCamposLinea.GetValue(4).ToString().Trim();	
					descripcion = Caracteres(descripcion);
					
					//Codigo de Retencion de Impuesto
					codRet = arrCamposLinea.GetValue(5).ToString().Trim();	
					codRet = Caracteres(codRet);
					
					//Total Importe Pagado
                    totalPagados = arrCamposLinea.GetValue(6).ToString().Trim();
                    totalPagados = Caracteres(totalPagados);

                    //Cantidad Objeto Retencion 
                    cantObjRets = arrCamposLinea.GetValue(7).ToString().Trim();
                    cantObjRets = Caracteres(cantObjRets);

					//Tasa Impositiva % Retencion	
                    tasas = arrCamposLinea.GetValue(8).ToString().Trim();
                    tasas = Caracteres(tasas);

                    //Total Cantidad Objeto de Retencion Acumulada	
                    impuestoRets = arrCamposLinea.GetValue(9).ToString().Trim();
                    impuestoRets = Caracteres(impuestoRets);

                    //Total Rets Acumulada
                    totalRetAcums = arrCamposLinea.GetValue(10).ToString().Trim();
                    totalRetAcums = Caracteres(totalRetAcums);

                    //Periodo
                    periodo = arrCamposLinea.GetValue(11).ToString().Trim();
                    periodo = Caracteres(periodo);

                    //Importe Impuesto Retenido Total
                    totalImpRets = arrCamposLinea.GetValue(12).ToString().Trim();
                    totalImpRets = Caracteres(totalImpRets);

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
		
               //FECHA PAGO O ABONO

                try
                {
                    if (fechaPago == "")
                    {
                        fechaPago = "";
                    }
                    else
                    {
                        if (fechaPago.Length > LENGTHfechaPago)
                        {
                            fechaPago = fechaPago.Substring(0, LENGTHfechaPago);
                        }

                        string dia, mes, ano;

                        //Separa fecha
                        dia = fechaPago.Substring(6, 2);
                        mes = fechaPago.Substring(4, 2);
                        ano = fechaPago.Substring(0, 4);
                        fechaPago = ano + mes + dia;

                        int idia = Int32.Parse(dia);
                        int imes = Int32.Parse(mes);
                        int iano = Int32.Parse(ano);

                        if ((idia < 1) || (idia > 31))
                        {
                            msjError = msjError + "FECHA PAGO DIA INVALIDO -- ";
                            error = true;
                        }
                        if ((imes < 1) || (imes > 12))
                        {
                            msjError = msjError + "FECHA PAGO MES INVALIDO -- ";
                            error = true;
                        }
                        if ((iano < 1) || (iano > 3000))
                        {
                            msjError = msjError + "FECHA PAGO ANO INVALIDO -- ";
                            error = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION FECHA PAGO - ";
                }
				
				//DESCRIPCION

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
                    msjError = msjError + "EXCEPCION DESCRIPCION - ";
                } 
				
				//CODIGO DE RETENCION DE IMPUESTO

                try
                {
                    if (codRet == "")
                    {
                        codRet = "";
                    }
                    else
                    {
                        if (codRet.Length > LENGTHcodRet)
                        {
                            codRet = codRet.Substring(0, LENGTHcodRet);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION CODIGO DE RETENCION DE IMPUESTO - ";
                } 
								
				//TOTAL IMPORTE PAGADO

                try
                {
                    if (totalPagados == "")
                    {
                        error = true;
                        msjError = msjError + "TOTAL IMPORTE PAGADO VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalPagados.Substring(0, 1) == ".")
                        {
                            totalPagados = "0" + totalPagados;
                        }
                        
                        totalPagado = Convert.ToDouble(totalPagados);
                        totalPagado = Math.Round(totalPagado, 2);
                        totalPagados = Convert.ToString(totalPagado);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION TOTAL IMPORTE PAGADO - ";
                }
				
				//CANTIDAD OBJETO DE RETENCION

                try
                {
                    if (cantObjRets == "")
                    {
                        error = true;
                        msjError = msjError + "CANTIDAD OBJETO DE RETENCION VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (cantObjRets.Substring(0, 1) == ".")
                        {
                            cantObjRets = "0" + cantObjRets;
                        }
                        
                        cantObjRet = Convert.ToDouble(cantObjRets);
                        cantObjRet = Math.Round(cantObjRet, 2);
                        cantObjRets = Convert.ToString(cantObjRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION CANTIDAD OBJETO DE RETENCION - ";
                }

				//TASA IMPOSITIVA % RETENCION

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

                //TOTAL IMPUESTO RETENIDO

                try
                {
                    if (impuestoRets == "")
                    {
                        error = true;
                        msjError = msjError + "IMPUESTO RETENIDO VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (impuestoRets.Substring(0, 1) == ".")
                        {
                            impuestoRets = "0" + impuestoRets;
                        }
                        
                        impuestoRet = Convert.ToDouble(impuestoRets);
                        impuestoRet = Math.Round(impuestoRet, 2);
                        impuestoRets = Convert.ToString(impuestoRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION IMPUESTO RETENIDO - ";
                }

                //TOTAL CANTIDAD OBJETO DE RETENCIONA CUMULADA

                try
                {
                    if (totalRetAcums == "")
                    {
                        error = true;
                        msjError = msjError + "RETENCION ACUMULADA VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalRetAcums.Substring(0, 1) == ".")
                        {
                            totalRetAcums = "0" + totalRetAcums;
                        }
                        
                        totalRetAcum = Convert.ToDouble(totalRetAcums);
                        totalRetAcum = Math.Round(totalRetAcum, 2);
                        totalRetAcums = Convert.ToString(totalRetAcum);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION RETENCION ACUMULADA - ";
                }

                //PERIODO

                try
                {
                    if (periodo == "")
                    {
                        error = true;
                        msjError = msjError + "PERIODO VACIO - ";
                    }
                    else
                    {
                        if (periodo.Length > LENGTHperiodo)
                        {
                            periodo = periodo.Substring(0, LENGTHperiodo);
                        }
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION PERIODO - ";
                }

				
				//IMPORTE IMPUESTO RETENIDO TOTAL

                try
                {
                    if (totalImpRets == "")
                    {
                        error = true;
                        msjError = msjError + "IMPORTE IMPUESTO RETENIDO TOTAL VACIO - ";
                    }
                    else
                    {
                        //Fix Caso .01
                        if (totalImpRets.Substring(0, 1) == ".")
                        {
                            totalImpRets = "0" + totalImpRets;
                        }
                        
                        totalImpRet = Convert.ToDouble(totalImpRets);
                        totalImpRet = Math.Round(totalImpRet, 2);
                        totalImpRets = Convert.ToString(totalImpRet);
                    }
                }
                catch (Exception e)
                {
                    error = true;
                    msjError = msjError + "EXCEPCION IMPORTE IMPUESTO RETENIDO TOTAL - ";
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
