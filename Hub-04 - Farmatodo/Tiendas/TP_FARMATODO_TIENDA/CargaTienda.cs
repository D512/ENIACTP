using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_CargaTienda
{
	/// <summary>
	/// Summary description for ClaseOrdenCompra.
	/// </summary>
    public class ClaseCargaTienda
	{
		
		#region Clases
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();

		#endregion

        public ClaseCargaTienda()
		{
			//
			// TODO: Add constructor logic here
			//
		}

        public void InsertCargaTienda(string strFileName)
		{

			string query = "";
			string strErrorIntentos = "NO";
			string ErrorTipoRegistro = "NO";
			int intReintentos = 4; // REINTENTA UNA VEZ MENOS DE LO ESPECIFICADO
			int intNumeroLinea = 0;
			int intReintentosAplicados = 0;
            string strCodLocalizacion = "";
            string strTempCodLocalizacion = "";
            string strLocalizacion = "";
			
			string strFaltaCamposEncabezado = "NO";
            
            int can_art = 0;
			

			string strDecimal = System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator;
						
			try
			{

				// CREA UNA INSTANCIA DE StreamReader PARA LEER DEL ARCHIVO.
				using (StreamReader sr = new StreamReader(strFileName, System.Text.Encoding.Default)) 
				{

					string strLineaLeida;
					Array arrCamposLinea;

					
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
						arrCamposLinea = strLineaLeida.Split('|');

                        if (strLineaLeida != "") 
                        {
                            string strFecha = Convert.ToString(System.DateTime.Now);
                            ErrorTipoRegistro = "NO";
                            strFaltaCamposEncabezado = "NO";

                            #region VALIDA CAMPOS REQUERIDOS

                            try
                            {

                                if (arrCamposLinea.GetValue(0).ToString().Trim() == "")
                                {
                                    objTextFile.EscribirLog("---> ERROR SE REQUIERE VALOR PARA EL CAMPO(1) CODIGO TIENDA, LINEA: " + intNumeroLinea);
                                    strFaltaCamposEncabezado = "SI";
                                    ErrorTipoRegistro = "SI";

                                }

                                if (arrCamposLinea.GetValue(1).ToString().Trim() == "")
                                {
                                    objTextFile.EscribirLog("---> ERROR SE REQUIERE VALOR PARA EL CAMPO(2) NOMBRE TIENDA , LINEA: " + intNumeroLinea);
                                    strFaltaCamposEncabezado = "SI";
                                    ErrorTipoRegistro = "SI";

                                }


                            }
                            catch (Exception ex)
                            {
                                Class1.strCodError = ex.Message;
                                objTextFile.EscribirLog("  ---> ERROR LEYENDO ARCHIVO CODIGO:" + Class1.strCodError + " LINEA: " + intNumeroLinea);
                                strFaltaCamposEncabezado = "SI";
                                ErrorTipoRegistro = "SI";
                            }


                            #endregion



                            if (strFaltaCamposEncabezado == "NO")
                            {

                                strCodLocalizacion = arrCamposLinea.GetValue(0).ToString().Trim();
                                strLocalizacion = arrCamposLinea.GetValue(1).ToString().Trim();



                                query = " SELECT * FROM " + Class1.strTableName01 + " ";
                                query += " WHERE codigo_localizacion = '" + strCodLocalizacion + "' ";


                                #region VERIFICA SI LA CONEXION A BD ESTA CERRADA PARA ABRIRLA

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
                                    objTextFile.EscribirLog("  ---> ERROR SELECT ENCABEZADO. CODIGO: " + Class1.strCodError + " .LINEA: " + intNumeroLinea);
                                    objTextFile.EscribirLog("  ---> ERROR -- QUERY: " + query);
                                }


                                while (myReader.Read())
                                {
                                    strTempCodLocalizacion = myReader["codigo_localizacion"].ToString();
                                    break;
                                }

                                myReader.Close();

                                if (strTempCodLocalizacion== strCodLocalizacion)
                                {

                                    query = "UPDATE " + Class1.strTableName01 + " SET ";
                                    query += "localizacion='" + strLocalizacion + "', ";
                                    query += "direccion='" + arrCamposLinea.GetValue(2).ToString().Trim() + "' ";    
                                    query += "WHERE codigo_localizacion='" + strTempCodLocalizacion + "' ";


                                    #region // VERIFICA SI LA CONEXION A BD ESTA CERRADA PARA ABRIRLA
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
                                                //objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS");
                                                strErrorIntentos = "SI";
                                            }
                                            catch (Exception ex)
                                            {
                                                Class1.strCodError = ex.Message;
                                                objTextFile.EscribirLog("  ---> ERROR -- ERROR ABRIENDO LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados + "  CODIGO: " + Class1.strCodError);
                                                intReintentosAplicados += intReintentosAplicados;
                                                // SLEEP DEL SISTEMA
                                                Thread.Sleep(15000);
                                            }

                                        } // FIN DEL WHILE

                                    } // FIN DEL IF DE LA CONEXION CERRADA							
                                    #endregion
                                    try
                                    {
                                        // MODIFICA EL REGISTRO DE ENCABEZADO
                                        mySqlCommand = objBDatos.Comando(query, mySqlConnection);
                                        mySqlCommand.ExecuteNonQuery();                                       
                                         can_art++;
                                    }
                                    catch (Exception ex)
                                    {
                                        Class1.strCodError = ex.Message;
                                        strFaltaCamposEncabezado = "SI";
                                        objTextFile.EscribirLog("  ---> ERROR -- ACTUALIZANDO TIENDAS: " + Class1.strCodError + " .LINEA: " + intNumeroLinea);
                                        objTextFile.EscribirLog(" QUERY: " + query);
                                    }

                                }
                                else
                                {

                                    query = " INSERT INTO " + Class1.strTableName01 + " (codigo_localizacion, localizacion, direccion) ";
                                    query += " VALUES ";
                                    query += " ('" + strCodLocalizacion + "', '" + strLocalizacion + "','" + arrCamposLinea.GetValue(2).ToString().Trim() + "') ";


                                    #region VERIFICA SI LA CONEXION A BD ESTA CERRADA PARA ABRIRLA
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
                                                //objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS");
                                                strErrorIntentos = "SI";
                                            }
                                            catch (Exception ex)
                                            {
                                                Class1.strCodError = ex.Message;
                                                objTextFile.EscribirLog("  ---> ERROR -- ERROR ABRIENDO LA BASE DE DATOS, INTENTO NUMERO: " + intReintentosAplicados + "  CODIGO: " + Class1.strCodError);

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
                                        can_art++;

                                    }
                                    catch (Exception ex)
                                    {
                                        Class1.strCodError = ex.Message;
                                        objTextFile.EscribirLog("  ---> ERROR -- ERROR CODIGO: " + Class1.strCodError + " LINEA: " + intNumeroLinea);
                                        objTextFile.EscribirLog("  ---> ERROR -- QUERY: " + query);
                                        ErrorTipoRegistro = "SI";
                                    }

                                }

                            }//fin (strFaltaCamposEncabezado == "NO")
                        
                        }

						


					}// fin while

			
					
					if(	ErrorTipoRegistro != "SI")
					{
						// INSERT EN EL LOG 
						objTextFile.EscribirLog("SE INSERTARON Y/O MODIFICARON " +  can_art + " REGISTROS" );			
					}


				}
				
			}
			catch (Exception ex) 
			{
				Class1.strCodError = ex.Message;
				objTextFile.EscribirLog("  ---> ERROR -- ERROR LEYENDO EL ARCHIVO DE ENTRADA: " + strFileName + "  .CODIGO: " + Class1.strCodError +" .LINEA: " + intNumeroLinea);				
			}
		}

	}//FIN FUNCION

}
