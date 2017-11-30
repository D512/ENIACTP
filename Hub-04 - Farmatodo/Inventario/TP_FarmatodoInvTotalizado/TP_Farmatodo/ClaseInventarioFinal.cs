using System;
using System.IO;
using System.Threading;
using System.Collections;
using System.Data.SqlClient;


namespace TP_Farmatodo
{
	/// <summary>
	/// Summary description for ClaseInventarioFinal.
	/// </summary>
	public class ClaseInventarioFinal
	{


		#region Clases
		public static ClaseBaseDatos objBDatos = new ClaseBaseDatos();
		public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
		#endregion


		public ClaseInventarioFinal()
		{
			//
			// TODO: Add constructor logic here
			//
		}
		
		public void insertarInventarioFinal()
		{
			string strSql1="";
			string strSql2="";
			string strSql3="";
			string FechaInv="";
			string strErrorIntentos = "NO";
			int intReintentos = 4; // REINTENTA UNA VEZ MENOS DE LO ESPECIFICADO
			int intReintentosAplicados = 0;
			int Cant_Reg = 0;

			// INSTANCIA LA CONEXION DE BASE DE DATOS
			SqlConnection mySqlConnection, mySqlConnection2 = new SqlConnection();
			SqlCommand mySqlCommand = new SqlCommand();
			mySqlConnection = objBDatos.Conexion();
			mySqlConnection2 = objBDatos.Conexion();
			
			
			//REALIZAMOS TRUNCATE DE LAS TABLAS DE INVENTARIO TOTAL
			strSql1 = "TRUNCATE TABLE farmatodo_inventario_total";

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

						//objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS");

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

			try // TRUNCA LA TABLA DE INVENTARIO TOTAL
			{
				mySqlCommand = objBDatos.Comando(strSql1, mySqlConnection);
				mySqlCommand.ExecuteNonQuery();	
			}
			catch (Exception ex)
			{
				objTextFile.EscribirLog("  ---> ERROR -- ERROR ELIMINANDO LA TABLA DE TOTALES. CODIGO: " + ex.Message );
			}

			
			// GENERA EL QUERY DE SELECT EN LA BD
			strSql2 = "SELECT  det.codigo_prod_comprador, SUM(det.cantidad_existencia) AS existencia, SUM(det.cantidad_ordenada) AS ordenada, SUM(det.ventas_diaria)";
			strSql2 = strSql2 + "AS ventas_diarias, enc.fecha_doc, det.codigo_prod_barras, det.descripcion, det.cod_prov_hub_tp,det.codigo_prod_proveedor ";
			strSql2 = strSql2 + "FROM TradePlace.farmatodo_encabezado_inventario enc INNER JOIN ";
			strSql2 = strSql2 + "TradePlace.farmatodo_detalles_inventario det ON det.Numero_doc = enc.numero_doc AND det.cod_prov_hub_tp = enc.cod_prov_hub_tp ";
			strSql2 = strSql2 + "Inner join TradePlace.farmatodo_localidades loc on (det.codigo_localizacion = loc.codigo_localizacion) ";
			strSql2 = strSql2 + "GROUP BY ALL det.codigo_prod_comprador, enc.fecha_doc, det.codigo_prod_barras, det.descripcion, det.cod_prov_hub_tp,det.codigo_prod_proveedor ";
			strSql2 = strSql2 + "ORDER BY det.codigo_prod_comprador ";

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

						//objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS");

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

			try // SE EJECUTA EL SELECT
			{
				mySqlCommand = objBDatos.Comando(strSql2, mySqlConnection);
				SqlDataReader rs = mySqlCommand.ExecuteReader ();	

				while (rs.Read())
				{
						
					FechaInv = rs.GetDateTime(4).Year.ToString() + rs.GetDateTime(4).Month.ToString().PadLeft(2, '0') + rs.GetDateTime(4).Day.ToString().PadLeft(2, '0');

					strSql3 = "INSERT INTO farmatodo_inventario_total (hub_tp, status_doc, codigo_prod_comprador,";
					strSql3 = strSql3 + "total_existencia, total_ordenado, total_venta,fecha_inv, ";
					strSql3 = strSql3 + "codigo_prod_barra, descripcion, cod_prov_hub_tp, codigo_prod_proveedor, fecha_status";
					strSql3 = strSql3 + ") VALUES ('HUB-04', '100','" ;
					strSql3 = strSql3 + rs.GetString(0) + "', ";
					strSql3 = strSql3 + rs.GetDecimal(1).ToString().Replace(",",".") + ", ";
					strSql3 = strSql3 + rs.GetDecimal(2).ToString().Replace(",",".") + ", ";
					strSql3 = strSql3 + rs.GetDecimal(3).ToString().Replace(",",".") + ", ";
					strSql3 = strSql3 + "CONVERT(DATETIME, '" + FechaInv + "')" + " ,'";
					strSql3 = strSql3 + rs.GetString(5) + "','";
					strSql3 = strSql3 + rs.GetString(6).Trim() + "','";
					strSql3 = strSql3 + rs.GetString(7).Trim() + "','";
					strSql3 = strSql3 + rs.GetString(8).Trim() + "',";
					strSql3 = strSql3 + "CONVERT(DATETIME, GetDate()))";

					// VERIFICA SI LA CONEXION A BD ESTA CERRADA PARA ABRIRLA
					if(mySqlConnection2.State.ToString() == "Closed")
					{

						// TRATAR DE HACER UN REINTENTO DE CONEXION A BASE DE DATOS.
						intReintentosAplicados = 1;
						strErrorIntentos = "NO";
									
						while ((intReintentosAplicados) == intReintentos || (strErrorIntentos != "SI"))
										
						{
							try
							{
								mySqlConnection2.Open();

								//objTextFile.EscribirLog("ABRIENDO CONEXION CON LA BASE DE DATOS");

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
						// INSERTA EL REGISTRO DEL INVENTARIO FINAL

						mySqlCommand = objBDatos.Comando(strSql3, mySqlConnection2);
						mySqlCommand.ExecuteNonQuery();
						Cant_Reg+=1;
					}

					catch(Exception ex)
					{

						objTextFile.EscribirLog("  ---> ERROR -- ERROR INSERTANDO EN LA TABLA DE TOTALES: SKU " + rs.GetString(0) + ".  CODIGO: " + ex.Message );
						objTextFile.EscribirLog("  ---> QUERY: " + strSql3 );
					}
				}//fin del while
			}
			catch (Exception ex2)
			{
				objTextFile.EscribirLog("  ---> ERROR -- ERROR EN EL QUERY SELECT. CODIGO: " + ex2.Message );
				objTextFile.EscribirLog("  ---> QUERY: " + strSql2 );
			}
			
			// INSERT EN EL LOG DE LA CANTIDAD DE DETALLES INSERTADOS POR EL ENCABEZADO
			objTextFile.EscribirLog("SE INSERTARON " + Cant_Reg + " REGISTROS EN EL INVENTARIO TOTAL DE FARMATODO");
				
			
			// VERIFICA SI LA CONEXION A BD ESTA ABIERTA PARA CERRARLA Y FINALIZAR LA OPERACION DE IMPORT
			if((mySqlConnection.State.ToString() == "Open") || (mySqlConnection2.State.ToString() == "Open"))
			{
				mySqlConnection.Close();
				mySqlConnection2.Close();
				objTextFile.EscribirLog("CERRANDO CONEXION CON LA BASE DE DATOS");
			}
						
		}

	}
}
