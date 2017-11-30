SELECT *
FROM [BDTradePlace].[TradePlace].[farmatodo_encabezado_islr]
SELECT *
FROM [BDTradePlace].[TradePlace].[farmatodo_detalles_islr]
SELECT *
FROM [BDTradePlace].[TradePlace].[farmatodo_islr_tracking]


DELETE
FROM [BDTradePlace].[TradePlace].[farmatodo_encabezado_islr]
DELETE
FROM [BDTradePlace].[TradePlace].[farmatodo_detalles_islr]
DELETE
FROM [BDTradePlace].[TradePlace].[farmatodo_islr_tracking]

DROP TABLE [BDTradePlace].[TradePlace].[farmatodo_encabezado_islr];
CREATE TABLE [BDTradePlace].[TradePlace].[farmatodo_encabezado_islr](
	[hub_tp] [varchar](8) NOT NULL,
	[status_doc] [varchar](3) NOT NULL,
	[fecha_status] [datetime] NOT NULL,
	[numero_doc] [varchar](20) NOT NULL,
	[cod_prov_hub_tp] [varchar](35) NOT NULL,
	[fecha_emision] [datetime] NOT NULL,
	[periodo_fiscal] [varchar](20) NOT NULL,
	[nom_agente_retencion] [varchar](240) NOT NULL,
	[rif_agente] [varchar](15) NOT NULL,
	[direccion_agente] [varchar](150) NOT NULL,	
	[proveedor] [varchar](240) NOT NULL,
	[rif_proveedor] [varchar](15) NOT NULL,
	[direccion_prov] [varchar](150) NOT NULL,	
	[total_abonado] [decimal](15,2) NULL,
	[total_obj_ret] [decimal](15,2) NULL,
	[total_imp_ret] [decimal](15,2) NOT NULL,
	[total_pagar] [decimal](15,2) NOT NULL,
	[total_cant_comp] [decimal](15,2) NOT NULL,
	[tipo_prov] [varchar](30) NULL

) ON [PRIMARY]	

DROP TABLE [BDTradePlace].[TradePlace].[farmatodo_detalles_islr];
	
CREATE TABLE [BDTradePlace].[TradePlace].[farmatodo_detalles_islr](
	[hub_tp] [varchar](8) NOT NULL,
	[status_item] [varchar](3) NOT NULL,
	[fecha_status] [datetime] NOT NULL,
	[numero_doc] [varchar](20) NOT NULL,
	[cod_prov_hub_tp] [varchar](35) NOT NULL,
	[fecha_doc] [datetime] NOT NULL,	
	[numero_cobro] [varchar](20) NULL,
	[tipo_doc] [varchar](20) NULL,
	[numero_factura] [varchar](20) NOT NULL,
	[numero_control] [varchar](20) NOT NULL,
	[fecha_factura] [datetime] NOT NULL,
	[nro_operacion] [varchar](35) NOT NULL,
	[total_factura] [decimal](15,2) NOT NULL,
	[monto_abonado] [decimal](15,2) NULL,
	[can_obj_ret] [decimal](15,2) NOT NULL,
	[alicuota] [decimal](5,2) NOT NULL,
	[impuesto_ret] [decimal](15,2) NOT NULL,
	[minimo] [decimal](15,2) NULL,
	[sustraendo] [decimal](15,2) NOT NULL
	
) ON [PRIMARY]
DROP TABLE [BDTradePlace].[TradePlace].[farmatodo_islr_tracking];
CREATE TABLE [BDTradePlace].[TradePlace].[farmatodo_islr_tracking](
	[hub_tp] [varchar](8) NOT NULL,
	[cod_prov_hub_tp] [varchar](20) NOT NULL,
	[numero_doc] [varchar](50) NOT NULL,
	[usuario] [varchar](50) NOT NULL,
	[fecha_registro] [datetime] NOT NULL,
	[status_ini] [varchar](100) NOT NULL,
	[status_fin] [varchar](100) NOT NULL
) ON [PRIMARY]



SELECT e.cod_prov_hub_tp,e.numero_doc, e.fecha_emision, e.status_doc, 
e.nom_suj_retenido, e.nom_agente_retencion, e.rif_agente, numero_factura 
FROM farmatodo_encabezado_iva e INNER JOIN farmatodo_detalles_iva d 
ON e.numero_doc = d.numero_doc AND e.cod_prov_hub_tp = d.cod_prov_hub_tp 
WHERE e.fecha_emision >= Convert(datetime,'13/5/2017',103) AND e.fecha_emision <= Convert(datetime,'13/6/2017',103) 
GROUP BY e.cod_prov_hub_tp,e.numero_doc, e.fecha_emision, e.status_doc, e.nom_suj_retenido, e.nom_agente_retencion, 
e.rif_agente, numero_factura ORDER BY e.numero_doc, e.fecha_emision, e.nom_suj_retenido


INSERT INTO farmatodo_encabezado_iva (
hub_tp, status_doc, fecha_status, numero_doc, cod_prov_hub_tp,
fecha_emision, fecha_entrega, nom_agente_retencion, rif_agente, periodo_fiscal,
direccion, nom_suj_retenido, rif_proveedor, 
total_factura, total_credito_iva,base_imponible, total_iva, total_iva_retenido) VALUES  (
'HUB-51','100',GETDATE(),'20170600009652','APSC',
convert(datetime,'20170609',112),convert(datetime,'20170609',112),'VELAS 3N, C.A.','J-3043670-5','201706',
'CALLE LAS FLORES SECT.AGUA SALADA GALPON 1, CD. BOLIVAR','AUTO PINTURAS SISTEM COLORS CA','J-30833959-8',
11999.99,0,10714.28,1285.71,964.28);
