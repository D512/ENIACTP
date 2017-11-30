SELECT *
FROM [BDTradePlace].[TradePlace].[farmatodo_encabezado_iva];
SELECT *
FROM [BDTradePlace].[TradePlace].[farmatodo_detalles_iva];
SELECT *
FROM [BDTradePlace].[TradePlace].[farmatodo_iva_tracking];


DELETE
FROM [BDTradePlace].[TradePlace].[farmatodo_encabezado_iva];
DELETE
FROM [BDTradePlace].[TradePlace].[farmatodo_detalles_iva];
DELETE
FROM [BDTradePlace].[TradePlace].[farmatodo_iva_tracking];

DROP TABLE [BDTradePlace].[TradePlace].[farmatodo_encabezado_iva];
CREATE TABLE [BDTradePlace].[TradePlace].[farmatodo_encabezado_iva](
	[hub_tp] [varchar](8) NOT NULL,
	[status_doc] [varchar](3) NOT NULL,
	[fecha_status] [datetime] NOT NULL,
	[numero_doc] [varchar](20) NOT NULL,
	[cod_prov_hub_tp] [varchar](35) NOT NULL,
	[fecha_emision] [datetime] NOT NULL,
	[fecha_entrega] [datetime] NOT NULL,
	[nom_agente_retencion] [varchar](240) NOT NULL,
	[rif_agente] [varchar](15) NOT NULL,
	[periodo_fiscal] [varchar](20) NOT NULL,
	[direccion] [varchar](150) NOT NULL,	
	[proveedor] [varchar](240) NOT NULL,
	[rif_proveedor] [varchar](15) NOT NULL,
	[total_factura] [decimal](15,2) NOT NULL,
	[total_sin_iva] [decimal](15,2) NOT NULL,
	[total_base_imponible] [decimal](15,2) NOT NULL,
	[total_iva] [decimal](15,2) NOT NULL,
	[total_iva_retenido] [decimal](15,2) NOT NULL,
	[periodo_impositivo] [varchar](35) NOT NULL,
	[total_con_iva] [decimal](15,2) NOT NULL
) ON [PRIMARY]	

DROP TABLE [BDTradePlace].[TradePlace].[farmatodo_detalles_iva];
	
CREATE TABLE [BDTradePlace].[TradePlace].[farmatodo_detalles_iva](
	[hub_tp] [varchar](8) NOT NULL,
	[status_item] [varchar](3) NOT NULL,
	[fecha_status] [datetime] NOT NULL,
	[numero_doc] [varchar](20) NOT NULL,
	[cod_prov_hub_tp] [varchar](35) NOT NULL,
    [nro_operacion] [varchar](15) NOT NULL,
	[fecha_factura] [datetime] NOT NULL,	
	[numero_factura] [varchar](20) NULL,
	[numero_control] [varchar](20) NOT NULL,
	[numero_debito] [varchar](20) NULL,
	[numero_control_debito] [varchar](20) NULL,
	[numero_credito] [varchar](20) NULL,
	[numero_control_credito] [varchar](20) NULL,
	[tipo_transaccion] [varchar](15) NOT NULL,
	[factura_afectada] [varchar](20) NULL,
	[total_con_iva] [decimal](15,2) NOT NULL,
	[compras_sin_iva] [decimal](15,2) NOT NULL,
	[base_imponible] [decimal](15,2) NOT NULL,
	[alicuota] [decimal](5,2) NOT NULL,
	[impuesto_iva] [decimal](15,2) NOT NULL,
	[iva_retenido] [decimal](15,2) NOT NULL,
	[tasa] [decimal](15,2) NOT NULL
	
) ON [PRIMARY]
DROP TABLE [BDTradePlace].[TradePlace].[farmatodo_iva_tracking];
CREATE TABLE [BDTradePlace].[TradePlace].[farmatodo_iva_tracking](
	[hub_tp] [varchar](8) NOT NULL,
	[cod_prov_hub_tp] [varchar](20) NOT NULL,
	[numero_doc] [varchar](50) NOT NULL,
	[usuario] [varchar](50) NOT NULL,
	[fecha_registro] [datetime] NOT NULL
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
