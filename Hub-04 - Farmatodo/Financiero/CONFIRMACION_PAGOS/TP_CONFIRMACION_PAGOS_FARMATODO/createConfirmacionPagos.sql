SELECT *
FROM [BDTradePlace].[TradePlace].[farmatodo_encabezado_conf_pagos];
SELECT *
FROM [BDTradePlace].[TradePlace].[farmatodo_detalles_conf_pagos];
SELECT *
FROM [BDTradePlace].[TradePlace].[farmatodo_conf_pagos_tracking];


DELETE
FROM [BDTradePlace].[TradePlace].[farmatodo_encabezado_conf_pagos];
DELETE
FROM [BDTradePlace].[TradePlace].[farmatodo_detalles_conf_pagos];
DELETE
FROM [BDTradePlace].[TradePlace].[farmatodo_conf_pagos_tracking];

DROP TABLE [BDTradePlace].[TradePlace].[farmatodo_encabezado_conf_pagos];
CREATE TABLE [BDTradePlace].[TradePlace].[farmatodo_encabezado_conf_pagos](

	[hub_tp] [varchar](8) NOT NULL,
	[status_doc] [varchar](3) NOT NULL,
	[fecha_status] [datetime] NOT NULL,
	[numero_doc] [varchar](35) NOT NULL,
	[cod_prov_hub_tp] [varchar](35) NOT NULL,
	[proveedor] [varchar](150) NOT NULL,
	[rif_proveedor] [varchar](15) NOT NULL,
	[fecha_doc] [datetime] NOT NULL,
	[monto_pago] [decimal](18,2) NOT NULL,
	[nombre_comprador] [varchar](150) NULL,
	[rif_comprador] [varchar](15)NULL

) ON [PRIMARY]	

DROP TABLE [BDTradePlace].[TradePlace].[farmatodo_detalles_conf_pagos];
	
CREATE TABLE [BDTradePlace].[TradePlace].[farmatodo_detalles_conf_pagos](

	[hub_tp] [varchar](8) NOT NULL,
	[status_item] [varchar](3) NOT NULL,
	[fecha_status] [datetime] NOT NULL,
	[numero_doc] [varchar](35) NOT NULL,
	[cod_prov_hub_tp] [varchar](35) NOT NULL,
    [numero_factura] [varchar](35) NOT NULL,
	[fecha_factura] [datetime] NOT NULL,	
	[monto_factura] [decimal](18,2) NOT NULL,
	[descripcion] [varchar](150) NULL,
	[cod_localizacion] [varchar](35) NULL,
	[nombre_tienda] [varchar](150) NULL,
	[banco_emisor] [varchar](150) NOT NULL,
	[cod_emisor] [varchar](35) NULL,
	[banco_receptor] [varchar](150) NOT NULL,
	[cod_receptor] [varchar](35) NULL,
	[cuenta_receptora] [varchar](35) NULL,
	[tipo_pago] [varchar](35) NOT NULL,
	[monto_gravable] [decimal](18,2) NOT NULL,
	[monto_exento] [decimal](18,2) NOT NULL,
	[monto_impuesto] [decimal](18,2) NOT NULL,
	[iva_retenido] [decimal](18,2) NOT NULL,
	[islr_retenido] [decimal](18,2) NOT NULL,
	[descuento] [decimal](18,2) NOT NULL,
	[monto_pagado_factura] [decimal](18,2) NOT NULL,
	[importe_pagado_anteriores] [decimal](18,2) NOT NULL
	
) ON [PRIMARY]
DROP TABLE [BDTradePlace].[TradePlace].[farmatodo_conf_pagos_tracking];
CREATE TABLE [BDTradePlace].[TradePlace].[farmatodo_conf_pagos_tracking](
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
