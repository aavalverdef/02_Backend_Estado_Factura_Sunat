--TABLAS PARA EL CRUCE CON EL API DE SUNAT

/* 1) COLA PARA EL WORKER DOCKER */
IF OBJECT_ID('INH.API_SUNAT_QUEUE','U') IS NULL
BEGIN
  CREATE TABLE INH.API_SUNAT_QUEUE (
      IdQueue       BIGINT IDENTITY(1,1) PRIMARY KEY,
      IdFactura     BIGINT        NOT NULL,
      RUC_Emisor    VARCHAR(20)   NOT NULL,
      RUC_Receptor  VARCHAR(20)   NULL,
      TipoDocumento CHAR(2)       NOT NULL,
      Serie         VARCHAR(10)   NOT NULL,
      Numero        VARCHAR(20)   NOT NULL,
      FechaEmision  DATE          NULL,
      ImporteTotal  DECIMAL(18,2) NULL,
      EnqueuedAt    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
      Status        VARCHAR(20)   NOT NULL DEFAULT 'queued',  -- queued|processing|done|error
      Attempts      INT           NOT NULL DEFAULT 0,
      LastError     NVARCHAR(4000) NULL
  );
  CREATE INDEX IX_SUNAT_QUEUE_STATUS ON INH.API_SUNAT_QUEUE(Status, EnqueuedAt);
END
GO

/* 2) HISTORIAL DE RESPUESTAS (una fila por consulta a SUNAT) */
IF OBJECT_ID('INH.SUNAT_VALIDACION','U') IS NULL
BEGIN
  CREATE TABLE INH.SUNAT_VALIDACION (
      IdValidacion     BIGINT IDENTITY(1,1) PRIMARY KEY,
      IdFactura        BIGINT       NOT NULL,
      RUC_Emisor       VARCHAR(20)  NOT NULL,
      RUC_Receptor     VARCHAR(20)  NULL,
      TipoDocumento    CHAR(2)      NOT NULL,
      Serie            VARCHAR(10)  NOT NULL,
      Numero           VARCHAR(20)  NOT NULL,
      FechaEmision     DATE         NULL,
      ImporteTotal     DECIMAL(18,2) NULL,
      Estado_SUNAT     VARCHAR(40)  NULL,       -- p.ej. ACEPTADO
      Codigo_Respuesta VARCHAR(40)  NULL,
      Mensaje          NVARCHAR(500) NULL,
      Fecha_Consulta   DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
      Token_Expira_UTC DATETIME2    NULL,
      Raw_JSON         NVARCHAR(MAX) NULL
  );
  CREATE INDEX IX_SUNAT_VAL_FACT ON INH.SUNAT_VALIDACION(IdFactura, Fecha_Consulta DESC);
END
GO

/* 3) SNAPSHOT (último estado por factura; no se trunca) */
IF OBJECT_ID('INH.SUNAT_ESTADO_ACTUAL','U') IS NULL
BEGIN
  CREATE TABLE INH.SUNAT_ESTADO_ACTUAL (
    IdFactura               BIGINT      PRIMARY KEY,
    RUC_Emisor              VARCHAR(20) NOT NULL,
    RUC_Receptor            VARCHAR(20) NULL,
    TipoDocumento           CHAR(2)     NOT NULL,
    Serie                   VARCHAR(10) NOT NULL,
    Numero                  VARCHAR(20) NOT NULL,
    ImporteTotal            DECIMAL(18,2) NULL,
    Estado_Actual           VARCHAR(40)   NULL,
    Estado_Descripcion      NVARCHAR(200) NULL,
    Codigo_Respuesta        VARCHAR(40)   NULL,
    Mensaje                 NVARCHAR(500) NULL,
    Fecha_Primera_Consulta  DATETIME2     NULL,
    Fecha_Ultima_Consulta   DATETIME2     NULL,
    Fecha_Ultimo_Cambio     DATETIME2     NULL,
    Cambio_Estado           BIT           NOT NULL DEFAULT 0
  );
  CREATE INDEX IX_SUNAT_ACT_CLAVE ON INH.SUNAT_ESTADO_ACTUAL(RUC_Emisor, TipoDocumento, Serie, Numero);
END
GO
