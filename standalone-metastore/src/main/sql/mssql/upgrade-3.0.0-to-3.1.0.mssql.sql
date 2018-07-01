SELECT 'Upgrading MetaStore schema from 3.0.0 to 3.1.0' AS MESSAGE;

-- HIVE-19440
ALTER TABLE GLOBAL_PRIVS ADD AUTHORIZER nvarchar(128) NULL;
DROP INDEX GLOBAL_PRIVS.GLOBALPRIVILEGEINDEX;
CREATE UNIQUE INDEX GLOBALPRIVILEGEINDEX ON GLOBAL_PRIVS (AUTHORIZER,PRINCIPAL_NAME,PRINCIPAL_TYPE,USER_PRIV,GRANTOR,GRANTOR_TYPE);

ALTER TABLE DB_PRIVS ADD AUTHORIZER nvarchar(128) NULL;
DROP INDEX DB_PRIVS.DBPRIVILEGEINDEX;
CREATE UNIQUE INDEX DBPRIVILEGEINDEX ON DB_PRIVS (AUTHORIZER,DB_ID,PRINCIPAL_NAME,PRINCIPAL_TYPE,DB_PRIV,GRANTOR,GRANTOR_TYPE);

ALTER TABLE TBL_PRIVS ADD AUTHORIZER nvarchar(128) NULL;
DROP INDEX TBL_PRIVS.TABLEPRIVILEGEINDEX;
CREATE INDEX TABLEPRIVILEGEINDEX ON TBL_PRIVS (AUTHORIZER,TBL_ID,PRINCIPAL_NAME,PRINCIPAL_TYPE,TBL_PRIV,GRANTOR,GRANTOR_TYPE);

ALTER TABLE PART_PRIVS ADD AUTHORIZER nvarchar(128) NULL;
DROP INDEX PART_PRIVS.PARTPRIVILEGEINDEX;
CREATE INDEX PARTPRIVILEGEINDEX ON PART_PRIVS (AUTHORIZER,PART_ID,PRINCIPAL_NAME,PRINCIPAL_TYPE,PART_PRIV,GRANTOR,GRANTOR_TYPE);

ALTER TABLE TBL_COL_PRIVS ADD AUTHORIZER nvarchar(128) NULL;
DROP INDEX TBL_COL_PRIVS.TABLECOLUMNPRIVILEGEINDEX;
CREATE INDEX TABLECOLUMNPRIVILEGEINDEX ON TBL_COL_PRIVS (AUTHORIZER,TBL_ID,"COLUMN_NAME",PRINCIPAL_NAME,PRINCIPAL_TYPE,TBL_COL_PRIV,GRANTOR,GRANTOR_TYPE);

ALTER TABLE PART_COL_PRIVS ADD AUTHORIZER nvarchar(128) NULL;
DROP INDEX PART_COL_PRIVS.PARTITIONCOLUMNPRIVILEGEINDEX;
CREATE INDEX PARTITIONCOLUMNPRIVILEGEINDEX ON PART_COL_PRIVS (AUTHORIZER,PART_ID,"COLUMN_NAME",PRINCIPAL_NAME,PRINCIPAL_TYPE,PART_COL_PRIV,GRANTOR,GRANTOR_TYPE);

-- HIVE-19340
ALTER TABLE TXNS ADD TXN_TYPE int NULL;

CREATE INDEX TAB_COL_STATS_IDX ON TAB_COL_STATS (CAT_NAME, DB_NAME, TABLE_NAME, COLUMN_NAME);

ALTER TABLE PART_COL_STATS ADD DB_EXTERNAL_LOCATION_URI nvarchar(4000) NULL;

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='3.1.0', VERSION_COMMENT='Hive release version 3.1.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.0.0 to 3.1.0' AS MESSAGE;
