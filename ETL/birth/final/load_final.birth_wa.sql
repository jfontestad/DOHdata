/****** CHANGE SCHEMA FROM STAGE to FINAL  ******/
DROP TABLE [PH_APDEStore].[final].[bir_wa]
ALTER SCHEMA [final] 
    TRANSFER [stage].[bir_wa]

/****** ADD COLUMSTORE CLUSTERED INDEX ******/
	CREATE CLUSTERED COLUMNSTORE INDEX idx_final_bir_wa
	ON [PH_APDEStore].[final].[bir_wa]
	WITH (DROP_EXISTING = OFF)

