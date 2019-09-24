/****** COPY FROM STAGE to FINAL  ******/
	IF OBJECT_ID('[PH_APDEStore].[final].[bir_wa]', 'U') IS NOT NULL 
		DROP TABLE [PH_APDEStore].[final].[bir_wa]

	SELECT *
		INTO [PH_APDEStore].[final].[bir_wa]	
		FROM [PH_APDEStore].[stage].[bir_wa]


/****** ADD COLUMSTORE CLUSTERED INDEX ******/
	CREATE CLUSTERED COLUMNSTORE INDEX idx_final_bir_wa
	ON [PH_APDEStore].[final].[bir_wa]
	WITH (DROP_EXISTING = OFF)


/****** BASIC ERROR CHECKING COMPARING STAGE & FINAL ******/
	SELECT COUNT(*) FROM [PH_APDEStore].[stage].[bir_wa]
	SELECT COUNT(*) FROM [PH_APDEStore].[final].[bir_wa]

	SELECT kotelchuck, 
	count(*) FROM [PH_APDEStore].[stage].[bir_wa]
	  GROUP BY kotelchuck
	  ORDER BY -count(*)

	 SELECT kotelchuck, 
	count(*) FROM [PH_APDEStore].[final].[bir_wa]
	  GROUP BY kotelchuck
	  ORDER BY -count(*)