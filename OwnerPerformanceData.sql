IF Object_ID('[Ingestion.Facts].OwnerPerformanceData') IS NOT NULL
    DROP PROC [Ingestion.Facts].OwnerPerformanceData
GO

CREATE	PROCEDURE [Ingestion.Facts].OwnerPerformanceData 

WITH EXECUTE AS OWNER AS


BEGIN TRY 
SET NOCOUNT ON
SET XACT_ABORT ON 


DECLARE @source VARCHAR(50) = 'ConsolidatedData'
		
BEGIN TRANSACTION 

---------------------------------------------------------------------------------------
--Table Full refresh
---------------------------------------------------------------------------------------
	
TRUNCATE TABLE FranchiseAccess.Fact_OwnerPerformanceData					


---------------------------------------------------------------------------
--Main Calculations
---------------------------------------------------------------------------

;WITH	
cte_Actual AS 
	(
		SELECT 
			F.FranchiseKey FranchiseKey,F.FranchiseId, F.Number Number,F.LegalOwners,F.City, F.State, F.Status,F.Country,
			MMF.DateKey,D.MonthYear,D.YearMonth,D.QuarterName,mmf.CompletedRevenueMonth,
			RANK()  OVER (PARTITION BY mmf.FranchiseKey ORDER BY mmf.DateKey DESC) rnk  ,
			F.SeniorsInTerritory,  
			MMF.TotalActualRevenue, 
			MMF.TotalBillableHours ,
			MMF.TwelveMonthRollingBillableHours,MMF.TwelveMonthRollingBillableHoursPriorYear,
			CAST(FA.FranchiseRenewalDate AS DATE) RenewalDate,
			PerformanceInceptionDate,
			(DATEDIFF(mm,PerformanceInceptionDate, D.FirstDayOfMonth)) /12 AS AgeOfFranchise_AsOfGivenMonth  
		FROM 
			Common.Fact_MonthlyMetricsByFranchise MMF 
			JOIN 
			Datamart.Dim_Franchise F ON F.FranchiseKey = MMF.FranchiseKey
			JOIN 
			Common.Dim_Date D ON D.DateKey = MMF.DateKey
			JOIN 
			Common.Dim_FranchiseAgreement FA ON FA.FranchiseNumber = F.Number and F.Deleted = 0 
		WHERE 
			MMF.DateKey>=20220101
	),
cte_MinStandards AS 
	(
		SELECT 
			SeniorPopulationMin, 
			SeniorPopulationMax,
			MinYearsInBusiness,
			YearsInBusiness AS MaxYearsInBusiness, 
			MinClientServiceHours * 27.07	MinGrossSales, 
			MinClientServiceHours,MinPerformanceType
		FROM 
			FranchisePerformanceTracker.Dim_MinimumPerfomance 
		WHERE 
			MinPerformanceType IN ('US', 'CA') 	
	),
cte_EnterpriseFranchises AS 
	(
		SELECT 
			DISTINCT f.Number
		FROM 
			Common.Dim_Franchise F 
			JOIN Common.Bridge_LegalEntityFranchise LF on LF.FranchiseKey = F.FranchiseKey and LF.Deleted =0 
			JOIN Common.Dim_LegalEntity L on L.LegalEntityKey = LF.LegalEntityKey and L.Deleted = 0
			JOIN Common.Bridge_LegalEntityOwner LO ON LO.LegalEntityKey = L.LegalEntityKey and LO.Deleted = 0
			JOIN Common.Dim_Owner O on O.OwnerKey = LO.OwnerKey and LO.Deleted = 0
		WHERE 
			O.OwnerKey IN (
				SELECT 
					O.ownerKey
				FROM 
					Common.Dim_Franchise F 
					JOIN Common.Bridge_LegalEntityFranchise LF on LF.FranchiseKey = F.FranchiseKey and LF.Deleted =0 
					JOIN Common.Dim_LegalEntity L on L.LegalEntityKey = LF.LegalEntityKey and L.Deleted = 0
					JOIN Common.Bridge_LegalEntityOwner LO ON LO.LegalEntityKey = L.LegalEntityKey and LO.Deleted = 0
					JOIN Common.Dim_Owner O on O.OwnerKey = LO.OwnerKey and LO.Deleted = 0
				GROUP BY 
					O.ownerKey
				HAVING COUNT(DISTINCT f.franchiseKey)>1 
							)
	),
cte_CombinActualAndTarget AS 
	(
		SELECT FranchiseKey,a.FranchiseId,
			a.Number, LegalOwners,City, State, Status,Country,
			CASE WHEN ent.Number IS NOT NULL THEN 'Enterprise' ELSE 'Single'  END AS 'Enterprise/Single',
			--------------
			DateKey,MonthYear,YearMonth, QuarterName CurrentQuarter,CompletedRevenueMonth,
			CASE WHEN rnk = 1 THEN 1 ELSE 0 END AS LatestCompletedRevenueMonth,
			SeniorsInTerritory,  
			TotalActualRevenue MeasuredRevenue, 
			MinGrossSales TargetRevenue, 
			CASE WHEN MinGrossSales = 0 THEN 100 ELSE CAST(TotalActualRevenue/MinGrossSales  AS DECIMAL(18,6)) END PercentTargetRevenue,
			TotalBillableHours MeasuredHours,
			MinClientServiceHours TargetHours,
			CASE WHEN MinClientServiceHours = 0 THEN 100 ELSE CAST(TotalBillableHours /MinClientServiceHours  AS DECIMAL(18,6)) END PercentTargetHours,
			TwelveMonthRollingBillableHours,TwelveMonthRollingBillableHoursPriorYear,
			RenewalDate,
			PerformanceInceptionDate,
			AgeOfFranchise_AsOfGivenMonth,
			SeniorPopulationMin, 
			SeniorPopulationMax,
			MinYearsInBusiness,
			MaxYearsInBusiness, 
			MinGrossSales, 
			MinClientServiceHours
		FROM 
			cte_Actual a 
			LEFT JOIN cte_MinStandards m 
			ON a.SeniorsInTerritory BETWEEN m.SeniorPopulationMin AND m.SeniorPopulationMax
				AND a.AgeOfFranchise_AsOfGivenMonth BETWEEN MinYearsInBusiness AND MaxYearsInBusiness
				AND a.Country = m.MinPerformanceType
			LEFT JOIN cte_EnterpriseFranchises ent ON  a.number = ent.Number
	) ,
cte_MostRecentQuarterForGivenMonth AS 
	(
		SELECT 
			d.Year, d.Quarter, d.QuarterName,
			MIN(d.firstDayOfMonth) QuarterStartDate,
			MIN(CAST(DATEADD( QUARTER, DATEDIFF( QUARTER, 0, d.firstDayOfMonth) - 1, 0) AS DATE )) 	AS PriorQuarterStartDatefrom,
			pq.QuarterName priorQName,d.MonthYear, d.YearMonth,d.FirstDayOfMonth,
			DATEPART(QUARTER, d.FirstDayOfMonth) QuaterNumber, DATEPART(Month, d.FirstDayOfMonth) monthnumber,
			CASE WHEN DATEPART(Month, d.FirstDayOfMonth) in (3,6,9,12) THEN d.QuarterName ELSE pq.QuarterName END AS MostRecentCompletedQuarter

		FROM 
			common.dim_date d 
			JOIN common.dim_date pq ON  (CAST(DATEADD( QUARTER, DATEDIFF( QUARTER, 0, d.firstDayOfMonth) - 1, 0) AS DATE )) = pq.Date
		WHERE 			d.year >=2020 --in (2020, 2021,2022, 2023)
		GROUP BY 
			d.Year, d.Quarter, d.QuarterName,pq.QuarterName, d.MonthYear, d.YearMonth,d.FirstDayOfMonth
			,DATEPART(QUARTER, d.FirstDayOfMonth), DATEPART(Month, d.FirstDayOfMonth)
		),
	cte_QuarterlyMetrics AS 
	(
		SELECT  
			FranchiseKey,Year, Quarter, QuarterName,min(firstDayOfMonth) QuarterStartDate,
			SUM(TotalBillableHours) QTRTotalBillableHours  , SUM(TotalBillableHoursPriorYear) QTRTotalBillableHoursPriorYear 
		FROM 
			Common.Fact_MonthlyMetricsByFranchise f
			JOIN 
			Common.dim_date d ON d.datekey = f.datekey		
		GROUP BY  
			FranchiseKey,Year, Quarter, QuarterName
	),
cte_finalSet AS 
	(
		SELECT 
			a.*
			,mapping.MostRecentCompletedQuarter
			,CASE WHEN PercentTargetHours >=1 OR PercentTargetrevenue >=1 THEN 'Would Meet' ELSE 'Would Not Meet' END AS Category,
			CASE WHEN TwelveMonthRollingBillableHoursPriorYear = 0 THEN 0 ELSE (TwelveMonthRollingBillableHours-TwelveMonthRollingBillableHoursPriorYear)/TwelveMonthRollingBillableHoursPriorYear END AS LTMGrowth,
			CASE WHEN q.QTRTotalBillableHoursPriorYear = 0 THEN 0 ELSE (QTRTotalBillableHours-QTRTotalBillableHoursPriorYear)/QTRTotalBillableHoursPriorYear END AS QTRHoursGrowth,
			q.QTRTotalBillableHours,
			TwelveMonthRollingBillableHours AS TwelveMonthRollingBillableHours_1,
			(q.QTRTotalBillableHours*4)/a.SeniorsInTerritory hoursPerSenior_Qtr,
			TwelveMonthRollingBillableHours/a.SeniorsInTerritory hoursPerSenior_LTM	
		FROM 
			cte_CombinActualAndTarget a 
			JOIN 
			cte_QuarterlyMetrics q 	ON q.FranchiseKey = a.FranchiseKey 
			JOIN 
			cte_MostRecentQuarterForGivenMonth mapping 
			on mapping.YearMonth = a.YearMonth and mapping.MostRecentCompletedQuarter = q.QuarterName
	)

SELECT  
		DateKey,
		a.FranchiseKey, 
		a.CurrentQuarter,
		MostRecentCompletedQuarter																								AS PreviousQuarter,
		NULL																													AS ScoreCardStatus , 
		LegalOwners																												AS ActiveOwnersName, 
		City, 
		State, 
		NULL																													AS GracePeriod , 
		NULL																													AS NoScorecard,
		a.[Enterprise/Single]																									AS Enterprise,
		a.Status,
		a.RenewalDate,
		Category ,			
		Category																												AS PerformanceStandard,
		AgeOfFranchise_AsOfGivenMonth																							AS PerformanceYears,
		NULL																													AS UsesAlternatePerformanceDate, 
		CASE 
		WHEN SeniorPopulationMax = 99999999 
		THEN '85K +' ELSE CAST(SeniorPopulationMax AS VARCHAR(30)) 
		END																														AS TerritorySize ,
		TargetHours, 
		MeasuredHours,
		PercentTargetHours																										AS PercentofHourTarget,
		TargetRevenue, 
		MeasuredRevenue,
		PercentTargetRevenue																									AS PercentOfRevenueTarget,
			CASE 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) < -.15 THEN '7%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= -.15 AND GREATEST(LTMGrowth, QTRHoursGrowth) <0 THEN '6%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= 0 AND GREATEST(LTMGrowth, QTRHoursGrowth) <.05  THEN '5.5%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= .05 AND GREATEST(LTMGrowth, QTRHoursGrowth) <.1 THEN '5%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= .1 AND GREATEST(LTMGrowth, QTRHoursGrowth) <.15 THEN '4.5%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= .15 THEN '4%' ELSE 'NA' 
		END																														AS ImpliedRoyaltyRate,
		hoursPerSenior_LTM																										AS LTMHoursPerSenior	,
		hoursPerSenior_Qtr																										AS QuarterlyHoursPerSenior	,
		GREATEST(hoursPerSenior_Qtr, hoursPerSenior_LTM)																		AS BetterOfQtrOrAnnual,	
		NULL																													AS HPSPercentileRange	, ----------Need to confirm if this can be null
		QTRHoursGrowth																											AS LastQuarterGrowth	,
		LTMGrowth																												AS LTMHoursGrowth	,
		GREATEST(LTMGrowth, QTRHoursGrowth)																						AS MAXHoursGrowth	,
		QTRTotalBillableHours																									AS LastQuarterHours	,
		CASE 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) < -.15 THEN '<-15%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= -.15 AND GREATEST(LTMGrowth, QTRHoursGrowth) <0 THEN '-15 to 0%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= 0 AND GREATEST(LTMGrowth, QTRHoursGrowth) <.05  THEN '0 to <5%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= .05 AND GREATEST(LTMGrowth, QTRHoursGrowth) <.1 THEN '5 to <10%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= .1 AND GREATEST(LTMGrowth, QTRHoursGrowth) <.15 THEN '10 to <15%' 
			WHEN GREATEST(LTMGrowth, QTRHoursGrowth) >= .15 THEN '>=15%' ELSE 'NA' 
		END																														AS	LTMHoursGrowthBand	,
		TwelveMonthRollingBillableHours																							AS LTMHours , 	
		SeniorsInTerritory																										AS Popn65	
INTO 
	#OwnerPerformanceData
FROM 
	cte_finalSet a
WHERE Status <> 'Inactive' AND Country in ( 'US','CA')
ORDER BY 2,10,11

---------------------------------------------------------------------------------------
--Aggregate US and CA CARELOG Hours by Franchise and Day IDs  + Translate Ids to Keys 
---------------------------------------------------------------------------------------


		INSERT INTO FranchiseAccess.Fact_OwnerPerformanceData	
			(
				DateKey						,	
				FranchiseKey				,	
				CurrentQuarter				,	
				PreviousQuarter				,	
				ScoreCardStatus				,	
				ActiveOwnersName			,	
				City						,	
				State						,	
				GracePeriod					,	
				NoScorecard					,	
				Enterprise					,	
				Status						,	
				RenewalDate					,	
				Category					,	
				PerformanceStandard			,	
				PerformanceYears			,	
				UsesAlternatePerformanceDate,	
				TerritorySize				,	
				TargetHours					,	
				MeasuredHours				,	
				PercentofHourTarget			,	
				TargetRevenue				,	
				MeasuredRevenue				,	
				PercentOfRevenueTarget		,	
				ImpliedRoyaltyRate			,	
				LTMHoursPerSenior			,	
				QuarterlyHoursPerSenior		,	
				BetterOfQtrOrAnnual			,	
				HPSPercentileRange			,	
				LastQuarterGrowth			,	
				LTMHoursGrowth				,	
				MAXHoursGrowth				,	
				LastQuarterHours			,	
				LTMHoursGrowthBand			,	
				LTMHours					,	
				Popn65						,
				CreatedSource				,
				Created						, 
				UpdatedSource				,
				Updated  
			)
		SELECT	
				DateKey						,	
				FranchiseKey				,	
				CurrentQuarter				,	
				PreviousQuarter				,	
				ScoreCardStatus				,	
				ActiveOwnersName			,	
				City						,	
				State						,	
				GracePeriod					,	
				NoScorecard					,	
				Enterprise					,	
				Status						,	
				RenewalDate					,	
				Category					,	
				PerformanceStandard			,	
				PerformanceYears			,	
				UsesAlternatePerformanceDate,	
				TerritorySize				,	
				CAST(TargetHours AS DECIMAL(18,0)) TargetHours,
				CAST(MeasuredHours AS DECIMAL(18,0)) MeasuredHours,					
				PercentofHourTarget	 ,
				CAST(TargetRevenue AS DECIMAL(18,0)) TargetRevenue	,	
				CAST(MeasuredRevenue	AS DECIMAL(18,0)) MeasuredRevenue	,
				PercentOfRevenueTarget		,	
				ImpliedRoyaltyRate			,	
				LTMHoursPerSenior			,	
				QuarterlyHoursPerSenior		,	
				BetterOfQtrOrAnnual			,	
				HPSPercentileRange			,	
				LastQuarterGrowth			,	
				LTMHoursGrowth				,	
				MAXHoursGrowth				,	
				LastQuarterHours			,	
				LTMHoursGrowthBand			,	
				LTMHours					,	
				Popn65						,
				@source					,
				GETUTCDATE()				,
				@source					,
				GETUTCDATE()	
		FROM 
			#OwnerPerformanceData 
		





COMMIT TRANSACTION 	
END TRY

BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
	EXEC Common.Log_SQLError
	RETURN -100	
END CATCH

RETURN 0