CREATE VIEW [Datamart.FranchiseWebPortal].FranchiseKeyPerformanceIndicators
AS 
WITH 
cte_NetCaregivers AS 
	(			
				SELECT 
					F.LegalOwners,F.FranchiseKey,	F.Number
					,COUNT(DISTINCT CaregiverId) FourWeeksTotalCaregivers  
				FROM 
						[Common].Fact_DailyCareActivityByFranchiseByCaregiver mmf 
					JOIN 
						Datamart.Dim_Franchise f ON f.FranchiseKey = mmf.FranchiseKey
					JOIN 
						Datamart.Dim_Date d ON D.DateKey = mmf.DateKey									
				WHERE 
						d.Year >=2023
						AND D.Date >CAST(DATEADD(dd, -(DATEPART(dw, (DATEADD(WK, -4, getdate())))-1), (DATEADD(WK, -4, getdate()))) as date)  -- last day t-5 weeks
						AND D.Date <= CAST(DATEADD(dd, -(DATEPART(dw, getdate())-1), getdate()) as date) -- Data should be as of last completed week (week ending Sunday)
						AND f.IsInternational =0 AND Status = 'Active' 
						AND OperatingSystem <> 'CarePlatform'
				GROUP BY 
						F.Number, F.LegalOwners,F.FranchiseKey
		),  
CTE_CaregiverUtilizationActualHours as 
			(
	SELECT 
					F.LegalOwners,F.FranchiseKey,	F.Number,CaregiverId,mmf.CreatedSource
					,SUM(TotalServiceHours) LastWeekTotalHours  
				FROM 
						[Common].Fact_DailyCareActivityByFranchiseByCaregiverByPlatform mmf 
					JOIN 
						Datamart.Dim_Franchise f ON f.FranchiseKey = mmf.FranchiseKey
					JOIN 
						Datamart.Dim_Date d ON D.DateKey = mmf.DateKey									
				WHERE 
						d.Year >=2023
						AND D.Date >CAST(DATEADD(dd, -(DATEPART(dw, (DATEADD(WK, -1, getdate())))-1), (DATEADD(WK, -1, getdate()))) as date)  -- last day t-1 weeks
						AND D.Date <= CAST(DATEADD(dd, -(DATEPART(dw, getdate())-1), getdate()) as date) -- Data should be as of last completed week (week ending Sunday)
						AND f.IsInternational =0 AND Status = 'Active'  
						AND OperatingSystem <> 'CarePlatform' and mmf.Platform = 'ClearCare'
				GROUP BY 
						F.Number, F.LegalOwners,F.FranchiseKey, CaregiverId,mmf.CreatedSource
			),
CTE_CaregiverDesiredHours AS 
			(
					SELECT 
						Id,DesiredHours, 'EntityData-US' CreatedSource 			
					FROM 
						[Ingestion.EntityData.US].Caregiver 
				UNION
					SELECT 
						Id,DesiredHours, 'EntityData-CA'	CreatedSource
					FROM 
						[Ingestion.EntityData.CA].Caregiver   			
			),
CTE_CaregiverUtilization AS 
			(
					SELECT 
						ah.FranchiseKey, ah.Number, ah.LegalOwners, SUM(dh.DesiredHours) DesiredHours, SUM(ah.LastWeekTotalHours) LastWeekTotalHours
						, SUM(ah.LastWeekTotalHours)*100.00 /SUM(dh.DesiredHours) utilization
					FROM 
						CTE_CaregiverDesiredHours dh JOIN CTE_CaregiverUtilizationActualHours ah 
							ON ah.CaregiverId = dh.id AND ah.CreatedSource = dh.CreatedSource
					GROUP BY ah.FranchiseKey, ah.Number, ah.LegalOwners
			),
CTE_CaregiverCountLessThan50PercentUtilization AS 
			(
					SELECT 
						ah.FranchiseKey, ah.Number, ah.LegalOwners,count(ah.CaregiverId) CaregiverCountLessThanFiftyPercentUtil
					FROM 
						CTE_CaregiverDesiredHours dh JOIN CTE_CaregiverUtilizationActualHours ah 
							on ah.CaregiverId = dh.id AND ah.CreatedSource = dh.CreatedSource
					WHERE CASE WHEN dh.DesiredHours = 0 THEN 100 ELSE  (ah.LastWeekTotalHours /dh.DesiredHours) END <0.5 
					GROUP BY ah.FranchiseKey, ah.Number, ah.LegalOwners
			),
cte_FourWeeksNetHours AS  
			(
					SELECT 
							F.LegalOwners,F.FranchiseKey,	F.Number
							,SUM(mmf.TotalServiceHours) FourWeeksTotalServiceHours  
						FROM 
								[Common].Fact_DailyCareActivityByFranchise mmf 
							JOIN 
								Datamart.Dim_Franchise f ON f.FranchiseKey = mmf.FranchiseKey
							JOIN 
								Datamart.Dim_Date d ON D.DateKey = mmf.DateKey									
						WHERE 
								d.Year >=2023
								 AND D.Date >CAST(DATEADD(dd, -(DATEPART(dw, (DATEADD(WK, -4, getdate())))-1), (DATEADD(WK, -4, getdate()))) as date)  -- last day t-5 weeks
					AND D.Date <= CAST(DATEADD(dd, -(DATEPART(dw, getdate())-1), getdate()) as date)
								AND f.IsInternational =0 AND Status = 'Active'  
								AND OperatingSystem <> 'CarePlatform'

						GROUP BY 
								F.Number, F.LegalOwners,F.FranchiseKey,F.OperatingSystem, F.OperatingSystemStartDate
			),	
cte_BillableHours AS 
			(
				SELECT 
					F.LegalOwners,F.FranchiseKey,	F.Number,
					SUM(mmf.TotalBillableHours) LastOneWeekTotalBillableHours 
				FROM 
						[Common].Fact_DailyCareActivityByFranchise mmf 
					JOIN 
						Datamart.Dim_Franchise f ON f.FranchiseKey = mmf.FranchiseKey
					JOIN 
						Datamart.Dim_Date d ON D.DateKey = mmf.DateKey									
				WHERE 
						d.Year >=2023
						 AND D.Date >CAST(DATEADD(dd, -(DATEPART(dw, (DATEADD(WK, -1, getdate())))-1), (DATEADD(WK, -1, getdate()))) as date)  -- last day t-5 weeks
						 AND D.Date <= CAST(DATEADD(dd, -(DATEPART(dw, getdate())-1), getdate()) as date) -- Data should be as of last completed week (week ending Sunday)
						AND f.IsInternational =0 AND Status = 'Active'  
						AND OperatingSystem <> 'CarePlatform'
				GROUP BY 
						F.Number, F.LegalOwners,F.FranchiseKey
			),
cte_ServedCount AS (
				SELECT	f.FranchiseKey,
						ServedCount LastOneWeekServedCount
				FROM 
					Common.Fact_WeeklyClientMetricsByFranchise mmf
						JOIN 
							Datamart.Dim_Franchise f ON f.FranchiseKey = mmf.FranchiseKey
						JOIN 
							Datamart.Dim_Date d ON D.DateKey = mmf.DateKey		
				WHERE
							d.Year >=2023
						 AND D.Date >CAST(DATEADD(dd, -(DATEPART(dw, (DATEADD(WK, -1, getdate())))-1), (DATEADD(WK, -1, getdate()))) as date)  -- last day t-5 weeks
						 AND D.Date <= CAST(DATEADD(dd, -(DATEPART(dw, getdate())-1), getdate()) as date)
						AND f.IsInternational =0 AND Status = 'Active' 
						AND OperatingSystem <> 'CarePlatform'
					),
cte_AvgHoursPerClient AS 
				(
					SELECT 
						h.*,sc.LastOneWeekServedCount LastWeekClientCount,
						CASE WHEN sc.LastOneWeekServedCount = 0 THEN 0 ELSE h.LastOneWeekTotalBillableHours/sc.LastOneWeekServedCount END AvgHoursPerClient 
					FROM cte_BillableHours h
						JOIN cte_servedcount sc ON h.franchisekey = sc.franchisekey 
				)

SELECT	f.FranchiseId,
		f.Number,f.LegalOwners,f.NetworkPerformancePartner,
		t1.FourWeeksTotalCaregivers							AS NetCareProsLastFourWeeks, 
		t2.Utilization										AS CareProUtilizationPercentLastWeek,
		t3.CaregiverCountLessThanFiftyPercentUtil			AS CaregiverCountLessThanfiftyPercentUtilization,
		t4.LastOneWeekServedCount							AS NetClientsLastWeek,
		t5.FourWeeksTotalServiceHours						AS NetHoursLastFourWeeks,
		t6.AvgHoursPerClient								AS WeeklyHoursPerClientLastWeek,
		t7.LastOneWeekTotalBillableHours					AS BillableHoursLastWeek
FROM 
	Common.Dim_Franchise f 
	LEFT JOIN cte_NetCaregivers	t1 on f.FranchiseKey = t1.FranchiseKey
	LEFT JOIN CTE_CaregiverUtilization t2 on f.FranchiseKey = t2.FranchiseKey
	LEFT JOIN CTE_CaregiverCountLessThan50PercentUtilization t3 on f.FranchiseKey = t3.FranchiseKey
	LEFT JOIN cte_ServedCount t4 on f.FranchiseKey = t4.FranchiseKey
	LEFT JOIN cte_FourWeeksNetHours t5 on f.FranchiseKey = t5.FranchiseKey
	LEFT JOIN cte_AvgHoursPerClient t6 ON f.FranchiseKey = t6.FranchiseKey
	LEFT JOIN cte_BillableHours t7 ON f.FranchiseKey = t7.FranchiseKey
WHERE 
	f.IsInternational =0 
	AND Status = 'Active' 
	AND OperatingSystem <> 'CarePlatform'
	AND Deleted <> 1
GO


