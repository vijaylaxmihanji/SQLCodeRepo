CREATE VIEW  [Datamart.FranchiseAccess].[FranchiseOwnerMapping]

AS

WITH 
cte_main AS (	
		----------------------------------------Franchise Owners (US)---------------------------
								SELECT 
							DISTINCT F.Id AS FranchiseId, F.Number, F.status,  O.FirstName , o.LastName , OT.Value as Email, [use]		
						FROM  
											[Ingestion.EntityData.US].Franchise F
							LEFT JOIN [Ingestion.EntityData.US].Ownership OWN on OWN.FranchiseId =F.Id and OWN.Active = 1
							LEFT JOIN [Ingestion.EntityData.US].Owner O on O.Id = OWN.OwnerId and O.Active = 1
							Left Join [Ingestion.EntityData.US].OwnerTelecom OT on O.Id = OT.OwnerId and OT.System = 'Email' and OT.[USE] ='Work'
							WHERE OT.Value IS NOT NULL 	AND F.Region <> 'Canada'  and F.CreatedSource <> 'TestData'

			 UNION
	----------------------------------------LegalEntity Owners (US)---------------------------

								SELECT 
							DISTINCT F.Id AS FranchiseId, F.Number, F.status,  O.FirstName , o.LastName , OT.Value as Email, [use]		
						FROM  
							[Ingestion.EntityData.US].Franchise F 
							LEFT JOIN [Ingestion.EntityData.US].FranchiseAgreement LF on LF.FranchiseId = F.Id and LF.Active = 1
							LEFT JOIN [Ingestion.EntityData.US].LegalEntity L on L.Id = LF.LegalEntityId and L.Active = 1
							LEFT JOIN [Ingestion.EntityData.US].LEOwnership LO ON LO.LegalEntityId = L.Id and LO.Active = 1
							LEFT JOIN [Ingestion.EntityData.US].Owner O on O.Id = LO.OwnerId and LO.Active = 1
							Left Join [Ingestion.EntityData.US].OwnerTelecom OT on O.Id = OT.OwnerId and OT.System = 'Email' and OT.[USE] ='Work'
							WHERE OT.Value IS NOT NULL 	AND F.Region <> 'Canada'  and F.CreatedSource <> 'TestData'

UNION
		----------------------------------------Franchise Owners (CA)---------------------------

						SELECT 
							DISTINCT F.Id AS FranchiseId, F.Number, F.status,  O.FirstName , o.LastName , OT.Value as Email, [use]		
						FROM  
										[Ingestion.EntityData.CA].Franchise F
						LEFT JOIN [Ingestion.EntityData.CA].Ownership OWN on OWN.FranchiseId =F.Id and OWN.Active = 1
						LEFT JOIN [Ingestion.EntityData.CA].Owner O on O.Id = OWN.OwnerId and O.Active = 1
						Left Join [Ingestion.EntityData.CA].OwnerTelecom OT on O.Id = OT.OwnerId and OT.System = 'Email' and OT.[USE] ='Work'
						WHERE OT.Value IS NOT NULL 	 and F.CreatedSource <> 'TestData'

			 UNION
			 	----------------------------------------LegalEntity Owners (CA)---------------------------

							SELECT 
					DISTINCT F.Id AS FranchiseId, F.Number, F.status,  O.FirstName , o.LastName , OT.Value as Email, [use]		
				FROM  
					[Ingestion.EntityData.CA].Franchise F 
					LEFT JOIN [Ingestion.EntityData.CA].FranchiseAgreement LF on LF.FranchiseId = F.Id and LF.Active = 1
					LEFT JOIN [Ingestion.EntityData.CA].LegalEntity L on L.Id = LF.LegalEntityId and L.Active = 1
					LEFT JOIN [Ingestion.EntityData.CA].LegalEntityOwnership LO ON LO.LegalEntityId = L.Id and LO.Active = 1
					LEFT JOIN [Ingestion.EntityData.CA].Owner O on O.Id = LO.OwnerId and LO.Active = 1
					Left Join [Ingestion.EntityData.CA].OwnerTelecom OT on O.Id = OT.OwnerId and OT.System = 'Email' and OT.[USE] ='Work'
					WHERE OT.Value IS NOT NULL  and F.CreatedSource <> 'TestData'

), 
cte_Owners AS (
	SELECT DISTINCT 		
		FirstName,
		LastName, 
		Email as ActiveDirectoryIdentity,
		'Owner' as UserType,
		'Email' AS "System", 
		GETDATE() as Updated	
	from 
		cte_main  -- Distinct OwnerList
),
cte_OwnersId AS (
	SELECT ROW_NUMBER() OVER( ORDER BY FirstName,LastName )  AS ID, 
		FirstName,
		LastName, 
		ActiveDirectoryIdentity,
		UserType,
		"System", 
		Updated	
	FROM 
		cte_Owners
)
,
cte_FranchiseOwners AS (				
	SELECT 
		DISTINCT 
		t.FranchiseId,
		GETDATE() as Updated,
		o.ID as OwnerId 
	FROM 
		cte_main t join cte_OwnersId o on o.ActiveDirectoryIdentity = t.Email 
)

SELECT ROW_NUMBER() OVER( ORDER BY FranchiseId)  AS ID, 
		FranchiseId,
		Updated, 
		OwnerId
FROM 
	cte_FranchiseOwners
			
GO
		


	

