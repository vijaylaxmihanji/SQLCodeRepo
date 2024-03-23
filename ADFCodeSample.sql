@concat(
' select  
   franchisor.invoicing_id_prefix ||  inv.invoice_id         as invoice_number, 
                                      inv.agency_id         as agency_id,
                                      inv.transaction_date      as invoice_date, 
                                      inv.due_date          as due_date,
                                      inv.total                 as total_amount ,
                                      inv.subtotal              as sub_total ,
                                      inv.taxes_total           as tax ,
                                      inv.start_date            as invoice_from_date,
                                      inv.end_date              as invoice_to_date, 
                                      inv.paid                  as paid, 
                                      patient.id            as patient_id, 
                                      patientcontact.id     as ppc_id,
                                       null            as org_id,
                                      inv.id as source_id,
                                      profile_patient.name,
                                      franchisor.subdomain, inv.created, inv.updated, inv.description, b.insurance_id

 
 from  
  agencies.invoice_transaction inv
INNER JOIN agencies.agencies_franchisor as franchisor
                      ON inv.franchisor_id = franchisor.franchisor_id
                     AND inv.agency_id = franchisor.agency_id
   
INNER JOIN agencies.Profile_Patient_AgencyLocation as profile_patient
    ON inv.agency_id = profile_patient.agency_id
   AND inv.payer_id = profile_patient.id
INNER JOIN agencies.patient_patient as patient
    ON profile_patient.agency_id  = patient.agency_id
   AND profile_patient.patient_id = patient.id
LEFT OUTER JOIN agencies.profile_patientcontact as patientcontact
    ON patient.agency_id  = patientcontact.agency_id
   AND patient.id = patientcontact.patient_id
   AND patientcontact.payer = true
LEFT OUTER JOIN agencies.profile_profile as profile_patientcontact
    ON patientcontact.agency_id  = profile_patientcontact.agency_id
   AND patientcontact.profile_id = profile_patientcontact.id  
LEFT OUTER JOIN     agencies.billing_clientauthorization b 
    ON b.id = inv.authorization_id
AND b.agency_id  = inv.agency_id
 where  
 inv.payer_id =  inv.client_id 
 and inv.agency_id not in (select agency_id from agencies.agency_agency_agency_tags where franchisortag_id = 93)  
 and (inv.created > CAST(''',variables('InvoiceLastLoadDate'),''' AS DATE) or inv.updated > CAST(''',variables('InvoiceLastLoadDate'),''' AS DATE)) 
 
 UNION
 
 select  
  franchisor.invoicing_id_prefix ||  inv.invoice_id         as invoice_number, 
                                      inv.agency_id         as agency_id,
                                      inv.transaction_date      as invoice_date, 
                                      inv.due_date          as due_date,
                                      inv.total                 as total_amount ,
                                      inv.subtotal              as sub_total ,
                                      inv.taxes_total           as tax ,
                                      inv.start_date            as invoice_from_date,
                                      inv.end_date              as invoice_to_date, 
                                      inv.paid                  as paid, 
                                      profile_Client.patient_id             as patient_id, 
                                      null                      as ppc_id,
                                      profile_org.id    as org_id,
                                      inv.id                    as source_id,
                                      profile_org.name,
                                      franchisor.subdomain, inv.created, inv.updated, inv.description, b.insurance_id
 
 from  
  agencies.invoice_transaction inv
INNER JOIN agencies.agencies_franchisor as franchisor
     ON inv.franchisor_id = franchisor.franchisor_id
     AND inv.agency_id = franchisor.agency_id   
INNER JOIN agencies.Profile_Patient_AgencyLocation as profile_org
    ON inv.agency_id = profile_org.agency_id
    AND inv.payer_id = profile_org.id
    AND profile_org.is_company = true
INNER JOIN agencies.Profile_Patient_AgencyLocation as profile_Client
    ON inv.agency_id = profile_Client.agency_id
    AND inv.Client_id = profile_Client.id    
LEFT OUTER JOIN     agencies.billing_clientauthorization b 
    ON b.id = inv.authorization_id
    AND b.agency_id  = inv.agency_id
 where  
 inv.payer_id <>  inv.client_id 
 and inv.agency_id not in (select agency_id from agencies.agency_agency_agency_tags where franchisortag_id = 93)  
 and (inv.created > CAST(''',variables('InvoiceLastLoadDate'),''' AS DATE) or inv.updated > CAST(''',variables('InvoiceLastLoadDate'),''' AS DATE)) '
)
