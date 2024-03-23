with cte_bill_item as
    (SELECT distinct    agency.invoicing_id_prefix ||  inv.invoice_id invoice_num,   
             item.agency_id,
             franchisor.subdomain,    
             franchisor.timezone,            
             item.carelog_id,
             0 patient_id,
             '' PatientFirstName,
             '' PatientLastName,
             0 Caregiver_id,
             '' CaregiverFirstName,
             '' CaregiverLastName,                
             the_date  as transaction_date,
             date('1900-01-01') as clock_out_utc,
              m.meaning as type,
              item.item_type as type_code,
              item.raw_description,
              item.miles Qunatity,
              item.bill_rate Rate,
              0                                                   as bill_amount,
              0                                                   as authorization_split_amount,
              item.bill_amount as final_bill_amount,
              inv.id as transaction_id,
              case when  item.item_type = 3 then 'Miles' else null end as unit                            
              FROM agencies.billing_item AS item
              INNER JOIN agencies.agencies_franchisor as franchisor
                      ON item.franchisor_id = franchisor.franchisor_id
                     AND item.agency_id = franchisor.agency_id
              INNER JOIN agencies.agencies_franchisor as agency
                      ON item.franchisor_id = agency.franchisor_id
                     AND item.agency_id = agency.id
              INNER JOIN meta.metadata m on item.item_type = m.value and   table_name = 'billing_item' and key_name = 'item_type'   
              INNER JOIN 
                    agencies.receivables_invoice_items as ic on ic.item_id = item.id  and ic.agency_id = item.agency_id
              INNER JOIN 
                    agencies.invoice_transaction inv on ic.invoice_id = inv.id  and ic.agency_id = inv.agency_id
             WHERE
                  franchisor.franchisor_id =17
   AND inv.agency_id = '421'
 AND inv.invoice_id = '17110'     
         --     AND inv.transaction_date >= timestamp'2022-01-01 00:00:00' and inv.id = 64954494
),
cte_carelog as
(  SELECT DISTINCT agency.invoicing_id_prefix ||  inv.invoice_id invoice_num,                    carelog.agency_id,
                  agency.subdomain,
                  agency.timezone,
                  carelog.id as carelog_id,
                  carelog.patient_id,
                  profile_patient.first_name PatientFirstName,
                  profile_patient.last_name PatienLasttName,
                  carelog.caregiver_id,
                  profile_CarePro.first_name CareProFirstName,
                  profile_CarePro.last_name CareProLastName,
                  carelog.clock_in as clock_in_utc,
                  carelog.clock_out as clock_out_utc,
                  'Care/Visit' as type,    
                  1 as type_code,
                  '' as raw_description,
                  carelog.new_total_bill_hours,
                  carelog.bill_rate_amount,           
                  COALESCE(carelog.bill_amount,0)                                                   as bill_amount,
                  COALESCE(carelog.authorization_split_amount,0)                                    as authorization_split_amount,
                  COALESCE(carelog.bill_amount,0) - COALESCE(carelog.authorization_split_amount,0)  as final_bill_amount,
                  inv.id as transaction_id,
                  'Hours' as unit         
                  FROM agencies.carelogs_carelog AS carelog
                  INNER JOIN agencies.agencies_franchisor as agency on agency.franchisor_id = carelog.franchisor_id and agency.id = carelog.agency_id
                  INNER JOIN agencies.receivables_invoice_carelogs as ic on ic.carelog_id = carelog.id  and ic.agency_id = carelog.agency_id
                  INNER JOIN agencies.invoice_transaction inv on ic.invoice_id = inv.id  and ic.agency_id = inv.agency_id
                  INNER JOIN  agencies.profile_patient_agencylocation as profile_patient ON profile_patient.patient_id = carelog.patient_id
                  INNER JOIN  agencies.profile_patient_agencylocation as profile_CarePro ON profile_CarePro.caregiver_id = carelog.caregiver_id
                  WHERE carelog.franchisor_id =17
                  AND  split = false
                  AND  carelog.status =30
                  AND  carelog.bill_finalized = true
                  AND (carelog.bill_amount IS NOT NULL
                      OR carelog.pay_amount IS NOT null)
              --   AND   inv.transaction_date >= timestamp'2022-01-01 00:00:00' and  inv.invoice_id = '19215' 
    AND inv.agency_id = '421'
 AND inv.invoice_id = '17110'     
                  )
,
cte_header as
( 

select  
   franchisor.invoicing_id_prefix ||  inv.invoice_id         as invoice_number, 
                                      inv.agency_id         as agency_id,
                                      inv.transaction_date      as invoice_date, 
                                    --  inv.due_date          as due_date,
                                      inv.total                 as total_amount ,
                                      inv.subtotal              as sub_total ,
                                      inv.taxes_total           as tax ,
                                      inv.start_date            as invoice_from_date,
                                      inv.end_date              as invoice_to_date, 
                                   --   inv.paid                  as paid, 
                                  --    patient.id            as patient_id, 
                                   --   patientcontact.id     as ppc_id,
                                  --     null            as org_id,
                                      inv.id as source_id--,
                                     -- profile_patient.name,
                                     -- franchisor.subdomain, inv.created, inv.updated, inv.description, b.insurance_id

 
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
 inv.agency_id not in (select agency_id from agencies.agency_agency_agency_tags where franchisortag_id = 93)  
 AND inv.agency_id = '421'
 AND inv.invoice_id = '17110'     
 
              
)



select h.source_id,h.sub_total,a.SumTotal from cte_header h join 
(
select SUM(Total) SumTotal ,sourceid from (
select SUM(final_bill_amount)*1.00 Total,transaction_id as sourceid  from cte_carelog c 
group by transaction_id
UNION
select SUM(final_bill_amount)*1.00 Total,transaction_id as sourceid from cte_bill_item
group by transaction_id
)
group by sourceid
) a
on a.sourceid = h.source_id
