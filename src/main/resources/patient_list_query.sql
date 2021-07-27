select p.patient_id
from sourceDatabase.patient p inner join sourceDatabase.encounter e on e.patient_id=p.patient_id
where e.voided=0 and p.voided=0 and e.encounter_type in (5,7) and e.encounter_datetime<=:endDate and e.location_id IN (:locations)

union

select    pg.patient_id
from      sourceDatabase.patient p inner join sourceDatabase.patient_program pg on p.patient_id=pg.patient_id
where   pg.voided=0 and p.voided=0 and program_id in (1,2) and date_enrolled<=:endDate and location_id IN (:locations)

union

Select    p.patient_id
from      sourceDatabase.patient p
  inner join sourceDatabase.encounter e on p.patient_id=e.patient_id
  inner join sourceDatabase.obs o on e.encounter_id=o.encounter_id
where   p.voided=0 and e.voided=0 and o.voided=0 and e.encounter_type=53 and
        o.concept_id=23891 and o.value_datetime is not null and
        o.value_datetime<=:endDate and e.location_id IN (:locations)