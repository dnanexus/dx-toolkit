SELECT `patient_1`.`patient_id` AS `patient_id` FROM `database_gyk2yg00vgppzj7ygy3vjxb9__create_cohort_pheno_database`.`patient` AS `patient_1` WHERE `patient_1`.`patient_id` IN ('patient_1', 'patient_2') AND `patient_1`.`patient_id` IN (SELECT `patient_id` FROM `database_gyk2yg00vgppzj7ygy3vjxb9__create_cohort_pheno_database`.`patient` WHERE `patient_id` in ('patient_1', 'patient_2', 'patient_3'))