CREATE INDEX IF NOT EXISTS covidinfo_history_txid__btree ON public.covidinfo_history USING btree (txid);
CREATE INDEX IF NOT EXISTS procedure_txid__btree ON public.procedure USING btree (txid);
CREATE INDEX IF NOT EXISTS schedulerule_txid__btree ON public.schedulerule USING btree (txid);
CREATE INDEX IF NOT EXISTS patientflag_txid__btree ON public.patientflag USING btree (txid);
CREATE INDEX IF NOT EXISTS patientbinding_txid__btree ON public.patientbinding USING btree (txid);
CREATE INDEX IF NOT EXISTS questionnaireresponse_txid__btree ON public.questionnaireresponse USING btree (txid);
CREATE INDEX IF NOT EXISTS vaccinationinfo_history_txid__btree ON public.vaccinationinfo_history USING btree (txid);
CREATE INDEX IF NOT EXISTS covidinfo_txid__btree ON public.covidinfo USING btree (txid);
CREATE INDEX IF NOT EXISTS medicalproduct_txid__btree ON public.medicalproduct USING btree (txid);
CREATE INDEX IF NOT EXISTS flag_history_txid__btree ON public.flag_history USING btree (txid);
CREATE INDEX IF NOT EXISTS signatureinfo_txid__btree ON public.signatureinfo USING btree (txid);
CREATE INDEX IF NOT EXISTS medrecordzno_history_txid__btree ON public.medrecordzno_history USING btree (txid);
CREATE INDEX IF NOT EXISTS clinicalimpression_txid__btree ON public.clinicalimpression USING btree (txid);
CREATE INDEX IF NOT EXISTS appointment_txid__btree ON public.appointment USING btree (txid);
CREATE INDEX IF NOT EXISTS specimen_txid__btree ON public.specimen USING btree (txid);
CREATE INDEX IF NOT EXISTS diagnosticreport_txid__btree ON public.diagnosticreport USING btree (txid);
CREATE INDEX IF NOT EXISTS episodeofcare_txid__btree ON public.episodeofcare USING btree (txid);
CREATE INDEX IF NOT EXISTS servicerequest_txid__btree ON public.servicerequest USING btree (txid);
CREATE INDEX IF NOT EXISTS careplan_history_txid__btree ON public.careplan_history USING btree (txid);
CREATE INDEX IF NOT EXISTS specimen_history_txid__btree ON public.specimen_history USING btree (txid);
CREATE INDEX IF NOT EXISTS flag_txid__btree ON public.flag USING btree (txid);
CREATE INDEX IF NOT EXISTS careplan_txid__btree ON public.careplan USING btree (txid);
CREATE INDEX IF NOT EXISTS relatedperson_txid__btree ON public.relatedperson USING btree (txid);
CREATE INDEX IF NOT EXISTS schedulerule_history_txid__btree ON public.schedulerule_history USING btree (txid);
CREATE INDEX IF NOT EXISTS concept_history_txid__btree ON public.concept_history USING btree (txid);
CREATE INDEX IF NOT EXISTS documentreference_history_txid__btree ON public.documentreference_history USING btree (txid);
CREATE INDEX IF NOT EXISTS medicationrequest_history_txid__btree ON public.medicationrequest_history USING btree (txid);
CREATE INDEX IF NOT EXISTS diagnosticreport_history_txid__btree ON public.diagnosticreport_history USING btree (txid);
CREATE INDEX IF NOT EXISTS analyzererror_history_txid__btree ON public.analyzererror_history USING btree (txid);
CREATE INDEX IF NOT EXISTS communicationrequest_history_txid__btree ON public.communicationrequest_history USING btree (txid);
CREATE INDEX IF NOT EXISTS medicationrequest_txid__btree ON public.medicationrequest USING btree (txid);
CREATE INDEX IF NOT EXISTS episodeofcare_history_txid__btree ON public.episodeofcare_history USING btree (txid);
CREATE INDEX IF NOT EXISTS condition_history_txid__btree ON public.condition_history USING btree (txid);
CREATE INDEX IF NOT EXISTS encounter_history_txid__btree ON public.encounter_history USING btree (txid);
CREATE INDEX IF NOT EXISTS servicerequest_history_txid__btree ON public.servicerequest_history USING btree (txid);
CREATE INDEX IF NOT EXISTS encounter_txid__btree ON public.encounter USING btree (txid);
CREATE INDEX IF NOT EXISTS documentreference_txid__btree ON public.documentreference USING btree (txid);
CREATE INDEX IF NOT EXISTS practitionerrole_txid__btree ON public.practitionerrole USING btree (txid);
CREATE INDEX IF NOT EXISTS communicationrequest_txid__btree ON public.communicationrequest USING btree (txid);
CREATE INDEX IF NOT EXISTS task_txid__btree ON public.task USING btree (txid);
CREATE INDEX IF NOT EXISTS patient_txid__btree ON public.patient USING btree (txid);
CREATE INDEX IF NOT EXISTS allergyintolerance_txid__btree ON public.allergyintolerance USING btree (txid);
CREATE INDEX IF NOT EXISTS concept_txid__btree ON public.concept USING btree (txid);
CREATE INDEX IF NOT EXISTS practitionerrole_history_txid__btree ON public.practitionerrole_history USING btree (txid);
CREATE INDEX IF NOT EXISTS observation_history_txid__btree ON public.observation_history USING btree (txid);
CREATE INDEX IF NOT EXISTS relatedperson_history_txid__btree ON public.relatedperson_history USING btree (txid);
CREATE INDEX IF NOT EXISTS task_history_txid__btree ON public.task_history USING btree (txid);
CREATE INDEX IF NOT EXISTS vaccinationinfo_txid__btree ON public.vaccinationinfo USING btree (txid);
CREATE INDEX IF NOT EXISTS patient_history_txid__btree ON public.patient_history USING btree (txid);
CREATE INDEX IF NOT EXISTS condition_txid__btree ON public.condition USING btree (txid);
CREATE INDEX IF NOT EXISTS riskassessment_history_txid__btree ON public.riskassessment_history USING btree (txid);
CREATE INDEX IF NOT EXISTS appointment_history_txid__btree ON public.appointment_history USING btree (txid);
CREATE INDEX IF NOT EXISTS questionnaireresponse_history_txid__btree ON public.questionnaireresponse_history USING btree (txid);
CREATE INDEX IF NOT EXISTS measurereport_txid__btree ON public.measurereport USING btree (txid);
CREATE INDEX IF NOT EXISTS observation_txid__btree ON public.observation USING btree (txid);
CREATE INDEX IF NOT EXISTS aidboxjobstatus_history_txid__btree ON public.aidboxjobstatus_history USING btree (txid);
CREATE INDEX IF NOT EXISTS personbinding_txid__btree ON public.personbinding USING btree (txid);
CREATE INDEX IF NOT EXISTS practitioner_history_txid__btree ON public.practitioner_history USING btree (txid);
CREATE INDEX IF NOT EXISTS medrecordzno_txid__btree ON public.medrecordzno USING btree (txid);