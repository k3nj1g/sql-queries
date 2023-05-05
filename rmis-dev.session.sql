BEGIN;
ALTER TABLE public.practitionerrole RENAME TO practitionerrole_temp;
ALTER TABLE public.practitionerrole_backup RENAME TO practitionerrole;
ALTER TABLE public.practitionerrole_temp RENAME TO practitionerrole_backup;
END;

BEGIN;
ALTER TABLE public.organization RENAME TO organization_temp;
ALTER TABLE public.organization_backup RENAME TO organization;
ALTER TABLE public.organization_temp RENAME TO organization_backup;
END;

BEGIN;
ALTER TABLE public.practitioner RENAME TO practitioner_temp;
ALTER TABLE public.practitioner_backup RENAME TO practitioner;
ALTER TABLE public.practitioner_temp RENAME TO practitioner_backup;
END;

with pr as (select count(*) pr from practitioner),
pr_b as (select count(*) pr_b from practitioner_backup),
prr as (select count(*) prr from practitionerrole),
prr_b as (select count(*) prr_b from practitionerrole_backup),
org as (select count(*) org from organization),
org_b as (select count(*) org_b from organization_backup)
select * from pr, pr_b, prr, prr_b, org, org_b
