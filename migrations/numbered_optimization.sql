SELECT *
FROM pg_indexes
WHERE tablename = 'servicerequest';

CREATE INDEX servicerequest_numbered_filters
  ON public.servicerequest (
    ( ( jsonb_extract_path_text( resource, 'authoredOn'))),
    ( ( resource#>>'{performerInfo,requestStatus}') ),
    ( ( reference_identifier(resource, '{managingOrganization}'::TEXT[]))),
    ( ( reference_identifier(resource, '$."performer"?(@."type" == "Organization")'::jsonpath))),
    ( ( resource #>> '{managingOrganization,id}' ) ),    
    ( ( JSONB_PATH_QUERY_FIRST( resource, '$."performer"?(@."resourceType" == "Organization").id') #>>'{}' ) ),
    ( ( JSONB_PATH_QUERY_FIRST( resource, '$."performerInfo"."requestActionHistory"?(@."action" == "AIS-integration-in-progress")."action"' )#>>'{}' ) ),
    ( ( JSONB_PATH_QUERY_FIRST( resource, '$."performerInfo"."requestActionHistory"?(@."action" == "AIS-integration-rejected")."action"' )#>>'{}' ) ),
    ( ( JSONB_PATH_QUERY_FIRST( resource, '$."performerInfo"."requestActionHistory"?(@."action" == "AIS-integration-cancelled")."action"' )#>>'{}' ) ),
    ( ( JSONB_PATH_QUERY_FIRST( resource, '$."performerInfo"."requestActionHistory"?(@."action" == "AIS-integration-failed")."action"' )#>>'{}' ) ))
WHERE ( JSONB_PATH_QUERY_FIRST( resource, '$."category"."coding" ? (@.system == "urn:CodeSystem:servicerequest-category")."code"' )#>>'{}' ) IN ('Referral-IMI','Referral-LMI','Referral-Rehabilitation','Referral-Consultation','Referral-Hospitalization');

ANALYZE servicerequest;

DROP INDEX servicerequest_numbered_filters_logical;

CREATE OR REPLACE FUNCTION reference_identifier(resource jsonb, path text[])
RETURNS text AS $$
DECLARE
  identifier jsonb;  
  result text;
BEGIN
    SELECT INTO identifier jsonb_extract_path(jsonb_extract_path(resource, VARIADIC path), 'identifier');
    SELECT INTO RESULT format('%s_%s', jsonb_extract_path_text(identifier, 'system'), jsonb_extract_path_text(identifier, 'value'));
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION reference_identifier(resource jsonb, path jsonpath)
RETURNS text AS $$
DECLARE
  identifier jsonb;  
  result text;
BEGIN
    SELECT INTO identifier jsonb_extract_path(jsonb_path_query_first(resource, VARIADIC path), 'identifier');
    SELECT INTO RESULT format('%s_%s', jsonb_extract_path_text(identifier, 'system'), jsonb_extract_path_text(identifier, 'value'));
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

