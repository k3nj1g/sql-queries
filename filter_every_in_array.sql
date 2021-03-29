select * from organization
where jsonb_path_exists(resource, '$ ? (!(@.identifier[*].system == "urn:source:1c:Organization"))')
