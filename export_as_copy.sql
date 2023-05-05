COPY (
    SELECT patient_fio(resource) AS "ФИО",
        resource#>>'{birthDate}' AS "Дата рождения",
        CASE
            WHEN resource#>>'{gender}' = 'male' THEN 'Мужской'
            WHEN resource#>>'{gender}' = 'female' THEN 'Женский'
            ELSE 'Неизвестный'
        END AS "Пол",
        jsonb_path_query_first(
            resource,
            '$.identifier ? (@.system == "urn:source:tfoms:Patient")'
        )#>>'{value}' AS tfoms_id
    FROM patient
    WHERE resource @@ 'identifier.#.system = "urn:source:tfoms:Patient"'::jsquery
    limit 100
) TO '/temp/patient_data.csv' WITH CSV HEADER;
