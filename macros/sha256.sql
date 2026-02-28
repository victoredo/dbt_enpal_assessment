{% macro sha256(field, hashing_salt=var('hashing_salt')) -%}
/*
Description:
------------
Generates a deterministic SHA-256 hash for a given field with an added salt.
Compatible with PostgreSQL using pgcrypto functions.

Behavior:
---------
- LOWER() + TRIM() normalize text
- CAST to TEXT for Postgres
- SALT is injected as a SQL string
- Uses digest() + encode() for SHA-256 hashing
*/

    CASE
        WHEN {{ field }} IS NOT NULL THEN
            encode(
                digest(
                    LOWER(TRIM(CAST({{ field }} AS TEXT))) || '{{ hashing_salt }}',
                    'sha256'::TEXT
                ),
                'hex'
            )
    END

{%- endmacro %}
