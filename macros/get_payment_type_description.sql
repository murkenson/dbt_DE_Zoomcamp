{# 
This macro returns the description of the payment_type #}
{% macro get_payment_type_description(payment_type) %}

    case
        {{ payment_type }}

        when 1
        then 'Credit card'

        when 2
        then 'Cash'

        when 3
        then 'No charge'

        when 4
        then 'Dispute'

        when 5
        then 'Unknown'

        when 6
        then 'Voided trip'
    end
{% endmacro %}


{% macro create_table_tmp(in_schema_nm, in_table_nm) %}

        create table if
        not exists {{ in_schema_nm }}.{{ in_table_nm }}(
            album_id int64,
            id int64,
            title string,
            url string,
            thumbnail_url string,
            _dlt_load_id string not null,
            _dlt_id string not null
        )
    ;

{% endmacro %}
