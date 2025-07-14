{% test is_no_result_matches(model) %}
  SELECT *
  FROM {{ model }}
  WHERE WINNER != 'No Result'
{% endtest %}

