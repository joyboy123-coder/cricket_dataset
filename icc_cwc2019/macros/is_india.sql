{% test is_india(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'India' AND AWAY_TEAM != 'India'
{% endtest %}