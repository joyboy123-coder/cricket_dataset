{% test is_australia(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'Australia' AND AWAY_TEAM != 'Australia'
{% endtest %}