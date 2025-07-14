{% test is_newzealand(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'New Zealand' AND AWAY_TEAM != 'New Zealand'
{% endtest %}