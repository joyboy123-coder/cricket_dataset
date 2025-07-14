{% test is_westindies(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'West Indies' AND AWAY_TEAM != 'West Indies'
{% endtest %}