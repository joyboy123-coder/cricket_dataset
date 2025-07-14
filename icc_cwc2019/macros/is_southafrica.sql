{% test is_southafrica(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'South Africa' AND AWAY_TEAM != 'South Africa'
{% endtest %}