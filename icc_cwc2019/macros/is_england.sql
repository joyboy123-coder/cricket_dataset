{% test is_england(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'England' AND AWAY_TEAM != 'England'
{% endtest %}