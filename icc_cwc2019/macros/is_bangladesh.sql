{% test is_bangladesh(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'Bangladesh' AND AWAY_TEAM != 'Bangladesh'
{% endtest %}