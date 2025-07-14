{% test is_pakistan(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'Pakistan' AND AWAY_TEAM != 'Pakistan'
{% endtest %}