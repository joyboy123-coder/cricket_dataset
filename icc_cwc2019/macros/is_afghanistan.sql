{% test is_afghanistan(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'Afghanistan' AND AWAY_TEAM != 'Afghanistan'
{% endtest %}

