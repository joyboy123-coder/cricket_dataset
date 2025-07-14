{% test is_srilanka(model) %}
  SELECT *
  FROM {{ model }}
  WHERE HOME_TEAM != 'Sri Lanka' AND AWAY_TEAM != 'Sri Lanka'
{% endtest %}