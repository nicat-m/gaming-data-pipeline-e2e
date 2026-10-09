"""Local-dev Superset configuration (loaded via SUPERSET_CONFIG_PATH).

The dashboard SPA issues POST /api/v1/chart/data requests with the session
cookie but (in this setup) without a usable CSRF token, so every chart data
call failed with 400 "The CSRF token is missing" and the dashboard hung on
the loading spinner. This stack is bound to localhost only and is a course
project, not an internet-facing deployment, so CSRF validation is switched
off here. Do NOT copy this into a production config.
"""

WTF_CSRF_ENABLED = False
