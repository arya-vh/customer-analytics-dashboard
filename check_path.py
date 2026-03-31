import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath('src/analytics_server.py')))
print('base', BASE_DIR)
alt = os.path.abspath(os.path.join(BASE_DIR, '..', '..', 'analytics-dashboard', 'dashboard'))
print('alt', alt)
print('exists', os.path.exists(os.path.join(alt, 'index.html')))
