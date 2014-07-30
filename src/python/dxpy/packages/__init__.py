import sys
import requests
sys.modules[__name__ + '.requests'] = sys.modules['requests']
