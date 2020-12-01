await (await fetch('/tree/db1/tb1/', {mode: 'no-cors', method: 'GET'})).json()
// more in tests/test_api.py