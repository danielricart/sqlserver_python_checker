__author__ = 'Stackdriver'
# Note: this code sample works for Stackdriver customers only. Google Cloud
# Monitoring beta users should use the Cloud Monitoring custom metrics API
# here: http://goo.gl/W7ORHb
import requests
import json
import time


def submit_custom_metric(metric_name, value, api_key, timestamp=int(time.time())):
    data_point = {
        'name': metric_name,
        'value': value,
        'collected_at': timestamp,
        # Note: Include an instance value if your metric is tied to an instance.
        # 'instance': 'i-123a567b',
        }
    gateway_msg = {
        'timestamp': int(time.time()),
        'proto_version': 1,
        'data': data_point,
        }
    headers = {
        'content-type': 'application/json',
        'x-stackdriver-apikey': api_key
        }
    resp = requests.post(
        'https://custom-gateway.stackdriver.com/v1/custom',
        data=json.dumps(gateway_msg),
        headers=headers)

    if not resp.ok:
        print(resp.content)
        assert resp.ok, 'Failed to submit custom metric.'
    else:
        print("Published.")
if __name__ == '__main__':
    submit_custom_metric()