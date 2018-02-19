from datadog import initialize, api
import time


def submit_custom_metric(metric_name, value, api_key, app_key, timestamp=int(time.time())):
    options = {
        'api_key': api_key,
        'app_key': app_key
    }

    data_point = (timestamp, value)
    initialize(**options)

    now = time.time()
    try:
        # Submit a point with a timestamp (must be ~current)
        response = api.Metric.send(metric=metric_name, points=data_point)
        assert response['status'] == "ok"
        print("Published {}: {}".format(metric_name, value))
    except Exception as e:
        print('Failed to submit custom metric.')
        print(e)


if __name__ == '__main__':
    submit_custom_metric(
        metric_name="middleware.tests.metrics",
        value=66,
        api_key="API_KEY",
        app_key="APP_KEY"
    )
