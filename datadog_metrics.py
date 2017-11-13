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
        if response['status'] == "ok":
            print('Failed to submit custom metric.')
        else:
            print("Published.")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    submit_custom_metric(
        metric_name="middleware.tests.metrics",
        value=66,
        api_key="ca3d835abbbef6afba3ec20d890f65a9",
        app_key="627daeae44f1982d1e64a2e79ac7d1f9c9d33419"
    )
