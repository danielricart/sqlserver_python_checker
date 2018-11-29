import argparse
import time
from datadog import initialize, api


def submit_custom_metric(metric_name, value, api_key, app_key, timestamp=int(time.time()), tags=None, type=None):
    options = {
        'api_key': api_key,
        'app_key': app_key
    }
    parsed_metric_name = metric_name.replace("-", "_").replace(" ", "_")

    data_point = (timestamp, value)
    initialize(**options)

    now = time.time()
    try:
        # Submit a point with a timestamp (must be ~current)
        response = api.Metric.send(metric=parsed_metric_name, points=data_point, tags=tags, type=None)
        assert response['status'] == "ok"
        print("Published {}: {}".format(parsed_metric_name, value))
    except Exception as e:
        print('Failed to submit custom metric.')
        print(e)


def submit_custom_metrics(metrics, api_key, app_key, timestamp=int(time.time())):
    for metric in metrics:
        if not isinstance(metric, dict):
            raise TypeError("metric is not a dict of {name, value, <optional>timestamp}")
        if "name" not in metric or "value" not in metric:
            raise TypeError("metric is not a dict of {name, value, <optional>timestamp}")

        if 'timestamp' not in metric:
            ts = timestamp
        else:
            ts = metric['timestamp']
        tags = metric["tags"] if "tags" in metric else None
        submit_custom_metric(metric['name'], metric['value'], api_key=api_key, app_key=app_key, timestamp=ts, tags=tags)


def argparser():
    parser = argparse.ArgumentParser(description="Send metrics to datadog")
    parser.add_argument("--metric_name", nargs="?", required=True)
    parser.add_argument("--metric_value", nargs="?", required=True)
    parser.add_argument("--datadog_apikey", nargs="?", required=False, default=None)
    parser.add_argument("--datadog_appkey", nargs="?", required=False, default=None)
    parser.add_argument("--metric_file", nargs="?", required=False, default=None)
    arguments = parser.parse_args()
    return arguments


if __name__ == '__main__':
    args = argparser()
    submit_custom_metric(
        metric_name=args.metric_name,
        value=args.metric_value,
        api_key=args.datadog_apikey,
        app_key=args.datadog_appkey
    )
