import argparse
import time
from datadog import initialize, api


def submit_custom_metric(metric_name, value, api_key, app_key, timestamp=int(time.time()), tags=None):
    options = {
        'api_key': api_key,
        'app_key': app_key
    }
    parsed_metric_name = metric_name.replace("-", "_").replace(" ", "_")

    data_point = (timestamp, value)
    initialize(**options)

    try:
        # Submit a point with a timestamp (must be ~current)
        if tags:
            for i in range(0, len(tags)):
                tags[i] = tags[i].replace("-", "_").replace(" ", "_").lower()
            response = api.Metric.send(metric=parsed_metric_name, points=data_point, tags=tags)
        else:
            response = api.Metric.send(metric=parsed_metric_name, points=data_point)

        assert response['status'] == "ok"
        tag_string = "<NO TAGS ADDED>" if tags is None else ";".join(tags)
        print("Published {}: {} : {}".format(parsed_metric_name, value, tag_string))
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
