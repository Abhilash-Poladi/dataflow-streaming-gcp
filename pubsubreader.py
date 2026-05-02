import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import logging
from datetime import datetime


# --------------------------------------------------
# DoFn: Parse Pub/Sub message
# --------------------------------------------------
def parse_event_time(event_time_str):
    return datetime.fromisoformat(event_time_str.replace("Z", "+00:00")).timestamp()

class AttachWindowTimestamps(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        store_id, total = element
        yield {
            "store_id": store_id,
            "total_sales_amount": total,
            "window_start": window.start.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S"),
            "window_end": window.end.to_utc_datetime().strftime("%Y-%m-%d %H:%M:%S")
        }


class ParsePubSubMessage(beam.DoFn):
    def process(self, element):
        """
        element: bytes from Pub/Sub
        """
        event = json.loads(element.decode("utf-8"))

        if event['event_type']!='SALE':
            return
        else:
            event_time = parse_event_time(event["event_time"])
            yield beam.window.TimestampedValue((event['store']['store_id'],event['transaction']['transaction_amount']), event_time)


# --------------------------------------------------
# Pipeline
# --------------------------------------------------
def run():
    pipeline_options = PipelineOptions(
        streaming=True,
        save_main_session=True
    )

    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSubSubscription" >> beam.io.ReadFromPubSub(
                subscription="projects/project-26fb084d-6d96-4ec6-8a7/subscriptions/pos-events-sub"
            )
            | "ParseJSON" >> beam.ParDo(ParsePubSubMessage())
            # | "LogEvents" >> beam.Map(
            #     lambda event: logging.info(f"RECEIVED EVENT: {event}")
            # )
            | "Window1Min" >> beam.WindowInto(
                beam.window.FixedWindows(60)
            )
            | "SumSalesPerStore" >> beam.CombinePerKey(sum)
            | "AttachWindowTimes" >> beam.ParDo(AttachWindowTimestamps())
            # | "FormatOutput" >> beam.Map(
            #     lambda x: {
            #         "store_id": x[0],
            #         "total_sales_amount": x[1],
            #         "window_start_ts": x[2],
            #         "window_end_ts": x[3]
            #     }
            # )
            # | "LogResults" >> beam.Map(
            #     lambda x: logging.info(f"REAL-TIME METRIC: {x}")
            # )   
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
    table="project-26fb084d-6d96-4ec6-8a7:my_dataset.realtime_sales",
    schema={
        "fields": [
            {"name": "store_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "total_sales_amount", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "window_start", "type": "STRING", "mode": "REQUIRED"},
            {"name": "window_end", "type": "STRING", "mode": "REQUIRED"},
        ]
    },
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
