import apache_beam as beam
import pyarrow as pa

# Esquema PyArrow para la Capa Silver
schema = pa.schema([
    pa.field("id", pa.string()),
    pa.field("store", pa.string()),
    pa.field(
        "financials",
        pa.struct([
            pa.field("raw_amount", pa.float64()),
            pa.field("currency", pa.string()),
        ]),
    ),
    pa.field(
        "status_history",
        pa.list_(
            pa.struct([
                pa.field("status", pa.string()),
                pa.field("date", pa.timestamp("us", tz="UTC")),
            ])
        ),
    ),
    pa.field(
        "metadata",
        pa.struct([
            pa.field("processed_at", pa.timestamp("us", tz="UTC")),
            pa.field("batch_id", pa.string()),
        ]),
    ),
])

def build_pipeline(sales_path, logs_path):
    with beam.Pipeline() as pipeline:
        #leer datos de ventas
        sales= (pipeline
                | "ReadSales" >> beam.io.ReadFromText(sales_path, skip_header_lines=1)
                | "ParseSales" >> beam.ParDo(ParseSalesData()).with_outputs("rejected", main="clean_sales")
        )
        #leer datos de logs
        logs = (pipeline
                | "ReadLogs" >> beam.io.ReadFromText(logs_path, skip_header_lines=1)
                | "ParseLogs" >> beam.ParDo(ParseLogsData()).with_outputs("rejected", main="clean_sales")
        )
