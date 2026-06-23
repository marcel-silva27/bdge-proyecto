import apache_beam as beam
import pyarrow as pa
import csv
from typing import Iterator
import datetime
import argparse

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

class ParseSalesData(beam.DoFn):
    REJECTED_TAG = "rejected"

    def process(self, element: str) -> Iterator:
        '''
        Parsea y valida los datos del archivo sales_data.csv
        '''
        try:
            # Parsear el CSV
            row = next(csv.DictReader([element], fieldnames=[
                "transaction_id", "store_id", "sku", "amount", "currency"
            ]))
            #
        except Exception as exc:
            # Si ocurre un error de parseo, se envía el elemento a la salida etiquetada "rejected"
            yield beam.pvalue.TaggedOutput(
                self.REJECTED_TAG,
                {"raw": element, "reason": f"CSV parse error: {exc}", "source": "sales"},
            )
            return
        
        #validar que los campos requeridos no estén vacíos
        for column in ["transaction_id" , "sku"]:
            if not row.get(column, "").strip(): #debe ser un string no vacío
                yield beam.pvalue.TaggedOutput( 
                    # devuelve el elemento a la salida etiquetada "rejected" con un mensaje de error
                    self.REJECTED_TAG,
                    {"raw": element, "reason": f"Missing required field: {column}", "source": "sales"},
                )
                return
            
        # validar amount numerico y positivo
        try:
            amount = float(row["amount"])
            if amount <= 0: # el monto debe ser positivo
                raise ValueError("Amount debe ser positivo")
            
        except ValueError as exc:
            yield beam.pvalue.TaggedOutput(
                # devuelve el elemento a la salida etiquetada "rejected" con un mensaje de error
                self.REJECTED_TAG,
                {**row, "reason": f"Financial anomaly: {exc}", "source": "sales"},
            )
            return
        
        # los datos son válidos, se construye el diccionario de ventas. 
        sales_dict = {
            "transaction_id": row["transaction_id"].strip(), 
            "store_id": row["store_id"].strip(),
            "sku": row["sku"].strip(),
            "amount": amount,
            "currency": row["currency"].strip(),
        }
        # Retornamos la tupla (Llave, Valor)
        yield (row["transaction_id"].strip(), sales_dict)

class ParseLogsData(beam.DoFn):

    REJECTED_TAG = "rejected"

    def process(self, element: str) -> Iterator:
        '''
        Parsea y valida los datos del archivo logs_data.csv
        '''
        try:
            # Parsear el CSV
            row = next(csv.DictReader([element], fieldnames=[
                "transaction_id", "status_name", "status_date"
            ]))
        except Exception as exc:
            # Si ocurre un error de parseo, se envía el elemento a la salida etiquetada "rejected"
            yield beam.pvalue.TaggedOutput(
                self.REJECTED_TAG,
                {"raw": element, "reason": f"CSV parse error: {exc}", "source": "logs"},
            )
            return
        
        #validar que los campos requeridos no estén vacíos
        for column in ["transaction_id" , "status_name", "status_date"]:
            if not row.get(column, "").strip(): #debe ser un string no vacío
                yield beam.pvalue.TaggedOutput( 
                    # devuelve el elemento a la salida etiquetada "rejected" con un mensaje de error
                    self.REJECTED_TAG,
                    {"raw": element, "reason": f"Missing required field: {column}", "source": "logs"},
                )
                return
            
        # validar que el status_name sea uno de los valores permitidos
        if row["status_name"].strip() not in ["CREATED" , "PENDING" , "COMPLETED" , "REFUNDED"]:
            yield beam.pvalue.TaggedOutput(
                self.REJECTED_TAG,
                {**row, "reason": f"Invalid status: {row['status_name']}", "source": "logs"},
            )
            return
        
        logs_dict = {
            "transaction_id": row["transaction_id"].strip(),
            "status_name": row["status_name"].strip(),
            "status_date": row["status_date"].strip(),
        }

        yield (row["transaction_id"].strip(), logs_dict)

class JoinSalesAndLogs(beam.DoFn):
    """
    Función DoFn para unir los datos de ventas y logs por transaction_id y generar el registro final para la capa Silver desnormalizado.
    Aplica CoGroupByKey para agrupar los datos de ventas y logs por transaction_id, luego construye el registro final con la información de ventas y el historial de estados de los logs.
    """
    def __init__(self, batch_id: str):
        # Inicializa la clase con el batch_id que se utilizará en el registro final.
        self.batch_id = batch_id

    def process(self, element):
        # element = (transaction_id, diccionario de venta o log)
        transaction_id, grouped_data = element
        
        #extraer los datos de ventas y logs
        sales_data = grouped_data.get("sales",[])
        logs_data = grouped_data.get("logs",[])

        # Si no hay venta principal, no podemos generar el registro (Outer Join / Left Join logic)
        if not sales_data:
            return
        
        #1:N
        sale = sales_data[0]  # Tomamos la primera venta, ya que debería haber solo una por transaction_id

        #construye el historial de estados
        status_history = []
        for log in logs_data:
            status_history.append({
                "status": log["status_name"],
                "date":  datetime.fromisoformat(log["status_date"])
            })

        # Construir el registro final para la capa Silver desnormalizado
        yield {
            "id": transaction_id,
            "store": sale["store_id"],
            "financials": {
                "raw_amount": sale["amount"],
                "currency": sale["currency"],
            },
            "status_history": status_history,
            "metadata": {
                "processed_at": datetime.datetime.now(datetime.timezone.utc),
                "batch_id": self.batch_id,
            },
        }


def build_pipeline(sales_path, logs_path, batch_id):
    # Opciones de Pipeline para ejecución local directa
    options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=options) as pipeline:
        #1. Procesar datos de ventas
        sales= (pipeline
                | "ReadSales" >> beam.io.ReadFromText(sales_path, skip_header_lines=1)
                | "ParseSales" >> beam.ParDo(ParseSalesData()).with_outputs("rejected", main="clean_sales")
        )
        #2. Procesar datos de logs
        logs = (pipeline
                | "ReadLogs" >> beam.io.ReadFromText(logs_path, skip_header_lines=1)
                | "ParseLogs" >> beam.ParDo(ParseLogsData()).with_outputs("rejected", main="clean_logs")
        )

        #3. Flujo del pipeline. Unir los datos de ventas y logs por transaction_id
        registros_silver = (
            {"sales": sales.clean_sales, "logs": logs.clean_logs}
            | "CoGroupByKey" >> beam.CoGroupByKey()
            | "JoinSalesAndLogs" >> beam.ParDo(JoinSalesAndLogs(batch_id=batch_id))
            | "WriteToParquet" >> beam.io.parquetio.WriteToParquet(
                file_path_prefix="data/silver/sales_enriched",
                schema=schema,
                file_name_suffix=".parquet"
            )
        )

        # 4. Manejo de registros rechazados 
        (
            (sales.rejected, logs.rejected)
            | "UnirRechazados" >> beam.Flatten()
            | "FormatToString" >> beam.Map(str) # Convierte el diccionario a cadena de texto
            | "GuardarRejected" >> beam.io.WriteToText(
                file_path_prefix="data/audit/rejected_sales", 
                file_name_suffix=".csv",
                shard_name_template="" # Genera un solo archivo de salida
            )
        )


if __name__ == "__main__":
    # Configuración de los argumentos para ejecutar desde consola
    parser = argparse.ArgumentParser(description="Pipeline Consolidacion Omnicanal")
    parser.add_argument("--sales_path", required=True, help="Ruta al archivo sales_data.csv")
    parser.add_argument("--logs_path", required=True, help="Ruta al archivo status_logs.csv")
    parser.add_argument(
        "--batch_id", 
        default=datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S"),
        help="ID único para la ejecución del pipeline"
    )
    
    args, beam_args = parser.parse_known_args()
    
    # Ejecutamos la función constructora
    build_pipeline(args.sales_path, args.logs_path, args.batch_id)
