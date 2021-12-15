from apache_beam.pvalue import PCollection
from dataclasses import dataclass
from datetime import datetime

import apache_beam as beam


@dataclass
class TransactionExtractingDoFn(beam.DoFn):
    """Parse a csv file and extract transaction data."""
    def process(self, element):
        """Returns an iterator over the data of this element.

        Args:
          element: the element being processed

        Returns:
          The processed element.
        """
        timestamp, origin, destination, transaction_amount = element.split(",")
        timestamp: datetime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %Z")  # The id of the user who made the purchase.
        transaction_amount: float = float(transaction_amount)

def csv_data(transaction_data: TransactionExtractingDoFn):
    return f"{transaction_data.date}, {transaction_data.total_amount}"


class ExtractionCompositeTransformation(beam.PTransform):
    def expand(self, p_collection: PCollection):

        return (
                p_collection
                | "Split" >> beam.ParDo(TransactionExtractingDoFn())
                | "Exclude all transaction amounts less than 20" >> beam.Filter( lambda transactions: transactions.transaction_amount > 20)
                | "Exclude all transactions made before the year 2010" >> beam.Filter( lambda transactions: int(transactions.timestamp.strftime("%Y")) >= 2010)
                | "Sum per date" >> beam.GroupBy(date=lambda transactions: transactions.timestamp.strftime("%Y-%m-%d")).aggregate_field("transaction_amount", sum, "total_amount")
                | "Prepare CSV" >> beam.Map(csv_data)
        )
