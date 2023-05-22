import pandas as pd
from . import S3utils as s3


class Topproduct:
    def __init__(self, data):
        self.data = data

    def top_20(self, execution_date):
        self.data = self.data[self.data["date"] == execution_date]
        self.data = (
            self.data.groupby(["advertiser_id", "product_id"])
            .size()
            .reset_index(name="event_count")
        )
        self.data = self.data.sort_values(
            ["advertiser_id", "event_count"], ascending=False
        )
        return self.data.groupby("advertiser_id").head(20)
