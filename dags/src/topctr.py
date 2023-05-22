import pandas as pd
from . import S3utils as s3


class Topctr:
    def __init__(self, data):
        self.data = data

    def top_20(self, execution_date):
        # print("#" * 100)
        # print("\n" * 4)
        # print(execution_date.split(" ")[0].split("T")[0])
        # print(self.data["date"])
        # print("\n" * 4)
        # print("#" * 100)
        self.data = self.data[self.data["date"] == execution_date]
        self.data["clicked"] = self.data["type"].apply(lambda x: x == "click")

        CTR_index = pd.DataFrame(
            self.data.groupby(["advertiser_id", "product_id"])["clicked"].mean()
        )

        CTR_index = CTR_index.reset_index().sort_values(
            by=["advertiser_id", "clicked"], ascending=False
        )

        CTR_index = CTR_index.rename(columns={"clicked": "CTR"})

        return CTR_index.groupby(["advertiser_id"]).head(20)
