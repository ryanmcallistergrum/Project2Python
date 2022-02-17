import datetime
import logging
from logging import info

from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import DecimalType


class QueryLoader:
    def __init__(self):
        logging.getLogger().setLevel(logging.INFO)
        self._covid_data = self.initialize_data()
        self._max_deaths = self._covid_data.select(f.col("Country/Region"), f.col("Deaths").cast("Int"))
        self._pop_data = self.get_spark_session().read.option("header", "true") \
            .csv("data/population_by_country_2020.csv")
        self._death_join_pop = self._max_deaths.join(self._pop_data, self._covid_data["Country/Region"].
                                                     __eq__(self._pop_data["Country"]), "inner")
        self._country_by_months = self.country_by_month()
        self._monthly_data = self.get_monthly()
        self._continent = self.get_spark_session().read.option("header", "true").csv("data/continents.csv")
        self._covid_continents = self._covid_data.join(self._continent, "Country/Region")

    def load_query(self, question: int) -> DataFrame:
        start_time = datetime.datetime.now()
        result = None
        if question == 1:
            result = self.question01()
        elif question == 2:
            result = self.question02()
        elif question == 3:
            result = self.question03()
        elif question == 4:
            result = self.question04()
        elif question == 5:
            result = self.question05()
        elif question == 6:
            result = self.question06()
        elif question == 7:
            result = self.question07()
        elif question == 8:
            result = self.question08()
        elif question == 9:
            result = self.question09()
        elif question == 10:
            result = self.question10()
        elif question == 11:
            result = self.question11()
        info("Question " + str(question) + " took " + str(datetime.datetime.now() - start_time) + " seconds.")
        return result

    @staticmethod
    def get_spark_session() -> SparkSession:
        return SparkSession.builder.appName("Covid Analyze App").master("local[*]").enableHiveSupport().getOrCreate()

    def question01(self) -> DataFrame:
        return self._monthly_data.select("Date", "Mortality Rate", "Spread Rate", "Difference").orderBy("Date")

    def question02(self) -> DataFrame:
        return self._monthly_data

    def question03(self) -> DataFrame:
        return self._monthly_data.select("Date", "Confirmed", "Deaths", "Recovered").orderBy("Date")

    def question04(self) -> DataFrame:
        return self._monthly_data.select("Date", "`Mortality Rate`").withColumn("Mortality Rate",
                                                                                f.round(f.col("Mortality Rate") *
                                                                                        100, 2)).orderBy("Date")

    def question05(self) -> DataFrame:
        return self._country_by_months.withColumn("Deaths", f.col("Deaths").cast("Int")).groupBy("Country/Region") \
            .sum("Deaths").orderBy(f.col("sum(Deaths)").desc()).limit(10)

    def question06(self) -> DataFrame:
        return self._country_by_months.withColumn("Deaths", f.col("Deaths").cast("Int")).groupBy("Country/Region") \
            .sum("Deaths").orderBy(f.col("sum(Deaths)").asc()).filter(f.col("sum(Deaths)").isNotNull())

    def question07(self) -> DataFrame:
        df = self._covid_data.select(
            f.col("Date"),
            f.col("Country/Region"),
            f.col("Confirmed").cast("long")
        ).withColumn("Date", f.to_date(f.col("Date"), "MM/dd/yyyy"))
        return df.groupBy(f.col("Date")).sum("Confirmed").orderBy("Date") \
            .withColumnRenamed("sum(Confirmed)", "Confirmed").withColumn("Difference",
                                                                         f.coalesce(
                                                                             f.col("Confirmed") - f.lag("Confirmed",
                                                                                                           1).over(
                                                                                 Window.partitionBy().orderBy("Date")),
                                                                             f.col("Confirmed"))).na.fill(
            0).withColumn("DayOfWeek",
                          f.to_date(f.col("Date"), "MM/dd/yyyy")) \
            .withColumn("DayOfWeek", f.date_format(f.col("DayOfWeek"), "E")).filter(f.col("Date").isNotNull())

    def question08(self) -> DataFrame:
        death_continents = self._death_join_pop.join(self._continent,
                                                     self._death_join_pop["Country"].__eq__(
                                                         self._continent["Country/Region"])).drop("Country/Region")
        sum_deaths = death_continents.groupBy("Deaths").sum("Deaths")
        death_continents = death_continents.join(sum_deaths, "Deaths", "inner")
        modified = death_continents.withColumn("sum(Deaths)", f.log("sum(Deaths)")).withColumn("Population",
                                                                                               f.log("Population"))
        print("Correlation Value: ", modified.stat.corr("Population", "sum(Deaths)"))
        return modified.sort(f.col("Population").desc_nulls_last()).filter(f.col("sum(Deaths)").isNotNull())

    def question09(self) -> DataFrame:
        death_capita = self._death_join_pop.withColumn("deaths_per_capita", ("sum(Deaths)" / f.col("Population"))
                                                       .cast(DecimalType(10, 10)))
        return death_capita.sort(f.col("deaths_per_capita").desc_nulls_last())

    def question10(self) -> DataFrame:
        return self._covid_continents.groupBy(f.col("Continent")).sum("Deaths").select(f.col("Continent"),
                                                                                          f.col("sum(Deaths)")) \
            .orderBy(f.col("sum(Deaths)").desc())

    def question11(self) -> DataFrame:
        d1 = self._covid_data.select(f.col("Deaths").alias("Data"))
        d2 = self._covid_data.select(f.col("Confirmed").alias("Data"))
        d3 = self._covid_data.select(f.col("Recovered").alias("Data"))
        all_data = d1.union(d2).union(d3)
        return all_data.filter(f.col("Data").isNotNull()).filter(f.col("Data") > 0).withColumn("Data",
                                                                                                     f.col("Data")
                                                                                                     .cast("string")
                                                                                                     .substr(0, 3)
                                                                                                     .cast("long")) \
            .groupBy("Data").count().orderBy(f.col("count").desc())

    def country_by_month(self) -> DataFrame:
        n_df = self._covid_data.withColumn("Date", f.date_format(f.col("Date"), "yyyy-MM"))
        n_df = n_df.groupBy("Country/Region", "Date").sum("Confirmed", "Deaths", "Recovered").orderBy("Date") \
            .filter(f.col("Date").isNotNull())
        return n_df.withColumnRenamed("sum(Deaths)", "Deaths").withColumnRenamed("sum(Confirmed)", "Confirmed") \
            .withColumnRenamed("sum(Recovered)", "Recovered")

    def get_monthly(self) -> DataFrame:
        covid = self.get_spark_session().read.option("header", "true").csv("data/covid_19_data_cleaned.csv")
        months = covid.withColumn("Date", f.to_date(f.col("Date"), "MM/dd/yyyy"))
        months = months.withColumn("Confirmed", f.col("Confirmed").cast("int")).withColumn("Deaths",
                                                                                              f.col("Deaths")
                                                                                              .cast("int")) \
            .withColumn("Recovered", f.col("Recovered").cast("int"))
        months = months.groupBy("Date").sum("Confirmed", "Deaths", "Recovered").orderBy(f.col("Date").asc()) \
            .withColumn("Mortality Rate", f.round(f.col("sum(Deaths)") / f.col("sum(Confirmed)"), 3)) \
            .withColumnRenamed("sum(Deaths)", "Deaths").withColumnRenamed("sum(Confirmed)", "Confirmed") \
            .withColumnRenamed("sum(Recovered)", "Recovered")
        months = months.select(f.date_format(f.col("Date"), "yyyy-MM").alias("Date"), f.col("Confirmed"),
                               f.col("Deaths"),
                               f.col("Recovered"))
        months = months.groupBy("Date").max("Confirmed", "Deaths", "Recovered").orderBy("Date") \
            .withColumnRenamed("max(Deaths)", "Deaths").withColumnRenamed("max(Confirmed)", "Confirmed") \
            .withColumnRenamed("max(Recovered)", "Recovered")
        months = months.withColumn("Mortality Rate", f.round(f.col("Deaths") / f.col("Confirmed"), 3))
        # window_spec = Window.partitionBy("Date").orderBy(F.Column("Date").asc)
        months = months.withColumn("Spread Rate", f.round((f.col("Confirmed") - f.lag("Confirmed", 1).over(
            Window.partitionBy().orderBy("Date"))) / f.lag("Confirmed", 1).over(Window.partitionBy().orderBy("Date")),
                                                          3)).withColumn("Difference",
                                                                         f.coalesce(
                                                                             f.col("Confirmed") - f.lag("Confirmed",
                                                                                                           1).over(
                                                                                 Window.partitionBy().orderBy("Date")),
                                                                             f.col("Confirmed"))).na.fill(0)
        months.withColumn("Increase in Cases", f.round((f.col("Difference")) / f.lag("Difference", 1).over(
            Window.partitionBy().orderBy("Date")), 3)).na.fill(0)
        return months

    def initialize_data(self) -> DataFrame:
        return self.get_spark_session().read.option("header", "true").csv("data/covid_daily_differences.csv") \
            .withColumn("Date", f.to_date(f.col("Date"), "MM/dd/yyyy")) \
            .withColumn("Confirmed", f.col("Confirmed").cast("long")) \
            .withColumn("Confirmed", f.col("Confirmed").cast("int")) \
            .withColumn("Deaths", f.col("Deaths").cast("int")) \
            .withColumn("Recovered", f.col("Recovered").cast("int")) \
            .withColumn("Recovered", f.when(f.col("Recovered") < 0, 0).otherwise(f.col("Recovered"))) \
            .withColumn("Deaths", f.when(f.col("Deaths") < 0, 0).otherwise(f.col("Deaths"))) \
            .withColumn("Confirmed", f.when(f.col("Confirmed") < 0, 0).otherwise(f.col("Confirmed")))
