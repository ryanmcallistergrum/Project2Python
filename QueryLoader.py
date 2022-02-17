import datetime
import logging
from logging import info

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import DecimalType


class QueryLoader:
    def __init__(self):
        logging.getLogger().setLevel(logging.INFO)
        self._covid_data = self.initialize_data()
        self._max_deaths = self._covid_data.select(f.Column("Country/Region"), f.Column("Deaths").cast("Int"))
        self._pop_data = self.get_spark_session().read.option("header", "true") \
            .csv("data/population_by_country_2020.csv")
        self._death_join_pop = self._max_deaths.join(self._pop_data, self._covid_data["Country/Region"].
                                                     __eq__(self._pop_data["Country"]), "inner")
        self._country_by_months = self.country_by_month()
        self._monthly_data = self.get_monthly()
        self._continent = self.get_spark_session().read.option("header", "true").csv("data/continents.csv")
        self._covid_continents = self._covid_data.join(self._continent, "Country/Region")

    def load_query(self, question):
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
    def get_spark_session():
        return SparkSession.builder.appName("Covid Analyze App").master("local[*]").enableHiveSupport().getOrCreate()

    def question01(self):
        return self._monthly_data.select("Date", "Mortality Rate", "Spread Rate", "Difference").orderBy("Date")

    def question02(self):
        return self._monthly_data

    def question03(self):
        return self._monthly_data.select("Date", "Confirmed", "Deaths", "Recovered").orderBy("Date")

    def question04(self):
        return self._monthly_data.select("Date", "`Mortality Rate`").withColumn("Mortality Rate",
                                                                                f.round(f.Column("Mortality Rate") *
                                                                                        100, 2)).orderBy("Date")

    def question05(self):
        return self._country_by_months.withColumn("Deaths", f.Column("Deaths").cast("Int")).groupBy("Country/Region") \
            .sum("Deaths").orderBy(f.Column("sum(Deaths)").desc()).limit(10)

    def question06(self):
        return self._country_by_months.withColumn("Deaths", f.Column("Deaths").cast("Int")).groupBy("Country/Region") \
            .sum("Deaths").orderBy(f.Column("sum(Deaths)").asc()).filter(f.Column("sum(Deaths)").isNotNull())

    def question07(self):
        df = self._covid_data.select(
            f.Column("Date"),
            f.Column("Country/Region"),
            f.Column("Confirmed").cast("long")
        ).withColumn("Date", f.to_date(f.Column("Date"), "MM/dd/yyyy"))
        return df.groupBy(f.Column("Date")).sum("Confirmed").orderBy("Date") \
            .withColumnRenamed("sum(Confirmed)", "Confirmed").withColumn("Difference",
                                                                         f.coalesce(
                                                                             f.Column("Confirmed") - f.lag("Confirmed",
                                                                                                           1).over(
                                                                                 Window.partitionBy().orderBy("Date")),
                                                                             f.Column("Confirmed"))).na.fill(
            0).withColumn("DayOfWeek",
                          f.to_date(f.Column("Date"), "MM/dd/yyyy")) \
            .withColumn("DayOfWeek", f.date_format(f.Column("DayOfWeek"), "E")).filter(f.Column("Date").isNotNull())

    def question08(self):
        death_continents = self._death_join_pop.join(self._continent,
                                                     self._death_join_pop["Country"].__eq__(
                                                         self._continent["Country/Region"])).drop("Country/Region")
        sum_deaths = death_continents.groupBy("Deaths").sum("Deaths")
        death_continents = death_continents.join(sum_deaths, "Deaths", "inner")
        modified = death_continents.withColumn("sum(Deaths)", f.log("sum(Deaths)")).withColumn("Population",
                                                                                               f.log("Population"))
        print("Correlation Value: ", modified.stat.corr("Population", "sum(Deaths)"))
        return modified.sort(f.Column("Population").desc_nulls_last()).filter(f.Column("sum(Deaths)").isNotNull())

    def question09(self):
        death_capita = self._death_join_pop.withColumn("deaths_per_capita", ("sum(Deaths)" / f.Column("Population"))
                                                       .cast(DecimalType(10, 10)))
        return death_capita.sort(f.Column("deaths_per_capita").desc_nulls_last())

    def question10(self):
        return self._covid_continents.groupBy(f.Column("Continent")).sum("Deaths").select(f.Column("Continent"),
                                                                                          f.Column("sum(Deaths)")) \
            .orderBy(f.Column("sum(Deaths)").desc())

    def question11(self):
        d1 = self._covid_data.select(f.Column("Deaths").alias("Data"))
        d2 = self._covid_data.select(f.Column("Confirmed").alias("Data"))
        d3 = self._covid_data.select(f.Column("Recovered").alias("Data"))
        all_data = d1.union(d2).union(d3)
        return all_data.filter(f.Column("Data").isNotNull()).filter(f.Column("Data") > 0).withColumn("Data",
                                                                                                     f.Column("Data")
                                                                                                     .cast("string")
                                                                                                     .substr(0, 3)
                                                                                                     .cast("long")) \
            .groupBy("Data").count().orderBy(f.Column("count").desc())

    def country_by_month(self):
        n_df = self._covid_data.withColumn("Date", f.date_format(f.Column("Date"), "yyyy-MM"))
        n_df = n_df.groupBy("Country/Region", "Date").sum("Confirmed", "Deaths", "Recovered").orderBy("Date") \
            .filter(f.Column("Date").isNotNull())
        return n_df.withColumnRenamed("sum(Deaths)", "Deaths").withColumnRenamed("sum(Confirmed)", "Confirmed") \
            .withColumnRenamed("sum(Recovered)", "Recovered")

    def get_monthly(self):
        covid = self.get_spark_session().read.option("header", "true").csv("data/covid_19_data_cleaned.csv")
        months = covid.withColumn("Date", f.to_date(f.Column("Date"), "MM/dd/yyyy"))
        months = months.withColumn("Confirmed", f.Column("Confirmed").cast("int")).withColumn("Deaths",
                                                                                              f.Column("Deaths")
                                                                                              .cast("int")) \
            .withColumn("Recovered", f.Column("Recovered").cast("int"))
        months = months.groupBy("Date").sum("Confirmed", "Deaths", "Recovered").orderBy(f.Column("Date").asc()) \
            .withColumn("Mortality Rate", f.round(f.Column("sum(Deaths)") / f.Column("sum(Confirmed)"), 3)) \
            .withColumnRenamed("sum(Deaths)", "Deaths").withColumnRenamed("sum(Confirmed)", "Confirmed") \
            .withColumnRenamed("sum(Recovered)", "Recovered")
        months = months.select(f.date_format(f.Column("Date"), "yyyy-MM").alias("Date"), f.Column("Confirmed"),
                               f.Column("Deaths"),
                               f.Column("Recovered"))
        months = months.groupBy("Date").max("Confirmed", "Deaths", "Recovered").orderBy("Date") \
            .withColumnRenamed("max(Deaths)", "Deaths").withColumnRenamed("max(Confirmed)", "Confirmed") \
            .withColumnRenamed("max(Recovered)", "Recovered")
        months = months.withColumn("Mortality Rate", f.round(f.Column("Deaths") / f.Column("Confirmed"), 3))
        # window_spec = Window.partitionBy("Date").orderBy(F.Column("Date").asc)
        months = months.withColumn("Spread Rate", f.round((f.Column("Confirmed") - f.lag("Confirmed", 1).over(
            Window.partitionBy().orderBy("Date"))) / f.lag("Confirmed", 1).over(Window.partitionBy().orderBy("Date")),
                                                          3)).withColumn("Difference",
                                                                         f.coalesce(
                                                                             f.Column("Confirmed") - f.lag("Confirmed",
                                                                                                           1).over(
                                                                                 Window.partitionBy().orderBy("Date")),
                                                                             f.Column("Confirmed"))).na.fill(0)
        months.withColumn("Increase in Cases", f.round((f.Column("Difference")) / f.lag("Difference", 1).over(
            Window.partitionBy().orderBy("Date")), 3)).na.fill(0)
        return months

    def initialize_data(self):
        return self.get_spark_session().read.option("header", "true").csv("data/covid_daily_differences.csv") \
            .withColumn("Date", f.to_date(f.Column("Date"), "MM/dd/yyyy")) \
            .withColumn("Confirmed", f.Column("Confirmed").cast("long")) \
            .withColumn("Confirmed", f.Column("Confirmed").cast("int")) \
            .withColumn("Deaths", f.Column("Deaths").cast("int")) \
            .withColumn("Recovered", f.Column("Recovered").cast("int")) \
            .withColumn("Recovered", f.when(f.Column("Recovered") < 0, 0).otherwise(f.Column("Recovered"))) \
            .withColumn("Deaths", f.when(f.Column("Deaths") < 0, 0).otherwise(f.Column("Deaths"))) \
            .withColumn("Confirmed", f.when(f.Column("Confirmed") < 0, 0).otherwise(f.Column("Confirmed")))
