import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import flatten, col, udf, to_date, coalesce, to_json, collect_list, create_map, lit, map_entries, array_distinct

from spark.dependencies.spark import start_spark


def load_from_csv_df(spark_: SparkSession, arg, delimiter=','):
    return spark_.read \
        .option("delimiter", delimiter) \
        .option("header", True) \
        .csv(arg)


def lower_case(str_):
    return str_.lower()


def main():
    """Main ETL script definition.

    :return: None
    """

    # start Spark application and get Spark session
    spark, log, config = start_spark(
        app_name='Drugs',
        files=['spark/configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('job is up-and-running')

    """ Load csv file for PubMed """
    pubmed_df = load_from_csv_df(spark, sys.argv[1])
    pubmed_df.printSchema()
    pubmed_df.show(truncate=False)

    """ Load csv file for ClinicalTrials """
    clinical_trials_df = load_from_csv_df(spark, sys.argv[2])
    clinical_trials_df.printSchema()
    clinical_trials_df.show(truncate=False)

    """ Load csv file for Drugs """
    drugs_df = load_from_csv_df(spark, sys.argv[3])
    drugs_df.printSchema()
    drugs_df.show(truncate=False)

    """ Converting function to UDF """
    lower_case_udf = udf(lambda z: lower_case(z))

    # drugs_df.printSchema()
    # drugs_df.show(truncate=False)

    drugs2_df = drugs_df.select(col("atccode").alias("Id"),
                                lower_case_udf(col("drug")).alias("Name"))

    # drugs2_df.show(truncate=False)

    # pubmed_df.printSchema()
    # pubmed_df.show(truncate=False)

    def to_date_(col, formats=("dd/MM/yyyy", "yyyy-MM-dd", "d MMMM yyyy")):
        return coalesce(*[to_date(col, f) for f in formats])


    pubmed2_df = pubmed_df.select(col("id").alias("Pm_Id"),
                                  lower_case_udf(col("title")).alias("Title"),
                                  to_date_(col("date")).alias("Date"),
                                  col("journal").alias("Journal"))
    # pubmed2_df.show(truncate=False)
    pubmed2_df.persist()

    # clinical_trials_df.printSchema()
    # clinical_trials_df.show(truncate=False)

    clinical_trials2_df = clinical_trials_df.select(col("id").alias("Ct_Id"),
                                                    lower_case_udf(col("scientific_title")).alias("Title"),
                                                    to_date_(col("date")).alias("Date"),
                                                    col("journal").alias("Journal"))
    # clinical_trials2_df.show(truncate=False)
    clinical_trials2_df.persist()

    # ========
    # show the journal that mention the most drugs

    pubmed_count_df = pubmed2_df.groupby("Journal", "Pm_Id").count().select("Journal", col("count"))
    clinical_trials_count_df = clinical_trials2_df.groupby("Journal", "Ct_Id").count().select("Journal", col("count"))

    count_df = pubmed_count_df.unionByName(clinical_trials_count_df) \
        .groupby(col("Journal")) \
        .count() \
        .select("Journal", col("count")) \
        .orderBy(col("count").desc()) \
        .limit(1)

    count_df.show(truncate=False)

    # ========

    clinical_trials_drugs_df = drugs2_df.join(clinical_trials2_df, clinical_trials2_df.Title.contains(drugs2_df.Name))  # default inner
    clinical_trials_drugs_df.persist()
    # clinical_trials_drugs_df.printSchema()
    # clinical_trials_drugs_df.show(truncate=False)

    clinical_trials_drugs_agg_df = clinical_trials_drugs_df.drop("Ct_Id", "Journal").groupby(col("Id"), col("Name")).agg(
        to_json(collect_list(create_map('Title', 'Date'))).alias('json1')
    ).withColumn("json2", lit(None)).withColumn("json3", lit(None))

    # clinical_trials_drugs_agg_df.printSchema()
    # clinical_trials_drugs_agg_df.show(truncate=False)

    # ========

    pubmed_drugs_df = drugs2_df.join(pubmed2_df, pubmed2_df.Title.contains(drugs2_df.Name), "inner")
    pubmed_drugs_df.persist()
    # pubmed_drugs_df.printSchema()
    # pubmed_drugs_df.show(truncate=False)

    pubmed_drugs_agg_df = pubmed_drugs_df.drop("Pm_Id", "Journal").groupby(col("Id"), col("Name")).agg(
        to_json(collect_list(create_map('Title', 'Date'))).alias('json2')
    ).withColumn("json1", lit(None)).withColumn("json3", lit(None))

    # pubmed_drugs_agg_df.printSchema()
    # pubmed_drugs_agg_df.show(truncate=False)

    # ========

    clinical_trials_drugs_journals_agg_df = clinical_trials_drugs_df.drop("Ct_Id", "Title").groupby(col("Id"), col("Name")).agg(
        to_json(collect_list(create_map('Journal', 'Date'))).alias('json3')
    ).withColumn("json1", lit(None)).withColumn("json2", lit(None))

    pubmed_drugs_journal_agg_df = pubmed_drugs_df.drop("Pm_Id", "Title").groupby(col("Id"), col("Name")).agg(
        to_json(collect_list(create_map('Journal', 'Date'))).alias('json3')
    ).withColumn("json1", lit(None)).withColumn("json2", lit(None))

    journal_agg_df = clinical_trials_drugs_journals_agg_df.unionByName(pubmed_drugs_journal_agg_df)
    # journal_agg_df.printSchema()
    # journal_agg_df.show(truncate=False)

    # ========

    agg_df = clinical_trials_drugs_agg_df.unionByName(pubmed_drugs_agg_df).unionByName(journal_agg_df)
    # agg_df.printSchema()
    agg_df.show(truncate=False)

    maps_agg_df = agg_df.select(col("Id"),
                                col("Name"),
                                create_map(lit("ClinicalTrials"), col("json1")).alias("ClinicalTrials"),
                                create_map(lit("PubMeds"), col("json2")).alias("PubMeds"),
                                create_map(lit("Journals"), col("json3")).alias("Journals")).drop('json1', 'json2', 'json3')

    # maps_agg_df.printSchema()
    # maps_agg_df.show()

    # merged_agg_df = maps_agg_df.withColumn("Map", map_concat("ClinicalTrials", "PubMeds", "Journals")) \
    #    .drop("ClinicalTrials", "PubMeds", "Journals")

    # merged_agg_df.printSchema()
    # merged_agg_df.show(truncate=False)

    final_df = maps_agg_df \
        .withColumn("ctMap", map_entries(col("ClinicalTrials"))) \
        .withColumn("pmMap", map_entries(col("PubMeds"))) \
        .withColumn("jMap", map_entries(col("Journals"))) \
        .groupby(col("Id"), col("Name")).agg(
            array_distinct(flatten(collect_list(col("ctMap")))).alias("ClinicalTrialsMap"),
            array_distinct(flatten(collect_list(col("pmMap")))).alias("PubMedsMap"),
            array_distinct(flatten(collect_list(col("jMap")))).alias("JournalsMap")
    )

    final_df.printSchema()
    final_df.show(truncate=False)

    final_df.coalesce(1) \
        .write \
        .partitionBy("Id", "Name") \
        .mode("overwrite") \
        .json(sys.argv[4])  # output dir

    # log the success and terminate Spark application
    log.warn('job is finished')

    spark.stop()
    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
