from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def transform(df):
    df = (
        df
        .withColumn("ID_NUM_PROPOSTA", col("ID_NUM_PROPOSTA").cast('integer'))
        .withColumn("DT_INICIO_PROPOSTA", to_date("DT_INICIO_PROPOSTA"))
        .withColumn("DT_FIM_PROPOSTA", to_date("DT_FIM_PROPOSTA"))
        .withColumn("VL_PROPOSTA", col("VL_PROPOSTA").cast('double'))
    )
    return df

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    
    df = (
        spark
        .read
        .option("header", True)
        .csv("data/proposta.csv", sep=";")
    )

    transform(df)
    
    df.printSchema()    
    df.show()