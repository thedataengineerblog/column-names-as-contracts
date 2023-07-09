from transform import transform
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pytest


@pytest.fixture()
def table():    
    spark = SparkSession.builder.getOrCreate()
    
    df = (
        spark
        .read
        .option("header", True)
        .csv("data/proposta-error.csv", sep=";")
    )    
        
    yield transform(df)
    

class TestTablePropostas():

    def test_if_exists_dt_init_greater_than_dt_end(self, table):        
        qtd = table.filter(col("DT_INICIO_PROPOSTA")>col("DT_FIM_PROPOSTA") ).count()
        assert qtd == 0

    def test_if_exists_nulls(self, table):
        cols = [
                    'ID_NUM_PROPOSTA',
                    'DT_INICIO_PROPOSTA',
                    'DT_FIM_PROPOSTA',
                    'VL_PROPOSTA',
              ]        
        for c in cols:
            assert 0 == table.filter(col(c).isNull()).count(), f"{c}: contains nulls"
        