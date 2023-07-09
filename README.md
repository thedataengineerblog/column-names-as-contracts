# Column Names as Contract

## Setup Environment

First, we have to create a virtual environment with Python. You can run the command below to do this.

```console
thedataengineerblog@desktop$ python3 -m venv venv
```

Second, we have to install the packages found in requirements.txt. Execute the command below:

```console
thedataengineerblog@desktop$ pip install -r requirements.txt
```

Third, we have to activate our environment. You can run the command below to do this too.

```console
thedataengineerblog@desktop$ source venv/bin/activate
```

## Testing our code

To test them, I created two bases. The first one (proposta.csv)  without errors in content and the second one (proposta-error.csv) with errors in content.

The basic idea is to change the base running at the moment for with errors and without errors.

In our test, we have the function below, responsible for returning our table. In this function, we can change the base.


```python
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
```