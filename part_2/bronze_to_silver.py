import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))




clean_text_udf = udf(clean_text, StringType())

# тут df - це spark DataFrame
df = df.withColumn(col_name, clean_text_udf(df[col_name]))
