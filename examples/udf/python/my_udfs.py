from pyflink.table.udf import udf
from pyflink.table import DataTypes

@udf(result_type=DataTypes.STRING())
def my_python_udf(input_str):
    # Your UDF logic here
    return input_str.upper() + "_processed"

@udf(result_type=DataTypes.INT())
def calculate_length(input_str):
    return len(input_str) if input_str else 0