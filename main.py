import timeit
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def print_elapsed_time(start_time, label):
    try:
        end_time = timeit.default_timer()
        elapsed_time = end_time - start_time
        print(f"{label} Elapsed time: {elapsed_time:.4f} seconds")
    except Exception as t:
        print("An error occur for calculating time.")

def initialize_spark():
    try:
        start_time = timeit.default_timer()
        spark = SparkSession.builder \
            .appName("IcebergLocalDevelopment") \
            .master("local[*]") \
            .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2') \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg") \
            .getOrCreate()
        print_elapsed_time(start_time, "Spark initialization")
    except Exception as e:
        print ("An error occur for spark initialization")
    return spark

def load_data(spark):
    try:
        start_time = timeit.default_timer()
        df = spark.read.json("expedia-lodging-listings-en_us-1-all.jsonl")
        df.show(10)
        print_elapsed_time(start_time, "Data loading")
    except Exception as e:
        print(f"An error occurred during DataFrame reading or displaying: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return df

def define_target_schema():
    try:
        start_time = timeit.default_timer()
        target_schema = StructType([
            StructField("listing_source_site", StringType(), False),
            StructField("property_modified_date", TimestampType(), False),
            StructField("star_rating", StringType(), True),
            StructField("currency", StringType(), False),
            StructField("usd_price", DoubleType(), True),
            StructField("chain_and_brand", StructType([
                StructField("brand_id", IntegerType(), True),
                StructField("chain_id", IntegerType(), True),
                StructField("brand_name", StringType(), True),
                StructField("chain_name", StringType(), True)
            ]), True),
            StructField("country_code", StringType(), False)
        ])
        print_elapsed_time(start_time, "Schema definition")
        return target_schema
    except Exception as e:
        print(f"An error occurred while defining the schema: {e}")


def get_country_code_mapping():
    try:
        start_time = timeit.default_timer()
        country_code_mapping = {

        "Bonaire Saint Eustatius and Saba": "BSES",
        "Paraguay": "PY",
        "Anguilla": "AI",
        "Macao": "MO",
        "U.S. Virgin Islands": "USV",
        "Senegal": "SN",
        "Sweden": "SE",
        "Guyana": "GY",
        "Philippines": "PH",
        "Jersey": "JE",
        "Eritrea": "ER",
        "Djibouti": "DJ",
        "Norfolk Island": "NF",
        "Tonga": "TO",
        "Singapore": "SG",
        "Malaysia": "MY",
        "Fiji": "FJ",
        "Turkey": "TR",
        "Malawi": "MW",
        "Germany": "DE",
        "Northern Mariana Islands": "MP",
        "Comoros": "KM",
        "Cambodia": "KH",
        "Maldives": "MV",
        "Ivory Coast": "IC",
        "Jordan": "JO",
        "Rwanda": "RW",
        "Palau": "PW",
        "France": "FR",
        "Turks and Caicos Islands": "TC",
        "Greece": "GR",
        "Sri Lanka": "LK",
        "Montserrat": "MS",
        "Taiwan": "TW",
        "Dominica": "DM",
        "British Virgin Islands": "VG",
        "Algeria": "DZ",
        "Togo": "TG",
        "Equatorial Guinea": "GQ",
        "Slovakia": "SK",
        "Reunion": "RE",
        "Argentina": "AR",
        "Belgium": "BE",
        "Angola": "AO",
        "San Marino": "SM",
        "Ecuador": "EC",
        "Qatar": "QA",
        "Lesotho": "LS",
        "Albania": "AL",
        "Madagascar": "MG",
        "Finland": "FI",
        "New Caledonia": "NC",
        "Ghana": "GH",
        "Myanmar": "MM",
        "Nicaragua": "NI",
        "Guernsey": "GG",
        "Peru": "PE",
        "Benin": "BJ",
        "Sierra Leone": "SL",
        "United States": "US",
        "India": "IN",
        "Bahamas": "BS",
        "China": "CN",
        "Curacao": "CUR",
        "Belarus": "BY",
        "Malta": "MT",
        "Kuwait": "KW",
        "Sao Tome and Principe": "ST",
        "Palestinian Territory": "PT",
        "Puerto Rico": "PR",
        "Chile": "CL",
        "Tajikistan": "TJ",
        "Martinique": "MQ",
        "Cayman Islands": "KY",
        "Isle of Man": "IM",
        "Croatia": "HR",
        "Burundi": "BI",
        "Nigeria": "NG",
        "Andorra": "AD",
        "Bolivia": "BO",
        "Gabon": "GA",
        "Italy": "IT",
        "Suriname": "SR",
        "Lithuania": "LT",
        "Norway": "NO",
        "Turkmenistan": "TM",
        "Spain": "ES",
        "Cuba": "CU",
        "Mauritania": "MR",
        "Guadeloupe": "GP",
        "Denmark": "DK",
        "Barbados": "BB",
        "Bangladesh": "BD",
        "Ireland": "IE",
        "Liechtenstein": "LI",
        "Swaziland": "SL",
        "Thailand": "TH",
        "Laos": "LA",
        "Christmas Island": "CX",
        "Bhutan": "BT",
        "Democratic Republic of the Congo": "DRC",
        "Morocco": "MA",
        "Monaco": "MC",
        "Panama": "PA",
        "Cape Verde": "CV",
        "Hong Kong": "HK",
        "Israel": "IL",
        "Iceland": "IS",
        "Saint Barthelemy": "SB",
        "Saint Kitts and Nevis": "KN",
        "Oman": "OM",
        "French Polynesia": "PF",
        "South Korea": "KR",
        "Cyprus": "CY",
        "Gibraltar": "GI",
        "Uruguay": "UY",
        "Mexico": "MX",
        "Aruba": "AW",
        "Montenegro": "ME",
        "Georgia": "GE",
        "Zimbabwe": "ZW",
        "Estonia": "EE",
        "Indonesia": "ID",
        "Saint Vincent and the Grenadines": "VC",
        "Guatemala": "GT",
        "Guam": "GU",
        "Mongolia": "MN",
        "Republic of the Congo": "CG",
        "Azerbaijan": "AZ",
        "Sint Maarten": "SIM",
        "Grenada": "GD",
        "Armenia": "AM",
        "Tunisia": "TN",
        "Liberia": "LR",
        "Honduras": "HN",
        "Trinidad and Tobago": "TT",
        "Saudi Arabia": "SA",
        "Uganda": "UG",
        "Wallis and Futuna": "WF",
        "French Guiana": "GF",
        "Namibia": "NA",
        "Mayotte": "YT",
        "Switzerland": "CH",
        "Zambia": "ZM",
        "Ethiopia": "ET",
        "Jamaica": "JM",
        "Latvia": "LV",
        "United Arab Emirates": "AE",
        "Brunei": "BR",
        "Saint Lucia": "LC",
        "Saint Martin": "SAM",
        "Aland Islands": "AI",
        "Guinea": "GN",
        "Canada": "CA",
        "Seychelles": "SC",
        "Kyrgyzstan": "KG",
        "Uzbekistan": "UZ",
        "Macedonia": "MD",
        "Faroe Islands": "FO",
        "Samoa": "WS",
        "Czech Republic": "CZ",
        "Mozambique": "MZ",
        "Cook Islands": "CK",
        "Brazil": "BR",
        "Belize": "BZ",
        "Kenya": "KE",
        "Gambia": "GM",
        "Lebanon": "LB",
        "Slovenia": "SI",
        "Antigua and Barbuda": "AG",
        "Dominican Republic": "DO",
        "Japan": "JP",
        "Tanzania": "TZ",
        "Botswana": "BW",
        "Luxembourg": "LU",
        "New Zealand": "NZ",
        "United States Minor Outlying Islands": "UM",
        "Bosnia and Herzegovina": "BA",
        "Greenland": "GL",
        "Haiti": "HT",
        "Poland": "PL",
        "Portugal": "PT",
        "Australia": "AU",
        "Cameroon": "CM",
        "Papua New Guinea": "PG",
        "Romania": "RO",
        "Guinea-Bissau": "GW",
        "Bulgaria": "BG",
        "Austria": "AT",
        "Nepal": "NP",
        "Egypt": "EG",
        "Costa Rica": "CR",
        "El Salvador": "SV",
        "Kazakhstan": "KZ",
        "Serbia": "RS",
        "South Africa": "ZA",
        "Burkina Faso": "BF",
        "Bermuda": "BM",
        "Bahrain": "BH",
        "Micronesia": "MC",
        "Colombia": "CO",
        "Hungary": "HU",
        "Pakistan": "PK",
        "Vanuatu": "VU",
        "Mauritius": "MU",
        "United Kingdom": "GB",
        "Moldova": "MD",
        "Vietnam": "VN",
        "Netherlands": "NL",
        "Mali": "ML",
        "Chad": "TD",
        "Svalbard and Jan Mayen": "SJ",
        "Sudan": "SD",
        "Niue": "NU",
        "Kiribati": "KI",
        "Iraq": "IQ",
        "American Samoa": "AS",
        "Saint Pierre and Miquelon": "PM",
        "Niger": "NE",
        "Solomon Islands": "SB"
    }
        print_elapsed_time(start_time, "Country code mapping")
        return country_code_mapping
    except Exception as e:
        print(f"An error occurred while defining or registering the UDF: {e}")

def define_country_udf(country_code_mapping):
    try:
        start_time = timeit.default_timer()
        def get_country_code(country_name):
            if country_name is None:
                return "' '"
            return country_code_mapping.get(country_name, "' '")

        country_udf = F.udf(get_country_code, StringType())
        print_elapsed_time(start_time, "UDF definition")
        return country_udf
    except Exception as e:
        print("An error occur for country code mapping")

def transform_dataframe(df, country_udf):
    try:
        start_time = timeit.default_timer()
        transformed_df = df.select(
            F.lit("expedia").alias("listing_source_site"),
            F.when(F.col("lastUpdated").isNull(), F.lit("' '"))
            .otherwise(F.to_timestamp("lastUpdated", "dd-MM-yyyy HH:mm:ss"))
            .alias("property_modified_date"),
            F.when(F.col("starRating").isNull(), F.lit("' '"))
            .otherwise(F.col("starRating").cast(StringType()))
            .alias("star_rating"),
            F.when(F.col("referencePrice.currency").isNull(), F.lit("' '"))
            .otherwise(F.col("referencePrice.currency"))
            .alias("currency"),
            F.when(F.col("referencePrice.value").isNull(), F.lit("' '"))
            .otherwise(F.col("referencePrice.value").cast(DoubleType()))
            .alias("usd_price"),
            F.struct(
                F.when(F.col("chainAndBrand.brandId").isNull(), F.concat(F.lit('"brand_id": '), F.lit("' '")))
                .otherwise(F.concat(F.lit('"brand_id": '), F.col("chainAndBrand.brandId").cast(IntegerType()))).alias("brand_id"),
                F.when(F.col("chainAndBrand.chainId").isNull(), F.concat(F.lit('"chain_id": '), F.lit("' '")))
                .otherwise(F.concat(F.lit('"chain_id": '), F.col("chainAndBrand.chainId").cast(IntegerType()))).alias("chain_id"),
                F.when(F.col("chainAndBrand.brandName").isNull(), F.concat(F.lit('"brand_name": '), F.lit("' '")))
                .otherwise(F.concat(F.lit('"brand_name": '), F.col("chainAndBrand.brandName").cast(StringType()))).alias("brand_name"),
                F.when(F.col("chainAndBrand.chainName").isNull(), F.concat(F.lit('"chain_name": '), F.lit("' '")))
                .otherwise(F.concat(F.lit('"chain_name": '), F.col("chainAndBrand.chainName").cast(StringType()))).alias("chain_name")
            ).alias("chain_and_brand"),
            F.col("country")  
        )
        print_elapsed_time(start_time, "DataFrame transformation")
        return transformed_df
    except Exception as e:
        print(f"An error occuured during DataFrame transformation: {e}")


def add_country_code_and_show(transformed_df, country_udf):
    try:
        start_time = timeit.default_timer()
        final_df = transformed_df.withColumn("country_code", country_udf(F.col("country"))).drop("country")
        final_df.show(5, truncate=False)
        print_elapsed_time(start_time, "Country code addition")
        return final_df
    except Exception as e:
        print(f"An error occurred during DataFrame transformation or display: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def write_to_iceberg(final_df):
    try:
        start_time = timeit.default_timer()
        final_df.writeTo("local.test_db") \
            .partitionedBy("country_code") \
            .createOrReplace()
        print_elapsed_time(start_time, "Data writing")
    except Exception as e:
        print(f"An error occurred while writing to the Iceberg table: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def read_from_iceberg(spark):
    try:
        start_time = timeit.default_timer()
        iceberg_df = spark.read \
            .format("iceberg") \
            .load("local.test_db")
        #iceberg_df.show()
        iceberg_df.limit(200).show(truncate=False)
        print_elapsed_time(start_time, "Data reading")
    except Exception as r:
        print(f"An error occured while writing to Iceberg table: {e}")

def query_iceberg_table(spark, table_name):
    try:
        start_time = timeit.default_timer()
        spark.sql(f"USE local")
        
        result_df = spark.sql(f"SELECT * FROM {table_name} LIMIT 200")
        
        result_df.show(truncate=False)
        print_elapsed_time(start_time, "Data reading")
    except Exception as e:
        print("Problem for read iceberg table")


def main():
    try:
        start_time = timeit.default_timer()
        spark = initialize_spark()
        df = load_data(spark)
        #target_schema = define_target_schema()
        country_code_mapping = get_country_code_mapping()
        country_udf = define_country_udf(country_code_mapping)
        transformed_df = transform_dataframe(df, country_udf)
        final_df = add_country_code_and_show(transformed_df, country_udf)
        table_name = "test_db"
        write_to_iceberg(final_df)
        query_iceberg_table(spark, table_name)

        #read_from_iceberg(spark)

        end_time = timeit.default_timer()
        print_elapsed_time(start_time, "Total Time required for executaion")
    except Exception as e:
        print("An error occur in the main function")

if __name__ == "__main__":
    main()