import timeit

start_time = timeit.default_timer()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import random

end_time = timeit.default_timer()
elapsed_time = end_time - start_time

print("Elapsed time : ",elapsed_time)

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

end_time = timeit.default_timer()
elapsed_time = end_time - start_time

print("Elapsed time : ",elapsed_time)

start_time = timeit.default_timer()

df = spark.read.json("expedia-lodging-listings-en_us-1-all.jsonl")
df.show(10)

end_time = timeit.default_timer()
elapsed_time = end_time - start_time

print("Elapsed time : ",elapsed_time)

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

end_time = timeit.default_timer()
elapsed_time = end_time - start_time

print("Elapsed time : ",elapsed_time)

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

end_time = timeit.default_timer()
elapsed_time = end_time - start_time

print("Elapsed time : ",elapsed_time)

start_time = timeit.default_timer()

# Define a UDF that returns country code based on country name
def get_country_code(country_name):
    if country_name is None:
        return "Unknown"
    return country_code_mapping.get(country_name, "Unknown")

# Register the UDF
country_udf = F.udf(get_country_code, StringType())

end_time = timeit.default_timer()
elapsed_time = end_time - start_time

print("Elapsed time : ",elapsed_time)

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
    F.col("country")  # Include the `country` column explicitly here
)

end_time = timeit.default_timer()
elapsed_time = end_time - start_time

print("Elapsed time : ",elapsed_time)

start_time = timeit.default_timer()

final_df = transformed_df.withColumn("country_code", country_udf(F.col("country"))).drop("country")
final_df.show(5, truncate=False)

end_time = timeit.default_timer()
elapsed_time = end_time - start_time

print("Elapsed time : ",elapsed_time)

final_df.writeTo("local.test_db") \
    .partitionedBy("country_code") \
    .createOrReplace()

# Read from the Iceberg table
iceberg_df = spark.read \
    .format("iceberg") \
    .load("local.test_db")

# Show the data
iceberg_df.show()

