import pandas as pd
from dagster import AssetExecutionContext, asset

from dagster_project.constants import SCHEMA_REFERENCE

# ISO 4217 currency code -> common English name. Static reference data (not
# derived from the REST Countries API) - REST Countries gives per-country
# currency names, but dm_currency_distribution aggregates by code across
# countries, so a per-country name can't survive that GROUP BY on its own
# (countries phrase the same currency's name slightly differently). Kept as
# its own dimension table instead of a hardcoded dict in the dashboard, so
# it's joined in SQL like any other reference data.
CURRENCY_NAMES: dict[str, str] = {
    "AED": "UAE dirham", "AFN": "Afghan afghani", "ALL": "Albanian lek",
    "AMD": "Armenian dram", "ANG": "Neth. Antillean guilder", "AOA": "Angolan kwanza",
    "ARS": "Argentine peso", "AUD": "Australian dollar", "AWG": "Aruban florin",
    "AZN": "Azerbaijani manat", "BAM": "Bosnia-Herz. mark", "BBD": "Barbadian dollar",
    "BDT": "Bangladeshi taka", "BGN": "Bulgarian lev", "BHD": "Bahraini dinar",
    "BIF": "Burundian franc", "BMD": "Bermudian dollar", "BND": "Brunei dollar",
    "BOB": "Bolivian boliviano", "BRL": "Brazilian real", "BSD": "Bahamian dollar",
    "BTN": "Bhutanese ngultrum", "BWP": "Botswana pula", "BYN": "Belarusian ruble",
    "BZD": "Belize dollar", "CAD": "Canadian dollar", "CDF": "Congolese franc",
    "CHF": "Swiss franc", "CLP": "Chilean peso", "CNY": "Chinese yuan",
    "COP": "Colombian peso", "CRC": "Costa Rican colon", "CUP": "Cuban peso",
    "CVE": "Cape Verdean escudo", "CZK": "Czech koruna", "DJF": "Djiboutian franc",
    "DKK": "Danish krone", "DOP": "Dominican peso", "DZD": "Algerian dinar",
    "EGP": "Egyptian pound", "ERN": "Eritrean nakfa", "ETB": "Ethiopian birr",
    "EUR": "Euro", "FJD": "Fijian dollar", "FKP": "Falkland Islands pound",
    "GBP": "British pound", "GEL": "Georgian lari", "GHS": "Ghanaian cedi",
    "GIP": "Gibraltar pound", "GMD": "Gambian dalasi", "GNF": "Guinean franc",
    "GTQ": "Guatemalan quetzal", "GYD": "Guyanese dollar", "HKD": "Hong Kong dollar",
    "HNL": "Honduran lempira", "HRK": "Croatian kuna", "HTG": "Haitian gourde",
    "HUF": "Hungarian forint", "IDR": "Indonesian rupiah", "ILS": "Israeli shekel",
    "INR": "Indian rupee", "IQD": "Iraqi dinar", "IRR": "Iranian rial",
    "ISK": "Icelandic krona", "JMD": "Jamaican dollar", "JOD": "Jordanian dinar",
    "JPY": "Japanese yen", "KES": "Kenyan shilling", "KGS": "Kyrgyzstani som",
    "KHR": "Cambodian riel", "KMF": "Comorian franc", "KPW": "N. Korean won",
    "KRW": "S. Korean won", "KWD": "Kuwaiti dinar", "KYD": "Cayman Islands dollar",
    "KZT": "Kazakhstani tenge", "LAK": "Lao kip", "LBP": "Lebanese pound",
    "LKR": "Sri Lankan rupee", "LRD": "Liberian dollar", "LSL": "Lesotho loti",
    "LYD": "Libyan dinar", "MAD": "Moroccan dirham", "MDL": "Moldovan leu",
    "MGA": "Malagasy ariary", "MKD": "Macedonian denar", "MMK": "Myanmar kyat",
    "MNT": "Mongolian tugrik", "MOP": "Macanese pataca", "MRU": "Mauritanian ouguiya",
    "MUR": "Mauritian rupee", "MVR": "Maldivian rufiyaa", "MWK": "Malawian kwacha",
    "MXN": "Mexican peso", "MYR": "Malaysian ringgit", "MZN": "Mozambican metical",
    "NAD": "Namibian dollar", "NGN": "Nigerian naira", "NIO": "Nicaraguan cordoba",
    "NOK": "Norwegian krone", "NPR": "Nepalese rupee", "NZD": "NZ dollar",
    "OMR": "Omani rial", "PAB": "Panamanian balboa", "PEN": "Peruvian sol",
    "PGK": "Papua New Guinean kina", "PHP": "Philippine peso", "PKR": "Pakistani rupee",
    "PLN": "Polish zloty", "PYG": "Paraguayan guarani", "QAR": "Qatari riyal",
    "RON": "Romanian leu", "RSD": "Serbian dinar", "RUB": "Russian ruble",
    "RWF": "Rwandan franc", "SAR": "Saudi riyal", "SBD": "Solomon Islands dollar",
    "SCR": "Seychellois rupee", "SDG": "Sudanese pound", "SEK": "Swedish krona",
    "SGD": "Singapore dollar", "SHP": "Saint Helena pound", "SLE": "Sierra Leonean leone",
    "SOS": "Somali shilling", "SRD": "Surinamese dollar", "SSP": "South Sudanese pound",
    "STN": "Sao Tome dobra", "SYP": "Syrian pound", "SZL": "Eswatini lilangeni",
    "THB": "Thai baht", "TJS": "Tajikistani somoni", "TMT": "Turkmenistani manat",
    "TND": "Tunisian dinar", "TOP": "Tongan paanga", "TRY": "Turkish lira",
    "TTD": "Trinidad & Tobago dollar", "TWD": "New Taiwan dollar", "TZS": "Tanzanian shilling",
    "UAH": "Ukrainian hryvnia", "UGX": "Ugandan shilling", "USD": "US dollar",
    "UYU": "Uruguayan peso", "UZS": "Uzbekistani som", "VES": "Venezuelan bolivar",
    "VND": "Vietnamese dong", "VUV": "Vanuatu vatu", "WST": "Samoan tala",
    "XAF": "C. African CFA franc", "XCD": "E. Caribbean dollar", "XOF": "W. African CFA franc",
    "XPF": "CFP franc", "YER": "Yemeni rial", "ZAR": "South African rand",
    "ZMW": "Zambian kwacha", "ZWL": "Zimbabwean dollar",
}


@asset(
    group_name="reference",
    io_manager_key="iceberg_io_manager",
    metadata={"schema": SCHEMA_REFERENCE},
)
def dim_currency(context: AssetExecutionContext) -> pd.DataFrame:
    """Reference table: ISO 4217 currency code -> name, joined into
    dm_currency_distribution so the dashboard can show full currency names
    without hardcoding a lookup of its own."""
    df = pd.DataFrame(
        list(CURRENCY_NAMES.items()), columns=["currency_code", "currency_name"]
    )
    context.log.info(f"Loaded {len(df)} currency names")
    return df
