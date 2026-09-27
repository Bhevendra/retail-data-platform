import pyspark.sql.functions as F
from common_utils.logging import get_logger


logger = get_logger("generic_functions")


def rename_columns(df, mapping):
    """
    Rename columns from a dictionary.
    Missing columns are ignored.
    """

    for old_name, new_name in mapping.items():

        if old_name in df.columns:

            logger.info(
                "Renaming column [%s] to [%s]",
                old_name,
                new_name
            )

            df = df.withColumnRenamed(
                old_name,
                new_name
            )

    return df



def safe_cast(df, mapping):
    """
    Safely cast columns using try_cast.

    For integral data types, first cast to DOUBLE
    so values such as '34.0' can become 34.
    """

    INTEGRAL_TYPES = {
    "byte",
    "short",
    "int",
    "integer",
    "long" }

    for column, target_type in mapping.items():

        if column not in df.columns:

            logger.warning(
                "Column [%s] not found. Skipping cast.",
                column
            )

            continue

        logger.info(
            "Casting column [%s] to [%s]",
            column,
            target_type
        )

        if target_type.lower() in INTEGRAL_TYPES:

            expression = (
                f"try_cast("
                f"try_cast(`{column}` AS DOUBLE) "
                f"AS {target_type})"
            )

        else:

            expression = (
                f"try_cast(`{column}` AS {target_type})"
            )

        df = df.withColumn(
            column,
            F.expr(expression)
        )

    return df


