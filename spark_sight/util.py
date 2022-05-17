import logging
from json import loads as json_loads
from typing import Optional

from requests import get, codes, Response

from spark_sight import __version__ as spark_sight_version


try:
    from packaging.version import parse
except ImportError:
    from pip._vendor.packaging.version import parse


def configure_pandas(
    pandas,
    infinite_rows: bool = False,
    additional_params: bool = False,
    float_precision: int = 2,
):
    """Configure Pandas module.

    Args:
        pandas: Pandas module
            where to apply the configuration.
        infinite_rows (bool): Whether to configure Pandas
            to display all rows.
        additional_params (bool): Whether to configure Pandas
            to display wider and justified columns.

    Returns:

    """
    pandas.set_option("display.expand_frame_repr", False)
    pandas.options.display.width = 0
    pandas.set_option("display.max_columns", None)
    pandas.options.display.float_format = (
        "{:,." + str(float_precision) + "f}"
    ).format
    
    if infinite_rows:
        pandas.set_option("display.max_rows", None)
    
    if additional_params:
        pandas.set_option("display.max_colwidth", 100)
        pandas.set_option("display.column_space", 40)
        pandas.set_option("display.colheader_justify", "left")


def is_latest_version():
    response: Optional[Response]
    
    try:
        response = get(
            'https://pypi.python.org/pypi/spark-sight/json',
            timeout=1,
        )
    except ConnectionError:
        response = None
    except Exception as e:
        logging.debug(
            f"Getting latest version, not connection error: {e}"
        )
        response = None
    
    if response is None:
        return True

    if response.status_code == codes.ok:
        j = json_loads(response.text.encode(response.encoding))
        releases = j.get('releases', [])
        assert releases, "No releases found"
        return spark_sight_version == max(
            parse(release)
            for release in releases
            if not parse(release).is_prerelease
        )
