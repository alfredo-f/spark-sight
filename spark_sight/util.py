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
