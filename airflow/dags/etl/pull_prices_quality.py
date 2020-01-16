if STOCKS is None or len(STOCKS) == 0:
    check_basic_quality(logger, DB_HOST, TABLE_STOCK_INFO_NASDAQ, table_type='csv')
    check_basic_quality(logger, DB_HOST, TABLE_STOCK_INFO_NYSE, table_type='csv')
check_basic_quality(logger, DB_HOST, TABLE_STOCK_PRICES, table_type='csv')