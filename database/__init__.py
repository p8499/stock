from database import industry_constituents, instrumentinfos, history_instruments, history_1d, \
    trading_derivative_indicator, balance_sheet, income_statement, cashflow_statement, finance, power


def build():
    industry_constituents.full_build()
    instrumentinfos.full_build()
    history_instruments.increment_build()
    history_1d.increment_build()
    trading_derivative_indicator.increment_build()
    balance_sheet.increment_build()
    income_statement.increment_build()
    cashflow_statement.increment_build()
    power.increment_build()
    finance.increment_build()
