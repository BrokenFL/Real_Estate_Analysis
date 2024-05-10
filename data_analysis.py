
import pandas as pd
from Data_Loader import DataLoader

def get_first_day_of_month(dt):
    return dt.replace(day=1)

def get_last_day_of_month(dt):
    next_month = dt.replace(day=28) + pd.Timedelta(days=4)
    return next_month - pd.Timedelta(days=next_month.day)

def get_first_day_of_quarter(dt):
    quarter_start_month = ((dt.month - 1) // 3) * 3 + 1
    return dt.replace(month=quarter_start_month, day=1)

def get_last_day_of_quarter(dt):
    quarter_end_month = get_first_day_of_quarter(dt).month + 2
    return get_last_day_of_month(dt.replace(month=quarter_end_month))

def get_first_day_of_year(dt):
    return dt.replace(month=1, day=1)

def get_last_day_of_year(dt):
    return dt.replace(month=12, day=31)

def calculate_new_listings(df, timeframe, start_date, end_date):
    new_func = {
        'monthly': calculate_monthly_new_listings,
        'quarterly': calculate_quarterly_new_listings,
        'annually': calculate_annual_new_listings
    }
    return new_func[timeframe](df, start_date, end_date)

def calculate_monthly_new_listings(df, start_date, end_date):
    filtered_df = df[
        (df['listing_date'] >= start_date) &
        (df['listing_date'] <= end_date)
    ]
    return filtered_df['listing_date'].count()

def calculate_quarterly_new_listings(df, start_date, end_date):
    total_new_listings = 0
    current_date = start_date
    while current_date <= end_date:
        quarterly_df = df[
            (df['listing_date'] >= current_date) &
            (df['listing_date'] <= get_last_day_of_quarter(current_date))
        ]
        total_new_listings += quarterly_df['listing_date'].count()
        current_date = get_first_day_of_quarter(current_date) + pd.offsets.QuarterEnd()
    return total_new_listings

def calculate_annual_new_listings(df, start_date, end_date):
    total_new_listings = 0
    current_date = start_date
    while current_date <= end_date:
        annual_df = df[
            (df['listing_date'] >= current_date) &
            (df['listing_date'] <= get_last_day_of_year(current_date))
        ]
        total_new_listings += annual_df['listing_date'].count()
        current_date = get_first_day_of_year(current_date) + pd.offsets.YearEnd()
    return total_new_listings

def calculate_closed_listings(df, timeframe, start_date, end_date):
    closed_func = {
        'monthly': calculate_monthly_closed_listings,
        'quarterly': calculate_quarterly_closed_listings,
        'annually': calculate_annual_closed_listings
    }
    return closed_func[timeframe](df, start_date, end_date)

def calculate_monthly_closed_listings(df, start_date, end_date):
    filtered_df = df[
        (df['sold_date'] >= start_date) &
        (df['sold_date'] <= end_date)
    ]
    return filtered_df['sold_date'].count()

def calculate_quarterly_closed_listings(df, start_date, end_date):
    total_closed_listings = 0
    current_date = start_date
    while current_date <= end_date:
        quarterly_df = df[
            (df['sold_date'] >= current_date) &
            (df['sold_date'] <= get_last_day_of_quarter(current_date))
        ]
        total_closed_listings += quarterly_df['sold_date'].count()
        current_date = get_first_day_of_quarter(current_date) + pd.offsets.QuarterEnd()
    return total_closed_listings

def calculate_annual_closed_listings(df, start_date, end_date):
    total_closed_listings = 0
    current_date = start_date
    while current_date <= end_date:
        annual_df = df[
            (df['sold_date'] >= current_date) &
            (df['sold_date'] <= get_last_day_of_year(current_date))
        ]
        total_closed_listings += annual_df['sold_date'].count()
        current_date = get_first_day_of_year(current_date) + pd.offsets.YearEnd()
    return total_closed_listings

def calculate_avg_days_on_market(df, timeframe, start_date, end_date):
    days_func = {
        'monthly': calculate_monthly_avg_days_on_market,
        'quarterly': calculate_quarterly_avg_days_on_market,
        'annually': calculate_annual_avg_days_on_market
    }
    return days_func[timeframe](df, start_date, end_date)

def calculate_monthly_avg_days_on_market(df, start_date, end_date):
    filtered_df = df[
        (df['sold_date'] >= start_date) & (df['sold_date'] <= end_date)
    ]
    return filtered_df['cumulative_dom'].mean()

def calculate_quarterly_avg_days_on_market(df, start_date, end_date):
    total_dom = 0
    total_sales = 0
    current_date = start_date
    while current_date <= end_date:
        filtered_df = df[
            (df['sold_date'] >= current_date) &
            (df['sold_date'] <= get_last_day_of_quarter(current_date))
        ]
        total_dom += filtered_df['cumulative_dom'].sum()
        total_sales += filtered_df['sold_date'].count()
        current_date = get_first_day_of_quarter(current_date) + pd.offsets.QuarterEnd()

    if total_sales == 0:
        return None

    return total_dom / total_sales

def calculate_annual_avg_days_on_market(df, start_date, end_date):
    total_dom = 0
    total_sales = 0
    current_date = start_date
    while current_date <= end_date:
        filtered_df = df[
            (df['sold_date'] >= current_date) &
            (df['sold_date'] <= get_last_day_of_year(current_date))
        ]
        total_dom += filtered_df['cumulative_dom'].sum()
        total_sales += filtered_df['sold_date'].count()
        current_date = get_first_day_of_year(current_date) + pd.offsets.YearEnd()

    if total_sales == 0:
        return None

    return total_dom / total_sales

def calculate_total_dollar_volume(df, timeframe, start_date, end_date):
    volume_func = {
        'monthly': calculate_monthly_total_dollar_volume,
        'quarterly': calculate_quarterly_total_dollar_volume,
        'annually': calculate_annual_total_dollar_volume
    }
    return volume_func[timeframe](df, start_date, end_date)

def calculate_monthly_total_dollar_volume(df, start_date, end_date):
    filtered_df = df[
        (df['sold_date'] >= start_date) & (df['sold_date'] <= end_date)
    ]
    return filtered_df['sold_price'].sum()

def calculate_quarterly_total_dollar_volume(df, start_date, end_date):
    total_volume = 0
    current_date = start_date
    while current_date <= end_date:
        filtered_df = df[
            (df['sold_date'] >= current_date) &
            (df['sold_date'] <= get_last_day_of_quarter(current_date))
        ]
        total_volume += filtered_df['sold_price'].sum()
        current_date = get_first_day_of_quarter(current_date) + pd.offsets.QuarterEnd()

    return total_volume

def calculate_annual_total_dollar_volume(df, start_date, end_date):
    total_volume = 0
    current_date = start_date
    while current_date <= end_date:
        filtered_df = df[
            (df['sold_date'] >= current_date) &
            (df['sold_date'] <= get_last_day_of_year(current_date))
        ]
        total_volume += filtered_df['sold_price'].sum()
        current_date = get_first_day_of_year(current_date) + pd.offsets.YearEnd()

    return total_volume

def calculate_pending_listings(df, timeframe, end_date):
    pending_func = {
        'monthly': calculate_monthly_pending_listings,
        'quarterly': calculate_quarterly_pending_listings,
        'annually': calculate_annual_pending_listings
    }
    return pending_func[timeframe](df, end_date)

def calculate_monthly_pending_listings(df, end_date):
    filtered_df = df[
        (df['listing_date'] < end_date) &
        (df['under_contract_date'] <= end_date) &
        (df['end_of_listing_date'] > end_date)
    ]
    return filtered_df['listing_date'].count()

def calculate_quarterly_pending_listings(df, end_date):
    end_of_quarter = get_last_day_of_quarter(end_date)
    filtered_df = df[
        (df['listing_date'] < end_of_quarter) &
        (df['under_contract_date'] <= end_of_quarter) &
        (df['end_of_listing_date'] > end_of_quarter)
    ]
    return filtered_df['listing_date'].count()

def calculate_annual_pending_listings(df, end_date):
    end_of_year = get_last_day_of_year(end_date)
    filtered_df = df[
        (df['listing_date'] < end_of_year) &
        (df['under_contract_date'] <= end_of_year) &
        (df['end_of_listing_date'] > end_of_year)
    ]
    return filtered_df['listing_date'].count()

def calculate_list_price_to_sold_price_ratio(df, timeframe, start_date, end_date):
    ratio_func = {
        'monthly': calculate_monthly_list_price_to_sold_price_ratio,
        'quarterly': calculate_quarterly_list_price_to_sold_price_ratio,
        'annually': calculate_annual_list_price_to_sold_price_ratio
    }
    return ratio_func[timeframe](df, start_date, end_date)

def calculate_monthly_list_price_to_sold_price_ratio(df, start_date, end_date):
    filtered_df = df[(df['sold_date'] >= start_date) & (df['sold_date'] <= end_date)]
    return filtered_df['sold_price'].div(filtered_df['list_price']).mean()

def calculate_quarterly_list_price_to_sold_price_ratio(df, start_date, end_date):
    ratio = 0
    current_date = start_date
    while current_date <= end_date:
        monthly_ratio = calculate_monthly_list_price_to_sold_price_ratio(df, current_date, get_last_day_of_month(current_date))
        ratio += monthly_ratio
        current_date = get_first_day_of_month(current_date) + pd.offsets.MonthEnd()
    return ratio / 3

def calculate_annual_list_price_to_sold_price_ratio(df, start_date, end_date):
    ratio = 0
    current_date = start_date
    while current_date <= end_date:
        monthly_ratio = calculate_monthly_list_price_to_sold_price_ratio(df, current_date, get_last_day_of_month(current_date))
        ratio += monthly_ratio
        current_date = get_first_day_of_month(current_date) + pd.offsets.MonthEnd()
    return ratio / 12

def calculate_active_inventory(df, timeframe, end_date):
    inventory_func = {
        'monthly': calculate_monthly_active_inventory,
        'quarterly': calculate_quarterly_active_inventory,
        'annually': calculate_annual_active_inventory
    }
    return inventory_func[timeframe](df, end_date)

def calculate_monthly_active_inventory(df, end_date):
    filtered_df = df[
        (df['listing_date'] < end_date) &
        (df['under_contract_date'].isna() | (df['under_contract_date'] > end_date)) &
        (df['end_of_listing_date'] > end_date)
    ]
    return filtered_df['listing_date'].count()

def calculate_quarterly_active_inventory(df, end_date):
    end_of_quarter = get_last_day_of_quarter(end_date)
    filtered_df = df[
        (df['listing_date'] < end_of_quarter) &
        (df['under_contract_date'].isna() | (df['under_contract_date'] > end_of_quarter)) &
        (df['end_of_listing_date'] > end_of_quarter)
    ]
    return filtered_df['listing_date'].count()

def calculate_annual_active_inventory(df, end_date):
    end_of_year = get_last_day_of_year(end_date)
    filtered_df = df[
        (df['listing_date'] < end_of_year) &
        (df['under_contract_date'].isna() | (df['under_contract_date'] > end_of_year)) &
        (df['end_of_listing_date'] > end_of_year)
    ]
    return filtered_df['listing_date'].count()

def calculate_msi(df, timeframe, start_date, end_date):
    msi_func = {
        'monthly': calculate_monthly_msi,
        'quarterly': calculate_quarterly_msi,
        'annually': calculate_annual_msi
    }
    return msi_func[timeframe](df, start_date, end_date)

def calculate_monthly_msi(df, start_date, end_date):
    monthly_active = calculate_monthly_active_inventory(df, end_date)
    monthly_closed = calculate_monthly_closed_listings(df, start_date, end_date)

    if monthly_closed == 0:
        return None

    return monthly_active / (monthly_closed / 12)

def calculate_quarterly_msi(df, start_date, end_date):
    total_active = 0
    total_closed = 0
    current_date = start_date
    while current_date <= end_date:
        monthly_active = calculate_monthly_active_inventory(df, get_last_day_of_month(current_date))
        monthly_closed = calculate_monthly_closed_listings(df, current_date, get_last_day_of_month(current_date))
        total_active += monthly_active
        total_closed += monthly_closed
        current_date = get_first_day_of_month(current_date) + pd.offsets.MonthEnd()

    if total_closed == 0:
        return None

    return total_active / (total_closed / 12)

def calculate_annual_msi(df, start_date, end_date):
    total_active = 0
    total_closed = 0
    current_date = start_date
    while current_date <= end_date:
        monthly_active = calculate_monthly_active_inventory(df, get_last_day_of_month(current_date))
        monthly_closed = calculate_monthly_closed_listings(df, current_date, get_last_day_of_month(current_date))
        total_active += monthly_active
        total_closed += monthly_closed
        current_date = get_first_day_of_month(current_date) + pd.offsets.MonthEnd()

    if total_closed == 0:
        return None

    return total_active / (total_closed / 12)

def calculate_percent_cash_sales(df, timeframe, start_date, end_date):
    cash_sales_func = {
        'monthly': calculate_monthly_percent_cash_sales,
        'quarterly': calculate_quarterly_percent_cash_sales,
        'annually': calculate_annual_percent_cash_sales
    }
    return cash_sales_func[timeframe](df, start_date, end_date)

def calculate_monthly_percent_cash_sales(df, start_date, end_date):
    filtered_df = df[(df['sold_date'] >= start_date) & (df['sold_date'] <= end_date)]
    cash_sales = filtered_df[filtered_df['terms_of_sale'] == 'cash']['sold_date'].count()
    total_sales = filtered_df['sold_date'].count()

    if total_sales == 0:
        return None

    return (cash_sales / total_sales) * 100

def calculate_quarterly_percent_cash_sales(df, start_date, end_date):
    total_cash_sales = 0
    total_sales = 0
    current_date = start_date
    while current_date <= end_date:
        monthly_cash = calculate_monthly_percent_cash_sales(df, current_date, get_last_day_of_month(current_date))
        if monthly_cash is not None:
            total_cash_sales += monthly_cash * calculate_monthly_closed_listings(df, current_date, get_last_day_of_month(current_date))
            total_sales += calculate_monthly_closed_listings(df, current_date, get_last_day_of_month(current_date))
        current_date = get_first_day_of_month(current_date) + pd.offsets.MonthEnd()

    if total_sales == 0:
        return None

    return total_cash_sales / total_sales

def calculate_annual_percent_cash_sales(df, start_date, end_date):
    total_cash_sales = 0
    total_sales = 0
    current_date = start_date
    while current_date <= end_date:
        monthly_cash = calculate_monthly_percent_cash_sales(df, current_date, get_last_day_of_month(current_date))
        if monthly_cash is not None:
            total_cash_sales += monthly_cash * calculate_monthly_closed_listings(df, current_date, get_last_day_of_month(current_date))
            total_sales += calculate_monthly_closed_listings(df, current_date, get_last_day_of_month(current_date))
        current_date = get_first_day_of_month(current_date) + pd.offsets.MonthEnd()

    if total_sales == 0:
        return None

    return total_cash_sales / total_sales

def calculate_sold_price_per_foot(df, timeframe, start_date, end_date):
    price_per_ft_func = {
        'monthly': calculate_monthly_avg_sold_price_per_foot,
        'quarterly': calculate_quarterly_avg_sold_price_per_foot,
        'annually': calculate_annual_avg_sold_price_per_foot
    }
    return price_per_ft_func[timeframe](df, start_date, end_date)

def calculate_monthly_avg_sold_price_per_foot(df, start_date, end_date):
    filtered_df = df[
        (df['sold_date'] >= start_date) & (df['sold_date'] <= end_date)
    ]
    return filtered_df['sold_price'].div(filtered_df['sqft_living']).mean()

def calculate_quarterly_avg_sold_price_per_foot(df, start_date, end_date):
    total_sold_price = 0
    total_area = 0
    current_date = start_date
    while current_date <= end_date:
        filtered_df = df[
            (df['sold_date'] >= current_date) &
            (df['sold_date'] <= get_last_day_of_quarter(current_date))
        ]
        total_sold_price += filtered_df['sold_price'].sum()
        total_area += filtered_df['sqft_living'].sum()
        current_date = get_first_day_of_quarter(current_date) + pd.offsets.QuarterEnd()

    if total_area == 0:
        return None

    return total_sold_price / total_area

def calculate_annual_avg_sold_price_per_foot(df, start_date, end_date):
    total_sold_price = 0
    total_area = 0
    current_date = start_date
    while current_date <= end_date:
        filtered_df = df[
            (df['sold_date'] >= current_date) &
            (df['sold_date'] <= get_last_day_of_year(current_date))
        ]
        total_sold_price += filtered_df['sold_price'].sum()
        total_area += filtered_df['sqft_living'].sum()
        current_date = get_first_day_of_year(current_date) + pd.offsets.YearEnd()

    if total_area == 0:
        return None

    return total_sold_price / total_area

def analyze_real_estate_data(df, params):
    results = {}

    timeframe = params.get('timeframe')
    start_date = pd.to_datetime(params.get('start_date'))
    end_date = pd.to_datetime(params.get('end_date'))
    stats_to_calculate = params.get('stats_to_calculate')

    function_map = {
        'new_listings': lambda df: calculate_new_listings(df, timeframe, start_date, end_date),
        'closed_listings': lambda df: calculate_closed_listings(df, timeframe, start_date, end_date),
        'avg_sold_price_per_foot': lambda df: calculate_sold_price_per_foot(df, timeframe, start_date, end_date),
        'avg_days_on_market': lambda df: calculate_avg_days_on_market(df, timeframe, start_date, end_date),
        'total_dollar_volume': lambda df: calculate_total_dollar_volume(df, timeframe, start_date, end_date),
        'pending_listings': lambda df: calculate_pending_listings(df, timeframe, end_date),
        'list_price_to_sold_price_ratio': lambda df: calculate_list_price_to_sold_price_ratio(df, timeframe, start_date, end_date),
        'active_inventory': lambda df: calculate_active_inventory(df, timeframe, end_date),
        'msi': lambda df: calculate_msi(df, timeframe, start_date, end_date),
        'percent_cash_sales': lambda df: calculate_percent_cash_sales(df, timeframe, start_date, end_date),
}

    # Execute functions based on selected statistics
    for stat in stats_to_calculate:
        if stat in function_map:
            try:
                results[stat] = function_map[stat](df)
            except Exception as e:
                results[stat] = f"Error calculating {stat}: {str(e)}"
        else:
            results[stat] = "No calculation function defined for this statistic"

    return results
