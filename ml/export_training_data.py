#!/usr/bin/env python3
"""
Export training data from D1 database using wrangler CLI.
This bypasses API authentication for local model training.
"""

import json
import subprocess
import sys

def run_d1_query(sql: str) -> list:
    """Execute D1 query via wrangler and return results."""
    cmd = ['npx', 'wrangler', 'd1', 'execute', 'pxi-db', '--remote', '--command', sql, '--json']
    result = subprocess.run(cmd, capture_output=True, text=True, cwd='/Users/scott/pxi/worker')

    if result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
        return []

    data = json.loads(result.stdout)
    return data[0]['results'] if data and 'results' in data[0] else []


def main():
    print("Exporting training data from D1...")

    # Get PXI scores
    print("  Fetching PXI scores...")
    pxi_data = run_d1_query("""
        SELECT date, score as pxi_score, delta_1d, delta_7d, delta_30d
        FROM pxi_scores ORDER BY date ASC
    """)
    print(f"    Got {len(pxi_data)} PXI records")

    # Get category scores
    print("  Fetching category scores...")
    category_data = run_d1_query("""
        SELECT date, category, score
        FROM category_scores ORDER BY date ASC
    """)
    print(f"    Got {len(category_data)} category records")

    # Get SPY prices
    print("  Fetching SPY prices...")
    spy_data = run_d1_query("""
        SELECT date, value
        FROM indicator_values
        WHERE indicator_id = 'spy_close'
        ORDER BY date ASC
    """)
    print(f"    Got {len(spy_data)} SPY price records")

    # Get key indicators
    print("  Fetching indicators...")
    indicators = ['vix', 'hy_oas', 'ig_oas', 'rsp_spy_ratio', 'yield_curve_2s10s', 'dxy', 'btc_vs_200d']
    indicator_data = {}
    for ind in indicators:
        data = run_d1_query(f"""
            SELECT date, value
            FROM indicator_values
            WHERE indicator_id = '{ind}'
            ORDER BY date ASC
        """)
        indicator_data[ind] = {r['date']: r['value'] for r in data}
        print(f"    {ind}: {len(data)} records")

    # Build category lookup
    category_lookup = {}
    for r in category_data:
        if r['date'] not in category_lookup:
            category_lookup[r['date']] = {}
        category_lookup[r['date']][r['category']] = r['score']

    # Build SPY price lookup
    spy_lookup = {r['date']: r['value'] for r in spy_data}

    # Calculate SPY returns
    def get_spy_price(date_str: str, max_days: int = 5):
        from datetime import datetime, timedelta
        date = datetime.strptime(date_str, '%Y-%m-%d')
        for i in range(max_days + 1):
            check_date = (date + timedelta(days=i)).strftime('%Y-%m-%d')
            if check_date in spy_lookup:
                return spy_lookup[check_date]
        return None

    # Combine into training records
    print("  Building training records...")
    records = []
    spy_return_count = 0

    for row in pxi_data:
        date = row['date']

        # Get SPY prices for return calculation
        today_price = get_spy_price(date)
        price_7d = get_spy_price(date, max_days=12) if today_price else None  # Look 7 days ahead + buffer
        price_30d = None

        # Calculate 7-day forward lookup date
        if today_price:
            from datetime import datetime, timedelta
            d = datetime.strptime(date, '%Y-%m-%d')
            date_7d = (d + timedelta(days=7)).strftime('%Y-%m-%d')
            date_30d = (d + timedelta(days=30)).strftime('%Y-%m-%d')
            price_7d = get_spy_price(date_7d)
            price_30d = get_spy_price(date_30d)

        spy_return_7d = None
        spy_return_30d = None

        if today_price and price_7d:
            spy_return_7d = ((price_7d - today_price) / today_price) * 100
            spy_return_count += 1
        if today_price and price_30d:
            spy_return_30d = ((price_30d - today_price) / today_price) * 100

        record = {
            'date': date,
            'pxi_score': row['pxi_score'],
            'pxi_delta_1d': row.get('delta_1d'),
            'pxi_delta_7d': row.get('delta_7d'),
            'pxi_delta_30d': row.get('delta_30d'),
            'spy_return_7d': spy_return_7d,
            'spy_return_30d': spy_return_30d,
            'categories': category_lookup.get(date, {}),
            'indicators': {
                ind: indicator_data[ind].get(date)
                for ind in indicators
            }
        }
        records.append(record)

    print(f"  Built {len(records)} training records with {spy_return_count} SPY return calculations")

    # Save to file
    output = {
        'count': len(records),
        'spy_data_points': spy_return_count,
        'data': records
    }

    output_path = '/Users/scott/pxi/ml/training_data.json'
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nExported to: {output_path}")
    print(f"File size: {len(json.dumps(output)) / 1024:.1f} KB")


if __name__ == '__main__':
    main()
