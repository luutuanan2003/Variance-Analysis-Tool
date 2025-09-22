#!/usr/bin/env python3
"""
Comprehensive Revenue Impact Analysis
Answers specific questions:
1. If revenue increases (511*), which specific revenue accounts drive the increase?
2. Which customers/entities drive the revenue changes for each account?
3. Gross margin analysis: (Revenue - Cost)/Revenue and risk identification
4. Utility revenue vs cost pairing analysis
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings("ignore")

def clean_numeric_value(val):
    """Convert value to numeric, handling various formats"""
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    # Handle string numbers
    try:
        return float(str(val).replace(',', '').replace(' ', ''))
    except:
        return 0.0

def analyze_revenue_impact(file_path: str) -> Dict:
    """
    Comprehensive revenue impact analysis
    """
    try:
        xls = pd.ExcelFile(file_path)
        pl_df = pd.read_excel(xls, sheet_name='PL Breakdown')

        # Find data start row
        data_start_row = None
        for i, row in pl_df.iterrows():
            if str(row.iloc[1]).strip().lower() == 'entity':
                data_start_row = i
                break

        if data_start_row is None:
            print("Could not find data start row")
            return {}

        # Extract headers
        headers = pl_df.iloc[data_start_row].fillna('').astype(str).tolist()
        month_headers = pl_df.iloc[data_start_row + 1].fillna('').astype(str).tolist()

        # Get month columns
        month_cols = []
        for i, h2 in enumerate(month_headers):
            if any(month in str(h2) for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug']):
                month_cols.append(str(h2).strip())

        # Extract data
        data_df = pl_df.iloc[data_start_row + 2:].copy()

        # Create column mapping based on actual number of columns
        actual_col_count = len(data_df.columns)
        new_columns = ['Account_Description', 'Entity', 'Account_Code']
        new_columns.extend(month_cols)

        # Add extra columns if needed
        while len(new_columns) < actual_col_count:
            new_columns.append(f'Extra_{len(new_columns)}')

        # Ensure we don't exceed actual column count
        data_df.columns = new_columns[:actual_col_count]
        data_df = data_df.dropna(how='all')

        print("=== COMPREHENSIVE REVENUE IMPACT ANALYSIS ===\n")
        print(f"Analyzing months: {month_cols[:8]}\n")

        # =====================================
        # 1. TOTAL REVENUE ANALYSIS (511*)
        # =====================================

        print("1. TOTAL REVENUE (511*) TREND ANALYSIS")
        print("=" * 50)

        # Calculate total revenue by month
        total_revenue_by_month = {}

        for month in month_cols[:8]:
            month_total = 0
            for i, row in data_df.iterrows():
                entity = str(row['Entity']) if 'Entity' in row and pd.notna(row['Entity']) else ''
                if entity and entity != 'nan' and not entity.startswith('Total'):
                    # Check if this is a 511* revenue row
                    for prev_i in range(max(0, i-10), i):
                        if prev_i < len(data_df):
                            prev_desc = str(data_df.iloc[prev_i]['Account_Description']) if pd.notna(data_df.iloc[prev_i]['Account_Description']) else ''
                            if '511' in prev_desc and 'revenue' in prev_desc.lower():
                                val = clean_numeric_value(row[month])
                                month_total += val
                                break
            total_revenue_by_month[month] = month_total

        # Calculate month-over-month changes
        months = list(total_revenue_by_month.keys())
        for i in range(len(months)):
            month = months[i]
            revenue = total_revenue_by_month[month]
            if i > 0:
                prev_month = months[i-1]
                prev_revenue = total_revenue_by_month[prev_month]
                change = revenue - prev_revenue
                pct_change = (change / prev_revenue * 100) if prev_revenue != 0 else 0
                print(f"{prev_month} → {month}: {prev_revenue:,.0f} → {revenue:,.0f} VND")
                print(f"  Change: {change:+,.0f} VND ({pct_change:+.1f}%)")
            else:
                print(f"{month}: {revenue:,.0f} VND (baseline)")
        print()

        # =====================================
        # 2. REVENUE BY ACCOUNT TYPE (511.xxx)
        # =====================================

        print("2. REVENUE BREAKDOWN BY ACCOUNT TYPE")
        print("=" * 50)

        revenue_accounts = {}
        current_account = None

        for i, row in data_df.iterrows():
            account_desc = str(row['Account_Description']) if pd.notna(row['Account_Description']) else ''
            entity = str(row['Entity']) if pd.notna(row['Entity']) else ''

            # Identify revenue account headers (511*)
            if '511' in account_desc and 'revenue' in account_desc.lower():
                current_account = account_desc
                if current_account not in revenue_accounts:
                    revenue_accounts[current_account] = {
                        'entities': {},
                        'monthly_totals': {month: 0 for month in month_cols[:8]}
                    }

            # Collect entity data under current account
            elif current_account and entity and entity != 'nan' and not entity.startswith('Total'):
                if entity not in revenue_accounts[current_account]['entities']:
                    revenue_accounts[current_account]['entities'][entity] = {}

                for month in month_cols[:8]:
                    val = clean_numeric_value(row[month])
                    revenue_accounts[current_account]['entities'][entity][month] = val
                    revenue_accounts[current_account]['monthly_totals'][month] += val

        # Analyze each revenue account
        for account, data in revenue_accounts.items():
            print(f"\nAccount: {account}")
            print("-" * 80)

            # Calculate month-over-month for this account
            months = list(data['monthly_totals'].keys())
            account_changes = []

            for i in range(1, len(months)):
                prev_month = months[i-1]
                curr_month = months[i]
                prev_val = data['monthly_totals'][prev_month]
                curr_val = data['monthly_totals'][curr_month]
                change = curr_val - prev_val
                pct_change = (change / prev_val * 100) if prev_val != 0 else 0

                account_changes.append({
                    'from': prev_month,
                    'to': curr_month,
                    'change': change,
                    'pct_change': pct_change,
                    'prev_val': prev_val,
                    'curr_val': curr_val
                })

                print(f"  {prev_month} → {curr_month}: {change:+,.0f} VND ({pct_change:+.1f}%)")

            # Find biggest change period for detailed customer analysis
            if account_changes:
                biggest_change = max(account_changes, key=lambda x: abs(x['change']))
                if abs(biggest_change['change']) > 1000000:  # > 1M VND change
                    print(f"\n  📊 CUSTOMER BREAKDOWN for biggest change ({biggest_change['from']} → {biggest_change['to']}):")

                    customer_impacts = []
                    for entity, entity_data in data['entities'].items():
                        prev_val = entity_data.get(biggest_change['from'], 0)
                        curr_val = entity_data.get(biggest_change['to'], 0)
                        entity_change = curr_val - prev_val

                        if abs(entity_change) > 100000:  # > 100K VND
                            customer_impacts.append({
                                'entity': entity,
                                'change': entity_change,
                                'prev_val': prev_val,
                                'curr_val': curr_val
                            })

                    # Sort by absolute impact
                    customer_impacts.sort(key=lambda x: abs(x['change']), reverse=True)

                    for impact in customer_impacts[:5]:  # Top 5 customers
                        pct = (impact['change'] / impact['prev_val'] * 100) if impact['prev_val'] != 0 else 0
                        print(f"    • {impact['entity']}: {impact['change']:+,.0f} VND ({pct:+.1f}%)")
                        print(f"      {impact['prev_val']:,.0f} → {impact['curr_val']:,.0f}")

        # =====================================
        # 3. GROSS MARGIN ANALYSIS
        # =====================================

        print("\n\n3. GROSS MARGIN ANALYSIS")
        print("=" * 50)

        # Read BS data for cost accounts (632*)
        try:
            bs_df = pd.read_excel(xls, sheet_name='BS Breakdown')

            # Find cost data (632* accounts) - these might be in PL or BS
            cost_accounts = {}
            current_cost_account = None

            # Look for 632* in PL data first
            for i, row in data_df.iterrows():
                account_desc = str(row['Account_Description']) if pd.notna(row['Account_Description']) else ''
                entity = str(row['Entity']) if pd.notna(row['Entity']) else ''

                if '632' in account_desc and 'cost' in account_desc.lower():
                    current_cost_account = account_desc
                    if current_cost_account not in cost_accounts:
                        cost_accounts[current_cost_account] = {
                            'entities': {},
                            'monthly_totals': {month: 0 for month in month_cols[:8]}
                        }

                elif current_cost_account and entity and entity != 'nan' and not entity.startswith('Total'):
                    if entity not in cost_accounts[current_cost_account]['entities']:
                        cost_accounts[current_cost_account]['entities'][entity] = {}

                    for month in month_cols[:8]:
                        val = clean_numeric_value(row[month])
                        cost_accounts[current_cost_account]['entities'][entity][month] = val
                        cost_accounts[current_cost_account]['monthly_totals'][month] += val

            # Calculate gross margins
            print("Overall Gross Margin Trend:")
            for i in range(len(months)):
                month = months[i]
                total_revenue = total_revenue_by_month[month]
                total_cost = sum([cost_data['monthly_totals'][month] for cost_data in cost_accounts.values()])

                if total_revenue > 0:
                    gross_margin_pct = ((total_revenue - total_cost) / total_revenue) * 100
                    print(f"  {month}: Revenue {total_revenue:,.0f} - Cost {total_cost:,.0f} = GM {gross_margin_pct:.1f}%")

                    if i > 0:
                        prev_month = months[i-1]
                        prev_total_revenue = total_revenue_by_month[prev_month]
                        prev_total_cost = sum([cost_data['monthly_totals'][prev_month] for cost_data in cost_accounts.values()])
                        prev_gm_pct = ((prev_total_revenue - prev_total_cost) / prev_total_revenue) * 100 if prev_total_revenue > 0 else 0
                        gm_change = gross_margin_pct - prev_gm_pct

                        if abs(gm_change) > 1:  # > 1% change
                            print(f"    ⚠️  Gross Margin changed by {gm_change:+.1f}% from previous month")
                            if gm_change < -2:
                                print(f"    🔴 RISK: Significant GM decrease - potential uncharged revenue risk")

            # Utility-specific analysis
            print(f"\n📊 UTILITY REVENUE vs COST ANALYSIS:")

            utility_revenue = None
            utility_cost = None

            # Find utility revenue account
            for account, data in revenue_accounts.items():
                if 'utilit' in account.lower():
                    utility_revenue = data
                    print(f"  Utility Revenue Account: {account}")
                    break

            # Find utility cost account
            for account, data in cost_accounts.items():
                if 'utilit' in account.lower():
                    utility_cost = data
                    print(f"  Utility Cost Account: {account}")
                    break

            if utility_revenue and utility_cost:
                print(f"  Utility Gross Margin by Month:")
                for month in months:
                    rev = utility_revenue['monthly_totals'][month]
                    cost = utility_cost['monthly_totals'][month]
                    if rev > 0:
                        gm_pct = ((rev - cost) / rev) * 100
                        print(f"    {month}: Revenue {rev:,.0f} - Cost {cost:,.0f} = GM {gm_pct:.1f}%")
            else:
                print(f"  ⚠️  Could not find matching utility revenue/cost accounts")

        except Exception as e:
            print(f"Note: Could not perform full gross margin analysis: {e}")

        print(f"\n=== ANALYSIS COMPLETE ===")

        return {
            'total_revenue_trend': total_revenue_by_month,
            'revenue_accounts': revenue_accounts,
            'cost_accounts': cost_accounts if 'cost_accounts' in locals() else {}
        }

    except Exception as e:
        print(f"Error in analysis: {e}")
        import traceback
        traceback.print_exc()
        return {}

if __name__ == "__main__":
    result = analyze_revenue_impact('./sample data/BHA - Combined.xlsx')