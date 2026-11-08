# Superset dashboards

The warehouse connection **Customer 360 Warehouse** is created automatically. In Superset:
**Datasets → + Dataset**, pick the `dwh` schema, and add the tables below. Then build the charts
and arrange them on one dashboard called **Customer 360**.

## Datasets

| Dataset | Use |
| --- | --- |
| `dwh.mart_kpi_daily` | Daily trends |
| `dwh.mart_customer_360` | Customer behaviour and segmentation |
| `dwh.mart_portfolio_quality` | Loan book quality |
| `dwh.customer_churn_scores` | Churn risk |
| `dwh.dq_results` | Data quality status |

## Charts

| # | Chart | Type | Dataset | Settings |
| --- | --- | --- | --- | --- |
| 1 | Transactions per day | Line | mart_kpi_daily | X: `kpi_date`, metric: `SUM(transactions)` |
| 2 | Digital share of transactions | Line | mart_kpi_daily | metric: `AVG(digital_share)`, format as % |
| 3 | App daily active users | Line | mart_kpi_daily | metric: `SUM(app_daily_active_users)` |
| 4 | Complaints vs accounts closed | Mixed / two lines | mart_kpi_daily | `SUM(complaints_opened)`, `SUM(accounts_closed)` |
| 5 | Customers by engagement status | Bar | mart_customer_360 | dimension: `engagement_status`, metric: `COUNT(*)` |
| 6 | Deposit balance by value tier and segment | Stacked bar | mart_customer_360 | `value_tier` × `segment`, `SUM(deposit_balance)` |
| 7 | Loan book by IFRS 9 stage | Bar | mart_portfolio_quality | `ifrs9_stage`, `SUM(outstanding)` |
| 8 | DPD buckets by product | Heatmap | mart_portfolio_quality | `product` × `dpd_bucket`, `SUM(outstanding)` |
| 9 | Churn risk bands | Big number + bar | customer_churn_scores | `risk_band`, `COUNT(*)` |
| 10 | High-risk customers to contact | Table | SQL Lab query below | sort by score |
| 11 | Data quality checks | Table | dq_results | colour `passed` red/green |

**Query for chart 10** (save it as a virtual dataset from SQL Lab):

```sql
SELECT c.customer_id, c.full_name, c.segment, c.value_tier,
       s.churn_score, c.complaints_90d, c.days_since_last_txn, c.app_sessions_30d
FROM dwh.customer_churn_scores s
JOIN dwh.mart_customer_360 c USING (customer_id)
WHERE s.scored_on = (SELECT max(scored_on) FROM dwh.customer_churn_scores)
  AND s.risk_band = 'High'
ORDER BY s.churn_score DESC
LIMIT 50;
```

Add native filters for `segment`, `district` and `value_tier`. Export the dashboard
(**⋯ → Download as image**) and add the screenshot to the README.
