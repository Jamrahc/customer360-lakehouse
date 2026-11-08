-- SCD Type 2 merge for dwh.dim_customer from dwh.stg_customer.
-- 1) close the current row of every customer whose tracked attributes changed
UPDATE dwh.dim_customer d
SET    valid_to = s.updated_at,
       is_current = FALSE
FROM   dwh.stg_customer s
WHERE  d.customer_id = s.customer_id
  AND  d.is_current
  AND  (d.segment, d.district, d.kyc_risk) IS DISTINCT FROM (s.segment, s.district, s.kyc_risk);

-- 2) insert a new current row for new customers and for the ones just closed
INSERT INTO dwh.dim_customer
       (customer_id, full_name, segment, district, kyc_risk, onboarding_date, valid_from)
SELECT s.customer_id, s.full_name, s.segment, s.district, s.kyc_risk, s.onboarding_date,
       CASE WHEN EXISTS (SELECT 1 FROM dwh.dim_customer h WHERE h.customer_id = s.customer_id)
            THEN s.updated_at ELSE s.onboarding_date::timestamp END
FROM   dwh.stg_customer s
LEFT   JOIN dwh.dim_customer d
       ON d.customer_id = s.customer_id AND d.is_current
WHERE  d.customer_id IS NULL;

-- 3) Type 1 attribute: name corrections overwrite every version
UPDATE dwh.dim_customer d
SET    full_name = s.full_name
FROM   dwh.stg_customer s
WHERE  d.customer_id = s.customer_id AND d.full_name <> s.full_name;
