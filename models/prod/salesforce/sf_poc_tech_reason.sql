with sf_poc as (
    select * from {{ ref('sf_poc')}}
),

split_values AS (
    SELECT 
        POC_DATE__C,
        POC_START_DATE__C,
        END_DATE__C,
        OPPORTUNITY_NAME,
        OPPORTUNITY_OWNER,
        STAGENAME,
        POC_NAME,
        START_DATE__C,
        CLOSE_DATE,
        ANNUAL_AMOUNT,
        POC_W_COMPETITORS,
        SILENT_OR_LIVE_POV__C,
        TECHNICAL_WIN__C,
        LOSS_REASON__C,
        LOST_TO_COMPETITOR__C,
        REASON_FOR_TECHNICAL_LOSS__C,
        CLOSED_LOST_DETAILS__C,
        TRIM(f.value::STRING) as split_reason
    FROM 
        sf_poc,
        TABLE(FLATTEN(SPLIT(TECHNICAL_LOSS_REASONS__C, ';'))) f
    WHERE 
        TECHNICAL_LOSS_REASONS__C IS NOT NULL
)
SELECT 
    *
FROM 
    split_values
WHERE 
    split_reason IS NOT NULL
    AND split_reason != ''
ORDER BY 
    OPPORTUNITY_NAME,
    split_reason