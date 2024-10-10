-- Step 1: Join both tables on call_id
WITH combined_data AS (
    SELECT 
        c.call_id,
        c.customer_id,
        c.agent_id,
        c.call_start_datetime,
        c.agent_assigned_datetime,
        c.call_end_datetime,
        r.primary_call_reason,
        c.call_transcript
    FROM callsf0d4f5a c
    LEFT JOIN reason18315ff r
    ON c.call_id = r.call_id
),

-- Step 2: Calculate call duration and agent response time
call_duration AS (
    SELECT 
        call_id,
        customer_id,
        agent_id,
        primary_call_reason,
        call_transcript,
        TIMESTAMPDIFF(MINUTE, call_start_datetime, call_end_datetime) AS call_duration_minutes,
        TIMESTAMPDIFF(MINUTE, call_start_datetime, agent_assigned_datetime) AS agent_response_time_minutes
    FROM combined_data
),

-- Step 3: Analyze call reasons
call_reason_analysis AS (
    SELECT 
        primary_call_reason,
        COUNT(*) AS total_calls,
        AVG(call_duration_minutes) AS avg_call_duration,
        AVG(agent_response_time_minutes) AS avg_agent_response_time
    FROM call_duration
    GROUP BY primary_call_reason
),

-- Step 4: Rank agents based on number of calls handled and average call duration
agent_performance AS (
    SELECT 
        agent_id,
        COUNT(call_id) AS total_calls_handled,
        AVG(call_duration_minutes) AS avg_call_duration,
        DENSE_RANK() OVER (ORDER BY COUNT(call_id) DESC) AS call_rank,
        DENSE_RANK() OVER (ORDER BY AVG(call_duration_minutes) ASC) AS efficiency_rank
    FROM call_duration
    GROUP BY agent_id
)

-- Step 5: Final query to show insights
SELECT 
    cr.primary_call_reason,
    cr.total_calls,
    cr.avg_call_duration,
    cr.avg_agent_response_time,
    ap.agent_id,
    ap.total_calls_handled,
    ap.avg_call_duration AS agent_avg_call_duration,
    ap.call_rank,
    ap.efficiency_rank
FROM call_reason_analysis cr
JOIN agent_performance ap
ON ap.agent_id IN (SELECT agent_id FROM call_duration WHERE primary_call_reason = cr.primary_call_reason)
ORDER BY cr.total_calls DESC, ap.call_rank;



-- 1. Join Sentiment and Customer Data based on Customer and Call ID.
SELECT
    s.call_id,
    c.customer_name,
    c.elite_level_code,
    s.agent_tone,
    s.customer_tone,
    s.average_sentiment,
    s.silence_percent_average
FROM
    sentiment_statistics s
JOIN
    customers c
ON
    s.call_id = c.customer_id;

-- 2. Calculate Average Sentiment and Silence by Elite Level using Window Function.
SELECT
    c.elite_level_code,
    AVG(s.average_sentiment) OVER (PARTITION BY c.elite_level_code) AS avg_sentiment,
    AVG(s.silence_percent_average) OVER (PARTITION BY c.elite_level_code) AS avg_silence
FROM
    sentiment_statistics s
JOIN
    customers c
ON
    s.call_id = c.customer_id
ORDER BY
    c.elite_level_code;

-- 3. Identify Calls with High Silence Percentage (>30%) and Negative Sentiment (<0).
SELECT
    s.call_id,
    c.customer_name,
    s.average_sentiment,
    s.silence_percent_average
FROM
    sentiment_statistics s
JOIN
    customers c
ON
    s.call_id = c.customer_id
WHERE
    s.silence_percent_average > 0.30
    AND s.average_sentiment < 0
ORDER BY
    s.silence_percent_average DESC;

-- 4. Rank Customers by Elite Level and Sentiment.
SELECT
    c.customer_name,
    c.elite_level_code,
    s.average_sentiment,
    RANK() OVER (PARTITION BY c.elite_level_code ORDER BY s.average_sentiment DESC) AS sentiment_rank
FROM
    sentiment_statistics s
JOIN
    customers c
ON
    s.call_id = c.customer_id
ORDER BY
    c.elite_level_code, sentiment_rank;

-- 5. Calculate Overall Sentiment and Silence Metrics for Each Agent.
SELECT
    s.agent_id,
    AVG(s.average_sentiment) AS avg_sentiment,
    AVG(s.silence_percent_average) AS avg_silence,
    COUNT(s.call_id) AS total_calls
FROM
    sentiment_statistics s
GROUP BY
    s.agent_id
ORDER BY
    avg_sentiment DESC;
