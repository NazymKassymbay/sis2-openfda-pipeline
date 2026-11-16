-- SQL verification queries for data/output.db
-- Run manually:  sqlite3 data/output.db < sql/verification_queries.sql
-- The loader executes the same queries (split on the '-- name:' markers).

-- name: total_reports
SELECT COUNT(*) AS total_reports FROM adverse_events;

-- name: distinct_ids
SELECT COUNT(DISTINCT safety_report_id) AS distinct_ids FROM adverse_events;

-- name: duplicate_ids
SELECT safety_report_id, COUNT(*) AS n FROM adverse_events
    GROUP BY safety_report_id HAVING COUNT(*) > 1;

-- name: null_required_fields
SELECT SUM(receive_date IS NULL) AS null_receive_date,
    SUM(serious_label IS NULL) AS null_serious,
    SUM(primary_suspect_drug IS NULL) AS null_drug
    FROM adverse_events;

-- name: orphan_reactions
SELECT COUNT(*) AS orphan_reactions FROM reactions r
    LEFT JOIN adverse_events e USING (safety_report_id)
    WHERE e.safety_report_id IS NULL;

-- name: date_range
SELECT MIN(receive_date) AS first_received, MAX(receive_date) AS last_received
    FROM adverse_events;

-- name: seriousness_split
SELECT serious_label, COUNT(*) AS reports,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM adverse_events), 1) AS pct
    FROM adverse_events GROUP BY serious_label;

-- name: top_suspect_drugs
SELECT primary_suspect_drug, COUNT(*) AS reports, SUM(is_serious) AS serious
    FROM adverse_events WHERE primary_suspect_drug <> 'UNKNOWN'
    GROUP BY primary_suspect_drug ORDER BY reports DESC LIMIT 10;

-- name: top_reactions
SELECT reaction_pt, COUNT(*) AS reports FROM reactions
    GROUP BY reaction_pt ORDER BY reports DESC LIMIT 10;

-- name: load_history
SELECT * FROM load_runs ORDER BY run_no;

