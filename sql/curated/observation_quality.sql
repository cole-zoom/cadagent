CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.quality.observation_quality` (
    observation_id      STRING NOT NULL,
    quality_confidence  FLOAT64,
    issue_codes         STRING,
    issue_notes         STRING,
    review_status       STRING DEFAULT 'unreviewed',
    reviewed_by         STRING,
    reviewed_at         TIMESTAMP,
)
CLUSTER BY review_status
OPTIONS (
    description = 'Quality and review metadata for fact observations. Used for debugging, manual review, and agent trust scoring.'
);
