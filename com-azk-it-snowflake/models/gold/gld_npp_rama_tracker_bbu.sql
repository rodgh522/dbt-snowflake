{{ config(
    materialized='table',
    alias='npp_rama_tracker_bbu'
) }}

SELECT
    ta,
    brand,
    study_name,
    indication,
    current_status,
    ra_category,
    gift_odd,
    pnr_track,
    ra_submission_base,
    ra_approval_base,
    pnr_submission_base,
    reim_launch_best,
    reim_launch_base,
    ra_remarks,
    pnr_remarks,
    ra_lead,
    pnr_lead,
    country_submission_1st,
    country_approval_1st,
    pivotal_study,
    need_for_korean_bridging_purpose,
    required_for_reg_submission,
    dedup_seq
FROM {{ ref('npp_rama_tracker_bbu') }}

