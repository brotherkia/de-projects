-- 001: make event_hour sortable.
--
-- WHY
-- event_hour was stored in the archive's own URL format, where the hour is
-- NOT zero-padded ('2026-09-07-9'). That format is correct for the URL and
-- must stay that way -- padding it produces a believable URL that 404s, which
-- is a mistake this project already made once and now has a test for.
--
-- The bug was letting that transport detail become the storage key. TEXT
-- sorts lexicographically, so hours came back 0, 1, 10, 11, ..., 19, 2, 20:
--
--     SELECT DISTINCT event_hour FROM gh_event_type_hourly
--      WHERE event_hour LIKE '2026-09-07-%' ORDER BY event_hour;
--     -> 2026-09-07-0, -1, -10, -11, ... -19, -2, -20, ...
--
-- Measured consequence, not a hypothetical: on any COMPLETE day the
-- lexicographic max of hours 0..23 is '9', because '9' > '2' > '10'. So
-- max(event_hour) -- the obvious "what is the latest hour I have loaded?"
-- watermark -- reports 09:00 as the newest hour of a day that runs to 23:00.
-- It read correctly before this migration only by luck: the data happened to
-- stop at hour 3, before the strings could disagree with the clock.
--
-- Equality was never affected, which is why nothing looked broken. The loads
-- match with WHERE event_hour = %s, so delete-then-insert and the idempotency
-- property held the whole time. What was broken is ordering, BETWEEN range
-- scans, and every window function -- i.e. exactly the hour-over-hour rollups
-- this table exists to support.
--
-- FIX
-- Zero-pad the stored key to '2026-09-07-09'. Padded, the text sorts
-- chronologically, so ORDER BY is correct again with no type change -- which
-- matters because these same functions run against SQLite in the tests and
-- Postgres in the containers, and a timestamptz column does not exist in both.
--
-- Pure string surgery, deliberately: no to_timestamp(), so the result cannot
-- depend on the session's TimeZone setting.
--
-- Idempotent, like everything else here. '2026-09-07-9' is 12 characters and
-- '2026-09-07-10' is 13, so the length filter only ever matches an unpadded
-- row. Running this twice changes nothing the second time.
--
-- ORDER OF OPERATIONS MATTERS. Apply this while the DAG is PAUSED, together
-- with the code change that writes padded keys. If new padded rows land while
-- old unpadded rows are still present, the same hour exists under two keys --
-- and load_partition's DELETE WHERE event_hour = '2026-09-07-09' will not
-- match '2026-09-07-9', so that hour is counted twice. That is the silent
-- double-count this pipeline's whole idempotency design exists to prevent.

BEGIN;

-- Pre-flight guard. This ABORTS the transaction rather than reporting a
-- number nobody reads: if padded rows already exist alongside unpadded ones,
-- the UPDATE below would collide with the primary key, and the right response
-- is to stop and reconcile the duplicated hour by hand.
DO $$
DECLARE
    n bigint;
BEGIN
    SELECT count(*) INTO n
      FROM gh_event_type_hourly a
      JOIN gh_event_type_hourly b
        ON b.event_hour = substring(a.event_hour from 1 for 11)
                       || lpad(substring(a.event_hour from 12), 2, '0')
       AND b.event_type = a.event_type
     WHERE length(a.event_hour) = 12;
    IF n > 0 THEN
        RAISE EXCEPTION
          'aborting: % event-type rows already exist under BOTH the padded and '
          'unpadded key. That hour is double-counted -- reconcile it before migrating.', n;
    END IF;

    SELECT count(*) INTO n
      FROM gh_repo_activity_hourly a
      JOIN gh_repo_activity_hourly b
        ON b.event_hour = substring(a.event_hour from 1 for 11)
                       || lpad(substring(a.event_hour from 12), 2, '0')
       AND b.repo_name = a.repo_name
     WHERE length(a.event_hour) = 12;
    IF n > 0 THEN
        RAISE EXCEPTION
          'aborting: % repo-activity rows already exist under BOTH keys.', n;
    END IF;
END $$;

UPDATE gh_event_type_hourly
   SET event_hour = substring(event_hour from 1 for 11)
                 || lpad(substring(event_hour from 12), 2, '0')
 WHERE length(event_hour) = 12;

UPDATE gh_repo_activity_hourly
   SET event_hour = substring(event_hour from 1 for 11)
                 || lpad(substring(event_hour from 12), 2, '0')
 WHERE length(event_hour) = 12;

-- Verification: every remaining key must now be 13 characters. Anything else
-- is a malformed key that predates this migration and needs looking at.
SELECT length(event_hour) AS key_len, count(*) AS rows
  FROM gh_event_type_hourly GROUP BY 1 ORDER BY 1;

COMMIT;
