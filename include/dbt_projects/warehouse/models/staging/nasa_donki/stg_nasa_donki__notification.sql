-- Space Weather Research Center notifications — typed 1:1 view over
-- raw.notification.
--
-- These are the human-written bulletins issued about the events in the other
-- resources, not events themselves. message_body is free text and can be long.
--
-- The pipeline must pass `type=all` to this endpoint; without it DONKI returns
-- nothing at all.

select
    message_id,
    message_type,
    message_issue_time,
    message_url,
    message_body,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_donki', 'notification') }}
