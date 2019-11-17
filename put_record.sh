#/bin/sh

aws kinesis put-record \
--stream-name $(pulumi stack output streamName) \
--region $(pulumi config get aws:region) \
--data '{"id": "111114", "event_type": "impression", "message": "ad was rendered"}' \
--partition-key '111114'
