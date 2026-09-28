# IRCCloud Keepalive

Runs in GitHub Actions and sends "hi" to CoachHardy on the Undernet connection whenever at least 100 minutes have elapsed since the previous successful keepalive.

The workflow wakes every 5 minutes because GitHub scheduled workflows are not guaranteed to execute at the exact scheduled minute. The Node program itself enforces the 100-minute interval.

Secrets required:
- IRCLOUD_EMAIL
- IRCLOUD_PASSWORD

The implementation talks to IRCCloud's WebSocket/RPC protocol directly and does not depend on the old node-irccloud package.
