# FeedAPI server/client for Go

See [FeedAPI](https://github.com/vippsas/feedapi-spec) for explanation about
what FeedAPI is. This directory contains an implementation
for the HTTP protocol in Go.

## Reading event pages

The v1 and v2 clients accept NDJSON lines up to 8 MiB, including the event
envelope. Larger lines and response read failures return an error. Callers must
not commit events or checkpoints from a failed page: the receiver may already
contain a partial page when the error is returned.

## TODO

This documentation is on the TODO-list currently.
