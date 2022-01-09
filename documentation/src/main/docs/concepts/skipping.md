# Skipping messages

Sometimes you receive a message and don’t want to produce an output
message. To handle this, you have several choices:

1.  for method processing single message or payload, producing `null`
    would produce an ignored message (not forwarded)
2.  for method processing streams, you can generate an *empty* stream.

## Skipping a single item

To skip a single message or payload, return `null`:

``` java
{{ insert('skip/SingleSkip.java', 'skip') }}
```

## Skipping in a stream

To skip a message or payload when manipulating a stream, emit an *empty* `Multi` (or `Publisher`):

``` java
{{ insert('skip/StreamSkip.java', 'skip') }}
```
