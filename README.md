# KAFKA TIMER (Redis module)

This module provides an ability to send delayed messages to Kafka using basic Redis data structures such as Sorted set and Hash map.

One and only command:
```
KTIMER.ADD delay-seconds topic key value [header-key header-value]
```

Kafka headers are passed as optional key/value pairs:
```
KTIMER.ADD 10 my-topic hello world header1 test header2 something
```

# Architecture
TODO