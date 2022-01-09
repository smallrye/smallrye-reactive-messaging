# Multiple Incoming Channels

!!!warning "Experimental"
    Multiple `@Incomings` is an experimental feature.

The `@Incoming` annotation is repeatable. It means that the method
receives the messages transiting on every listed channels, in no
specific order:

``` java
{{ insert('incomings/IncomingsExamples.java', 'code') }}
```
