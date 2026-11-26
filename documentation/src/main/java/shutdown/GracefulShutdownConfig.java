package shutdown;

// <incoming>
// Enable graceful shutdown on an incoming channel
// mp.messaging.incoming.my-channel.graceful-shutdown=true
// mp.messaging.incoming.my-channel.graceful-shutdown.drain-timeout=15000
// </incoming>

// <outgoing>
// Enable graceful shutdown on an outgoing channel
// mp.messaging.outgoing.my-channel.graceful-shutdown=true
// mp.messaging.outgoing.my-channel.graceful-shutdown.drain-timeout=5000
// </outgoing>
