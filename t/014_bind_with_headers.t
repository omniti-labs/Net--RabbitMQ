use Test::More 'no_plan'; #  19;
use strict;

my $host = $ENV{'MQHOST'} || "localhost";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq, "Created object");

SKIP: {
    skip "Ignore", 17;

    eval { $mq->connect($host, { user => "guest", password => "guest" }); };
    is($@, '', "connect");

    eval { $mq->channel_open(1); };
    is($@, '', "channel_open");

    my $delete = 1;
    my $key = 'key';
    my $queue;
    eval { $queue = $mq->queue_declare(1, "", { auto_delete => $delete } ); };
    is($@, '', "queue_declare");

    diag "Using queue $queue";

    my $exchange = "x-$queue";
    eval { $mq->exchange_declare( 1, $exchange, { exchange_type => 'headers', auto_delete => $delete } ); };
    is($@, '', "exchange_declare");

    my $headers = { foo => 'bar' };
    eval { $mq->queue_bind( 1, $queue, $exchange, $key, $headers ) };
    is( $@, '', "queue_bind" );

    # This message doesn't have the correct headers so will not be routed to the queue
    eval { $mq->publish( 1, $key, "Unroutable", { exchange => $exchange } ) };
    is( $@, '', "publish unroutable message" );

    eval { $mq->publish( 1, $key, "Routable", { exchange => $exchange }, { headers => $headers} ) };
    is( $@, '', "publish routable message" );

    eval { $mq->consume( 1, $queue ) };
    is( $@, '', "consume" );

    my $msg;
    eval { $msg = $mq->recv() };
    is( $@, '', "recv" );
    is( $msg->{body}, "Routable", "Got expected message" );

    eval { $mq->queue_unbind( 1, $queue, $exchange, $key, $headers ) };
    is( $@, '', "queue_unbind" );
}
