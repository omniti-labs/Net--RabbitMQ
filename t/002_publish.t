use Test::More tests => 7;
use strict;

my $host = $ENV{'MQHOST'} || "localhost";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

eval { $mq->connect($host, { user => "guest", pass => "guest" }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
eval { $mq->queue_declare(1, "nr_test_hole", 0, 1, 0, 0); };
is($@, '', "queue_declare");
eval { $mq->queue_bind(1, "nr_test_hole", "nr_test_x", "nr_test_route"); };
is($@, '', "queue_bind");
eval { $mq->publish(1, "nr_test_route", "Magic Payload", { exchange => "nr_test_x" }); };
is($@, '', "publish");

1;
