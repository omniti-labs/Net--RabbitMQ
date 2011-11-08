use Test::More 'no_plan'; #  20;
use strict;

my $host = $ENV{'MQHOST'} || "dev.rabbitmq.com";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq, "Created object");

eval { $mq->connect($host, { user => "guest", password => "guest" }); };
is($@, '', "connect");

eval { $mq->channel_open(1); };
is($@, '', "channel_open");

my $delete = 1;
my $queue = "x-headers-" . rand();

eval { $queue = $mq->queue_declare(1, $queue, { auto_delete => $delete }, { "x-ha-policy" => "all" }); };
is($@, '', "queue_declare");

eval { $queue = $mq->queue_declare(1, $queue, { auto_delete => $delete }); };
like( $@, qr/PRECONDITION_FAILED/, "Redeclaring queue without header arguments fails." );
