use Test::More tests => 6;
use strict;

my $host = $ENV{'MQHOST'} || "dev.rabbitmq.com";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

eval { $mq->connect($host, { user => "guest", password => "guest" }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
my $queuename = undef;
my $expect_qn = 'test.net.rabbitmq.perl';
eval { $queuename = $mq->queue_declare(1, $expect_qn, { passive => 0, durable => 1, exclusive => 0, auto_delete => 1 }); };
is($@, '', "queue_declare");
is($queuename, $expect_qn, "queue_declare -> $queuename = $expect_qn");

1;
