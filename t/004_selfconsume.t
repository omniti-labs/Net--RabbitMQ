use Test::More tests => 10;
use strict;

my $host = $ENV{'MQHOST'} || "localhost";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

eval { $mq->connect($host, { user => "guest", pass => "guest" }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
my $queuename = '';
eval { $queuename = $mq->queue_declare(1, '', { passive => 0, durable => 1, exclusive => 0, auto_delete => 1 }); };
is($@, '', "queue_declare");
isnt($queuename, '', "queue_declare -> private name");
eval { $mq->queue_bind(1, $queuename, "nr_test_x", "nr_test_q"); };
is($@, '', "queue_bind");
eval { $mq->publish(1, "nr_test_q", "Magic Transient Payload", { exchange => "nr_test_x" }); };
eval { $mq->consume(1, $queuename); };
is($@, '', "consume");

my $rv = {};
eval { $rv = $mq->recv(); };
is($@, '', "recv");
is_deeply($rv,
          {
          'body' => 'Magic Transient Payload',
          'routing_key' => 'nr_test_q',
          'delivery_tag' => 1,
          'exchange' => 'nr_test_x'
          }, "payload");

1;
