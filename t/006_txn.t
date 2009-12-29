use Test::More tests => 13;
use strict;

my $host = $ENV{'MQHOST'} || "localhost";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

eval { $mq->connect($host, { user => "guest", password => "guest" }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
my $queuename = '';
eval { $queuename = $mq->queue_declare(1, '', { passive => 0, durable => 1, exclusive => 0, auto_delete => 1 }); };
is($@, '', "queue_declare");
isnt($queuename, '', "queue_declare -> private name");
eval { $mq->queue_bind(1, $queuename, "nr_test_x", "nr_test_q"); };
is($@, '', "queue_bind");
eval { $mq->tx_select(1); };
is($@, '', "tx_select");
eval { $mq->publish(1, "nr_test_q", "Magic Transient Payload", { exchange => "nr_test_x" }); };
eval { $mq->tx_rollback(1); };
is($@, '', "tx_rollback");
eval { $mq->publish(1, "nr_test_q", "Magic Transient Payload (Commit)", { exchange => "nr_test_x" }); };
eval { $mq->tx_commit(1); };
is($@, '', "tx_commit");
eval { $mq->consume(1, $queuename); };
is($@, '', "consume");

my $rv = {};
eval { $rv = $mq->recv(); };
is($@, '', "recv");
is_deeply($rv,
          {
          'body' => 'Magic Transient Payload (Commit)',
          'routing_key' => 'nr_test_q',
          'delivery_tag' => pack('LL', 1, 0),
          'exchange' => 'nr_test_x'
          }, "payload");

1;
