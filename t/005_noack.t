use Test::More tests => 15;
use strict;

my $host = $ENV{'MQHOST'} || "dev.rabbitmq.com";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

eval { $mq->connect($host, { user => "guest", password => "guest" }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
eval { $mq->queue_declare(1, "nr_test_ack", { passive => 0, durable => 1, exclusive => 0, auto_delete => 0 }); };
is($@, '', "queue_declare");
eval { $mq->queue_bind(1, "nr_test_ack", "nr_test_x", "nr_test_ack_route"); };
is($@, '', "queue_bind");
eval { $mq->publish(1, "nr_test_ack_route", "Magic Payload $$", { exchange => "nr_test_x" }); };
is($@, '', "publish");
eval { $mq->consume(1, "nr_test_ack", { no_ack => 0 } ); };
is($@, '', "consuming");
my $payload = {};
eval { $payload = $mq->recv(); };
is_deeply($payload,
          {
          'body' => "Magic Payload $$",
          'routing_key' => 'nr_test_ack_route',
          'delivery_tag' => pack('LL', 1, 0),
          'exchange' => 'nr_test_x'
          }, "payload");
eval { $mq->disconnect(); };
is($@, '', "disconnect");

eval { $mq->connect($host, { user => "guest", password => "guest" }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
eval { $mq->consume(1, "nr_test_ack", { no_ack => 0 } ); };
is($@, '', "consuming");
$payload = {};
eval { $payload = $mq->recv(); };
is_deeply($payload,
          {
          'body' => "Magic Payload $$",
          'routing_key' => 'nr_test_ack_route',
          'delivery_tag' => pack('LL', 1, 0),
          'exchange' => 'nr_test_x'
          }, "payload");
eval { $mq->ack(1, $payload->{delivery_tag}); };
is($@, '', "acking");

1;
