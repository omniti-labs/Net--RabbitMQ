use Test::More tests => 7;
use strict;

my $host = $ENV{'MQHOST'} || "dev.rabbitmq.com";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

eval { $mq->connect($host, { user => "guest", password => "guest" }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
eval { $mq->consume(1, "nr_test_hole"); };
is($@, '', "consume");

my $rv = {};
eval { $rv = $mq->recv(); };
is($@, '', "recv");
$rv->{delivery_tag} =~ s/(.)/sprintf("%02x", ord($1))/esg;
is_deeply($rv,
          {
          'body' => 'Magic Payload',
          'routing_key' => 'nr_test_route',
          'delivery_tag' => '0100000000000000',
          'exchange' => 'nr_test_x'
          }, "payload");

1;
