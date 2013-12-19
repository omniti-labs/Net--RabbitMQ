use Test::More tests => 7;
use strict;

my $dtag=(unpack("L",pack("N",1)) != 1)?'0100000000000000':'0000000000000001';
my $host = $ENV{'MQHOST'} || "dev.rabbitmq.com";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

eval { $mq->connect($host, { user => "guest", password => "guest" }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
eval { $mq->consume(1, "nr_test_hole", {consumer_tag=>'ctag', no_local=>0,no_ack=>1,exclusive=>0}); };
is($@, '', "consume");

my $rv = {};
eval { local $SIG{ALRM} = sub {die}; alarm 5; $rv = $mq->recv(); alarm 0};
is($@, '', "recv");

$rv->{delivery_tag} =~ s/(.)/sprintf("%02x", ord($1))/esg;
is_deeply($rv,
          {
          'body' => 'Magic Payload',
          'routing_key' => 'nr_test_route',
          'delivery_tag' => $dtag,
          'exchange' => 'nr_test_x',
          'consumer_tag' => 'ctag',
          'props' => {
                content_type => 'text/plain',
                content_encoding => 'none',
                correlation_id => '123',
                reply_to => 'somequeue',
                expiration => 60 * 1000,
                message_id => 'ABC',
                type => 'notmytype',
                user_id => 'guest',
                app_id => 'idd',
                delivery_mode => 1,
                priority => 2,
                timestamp => 1271857990,
            },
          }, "payload");

1;
