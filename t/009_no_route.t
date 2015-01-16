use Test::More tests => 10;
use strict;
use Data::Dumper;

my $host = $ENV{'MQHOST'} || "dev.rabbitmq.com";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);
my $result = $mq->connect($host, {"user" => "guest", "password" => "guest"});
ok($result, 'connect');
eval { $mq->channel_open(1); };
$mq->basic_return(sub {
  my ($channel, $m) = @_;
  is($channel, 1, 'basic return channel');
  is($m->{reply_text}, 'NO_ROUTE', 'basic return reply');
});

is($@, '', 'channel_open');
$result = eval { $mq->publish(1, "nr_test_route", "Magic Payload",
                       { exchange => "nr_test_x" }); };
is($@, '', 'good pub');
is($result, 0, 'good pub code');
$result = eval { $mq->publish(1, "nr_test_noroute", "Magic Payload",
                       { exchange => "nr_test_x",
                         'mandatory' => 1}); };
is($@, '', 'bad pub');
is($result, 0, 'bad pub code');
$mq->disconnect();

