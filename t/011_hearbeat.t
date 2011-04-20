use Test::More tests => 6;
use strict;
use Data::Dumper;

my $host = $ENV{'MQHOST'} || "dev.rabbitmq.com";
$SIG{'PIPE'} = 'IGNORE';
use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

eval { $mq->connect($host, { user => "guest", password => "guest", heartbeat => 1 }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
sleep(5);
eval { $mq->heartbeat(); };
is($@, '', "heartbeat");
$mq->basic_return(sub {
  my ($channel, $m) = @_;
  print Dumper(\@_);
});
my $rv = 0;
eval { $rv = $mq->publish(1, "nr_test_q", "Magic Transient Payload", { exchange => "nr_test_x", "immediate" => 1, "mandatory" => 1 }); };
is($rv, 0, "publish");
sleep(1);
eval { $rv = $mq->publish(1, "nr_test_q", "Magic Transient Payload", { exchange => "nr_test_x", "immediate" => 1, "mandatory" => 1 }); };
is($rv, -1, "publish");

1;
