use Test::More tests => 5;
use strict;

my $host = $ENV{'MQHOST'} || "localhost";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

eval { $mq->connect($host, { user => "guest", password => "guest" }); };
is($@, '', "connect");
eval { $mq->channel_open(1); };
is($@, '', "channel_open");
eval { $mq->exchange_declare(1, "nr_test_x", { exchange_type => "direct", passive => 0, durable => 1, auto_delete => 0 }); };
is($@, '', "exchange_delcare");

1;
