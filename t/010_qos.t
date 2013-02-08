use Test::More tests => 6;
use strict;

my $dtag=(unpack("L",pack("N",1)) != 1)?'0100000000000000':'0000000000000001';
my $host = $ENV{'MQHOST'} || "localhost";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

SKIP: {
    skip "Ignore", 4;
    eval { $mq->connect($host, { user => "guest", password => "guest" }); };
    is($@, '', "connect");
    eval { $mq->channel_open(1); };
    is($@, '', "channel_open");
    my $qos = '';
    eval { $qos = $mq->basic_qos(1, { prefetch_count => 5 }); };
    is($@, '', "qos error");
    is($qos, undef, "qos");
}
1;
