use Test::More tests => 4;
use strict;
use Time::HiRes qw/gettimeofday tv_interval/;

my $host = '127.0.1.2';
$SIG{'PIPE'} = 'IGNORE';
use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

my $start = [gettimeofday];
my $attempt = 0.6;
eval { $mq->connect($host, { user => "guest", password => "guest", timeout => $attempt }); };
my $duration = tv_interval($start);
isnt($@, '', "connect");
# 50ms tolerance should work with most operating systems
cmp_ok(abs($duration-$attempt), '<', 0.05, 'timeout');
