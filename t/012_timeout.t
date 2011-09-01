use Test::More;
use strict;
use Time::HiRes qw/gettimeofday tv_interval/;

# These tests will only work without additional setup
# People installing from CPAN are unlikely to have done this setup,
# so only run these tests when run from git repository
# or if environment variable indicates that tests are run by the maintainer
if( -d '.git' or $ENV{IS_MAINTAINER} ) {
	plan tests => 4;
}
else {
	plan skip_all => 'Timeout tests require additional setup, only run for maintainer';
}

# On a GNU/Linux system, running the following command as root will generally
# allow these tests to pass:
# iptables -I OUTPUT --dst 127.0.1.2 -j DROP

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
