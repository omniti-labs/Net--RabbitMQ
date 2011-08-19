use Test::More tests => 16;
use strict;

package TestBlessings;
use overload
	'""' => sub { uc ${$_[0]} },
	;

sub new {
	my ($class, $self) = @_;

	bless \$self, $class;
}

package main;

my $host = $ENV{'MQHOST'} || "dev.rabbitmq.com";
$host = 'localhost'; # FIXME

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq, "Created object");

eval { $mq->connect($host, { user => "guest", password => "guest" }); };
is($@, '', "connect");

eval { $mq->channel_open(1); };
is($@, '', "channel_open");

eval { $mq->queue_declare(1, "nr_test_hole", { passive => 0, durable => 1, exclusive => 0, auto_delete => 0 }); };
is($@, '', "queue_declare");

eval { $mq->queue_bind(1, "nr_test_hole", "nr_test_x", "nr_test_route"); };
is($@, '', "queue_bind");

my $headers = {
	abc => 123,
	def => 'xyx',
	head3 => 3,
	head4 => 4,
	head5 => 5,
	head6 => 6,
	head7 => 7,
	head8 => 8,
	head9 => 9,
	head10 => 10,
	head11 => 11,
	head12 => 12,
};
eval { $mq->publish( 1, "nr_test_route", "Header Test",
		{ exchange => "nr_test_x" },
		{ headers => $headers },
	);
};

is( $@, '', "publish" );

eval { $mq->consume(1, "nr_test_hole", {consumer_tag=>'ctag', no_local=>0,no_ack=>1,exclusive=>0}); };
is($@, '', "consume");

my $msg;
eval { $msg = $mq->recv() };
is( $@, '', 'recv' );

is( $msg->{body}, 'Header Test', "Received body" );
use Data::Dumper;
diag Dumper($msg);
is( exists $msg->{props}, 1, "Props exist" );
is( exists $msg->{props}{headers}, 1, "Headers exist" );
is_deeply( $msg->{props}{headers}, $headers, "Received headers" );

$headers = {
	blah => TestBlessings->new('foo'),
};
eval { $mq->publish( 1, "nr_test_route", "Header Test",
		{ exchange => "nr_test_x" },
		{ headers => $headers },
	);
};
is( $@, '', 'publish with blessed header values' );

eval { $msg = $mq->recv() };
is( $@, '', 'recv from blessed header values' );

is_deeply( $msg->{props}{headers}, $headers, "Received blessed headers" );
