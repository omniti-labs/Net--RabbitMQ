require 5.004 ;
use ExtUtils::MakeMaker 5.16 ;
use Config ;

my @parts = qw/api connection debug framing mem socket table/;
my $cfiles = join(' ', map { "librabbitmq/amqp_$_.c" } @parts);

WriteMakefile(
        NAME            => 'Net::RabbitMQ',
        VERSION_FROM    => 'RabbitMQ.pm',
	OBJECT          => "RabbitMQ.o $cfiles",
        );

