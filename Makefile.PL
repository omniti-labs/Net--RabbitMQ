require 5.004 ;
use ExtUtils::MakeMaker 5.16 ;
use Config ;
use FindBin qw/$Bin/;

my $rabbbitmqc_dir = "$Bin/rabbitmq-c/librabbitmq";
my $cfiles = [<$rabbbitmqc_dir/*.c>];
push @$cfiles, "$Bin/amqp_debug.c";
my $ofiles = join ' ', map { $_ =~ s/\.c$/\.o/; $_ } @$cfiles;

WriteMakefile(
	NAME		    => 'Net::RabbitMQ',
	VERSION_FROM	=> 'RabbitMQ.pm',
	OBJECT		    => "RabbitMQ.o $ofiles",
	C		        => $cfiles,
	OPTIMIZE	    => $ENV{'OPTIMIZE'},
    CCFLAGS          => "-I$rabbbitmqc_dir",
	PREREQ_PM	    => {},
);
