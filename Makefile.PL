use strict;
use warnings;
require 5.004 ;

use ExtUtils::MakeMaker 5.16 ;
use Config ;
use FindBin qw/$Bin/;

my $ofiles = '';
foreach ( <$Bin/*.c> ){
    $_ =~ s/.c$/.o/;
    $ofiles .= "$_ ";
}

WriteMakefile(
    NAME            => 'Net::RabbitMQ',
    VERSION_FROM    => 'RabbitMQ.pm',
    OBJECT          => "RabbitMQ.o ${ofiles}",
    OPTIMIZE        => $ENV{'OPTIMIZE'},
    CCFLAGS         => "-I${Bin}",
    LIBS            =>['-lrt'],
);
