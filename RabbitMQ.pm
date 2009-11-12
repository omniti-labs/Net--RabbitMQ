package Net::RabbitMQ;

require DynaLoader;

use strict;
use vars qw($VERSION @ISA);
$VERSION = "0.0.1";
@ISA = qw/DynaLoader/;

bootstrap Net::RabbitMQ $VERSION ;

1;
