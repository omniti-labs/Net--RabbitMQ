use Test::More tests => 13;
use strict;

my $dtag=(unpack("L",pack("N",1)) != 1)?'0100000000000000':'0000000000000001';
my $host = $ENV{'MQHOST'} || "localhost";

use_ok('Net::RabbitMQ');

my $mq = Net::RabbitMQ->new();
ok($mq);

SKIP: {
    skip "Ignore", 11;
    eval { $mq->connect($host, { user => "guest", password => "guest" }); };
    is($@, '', "connect");
    eval { $mq->channel_open(1); };
    is($@, '', "channel_open");
    my $queuename = '';
    eval { $queuename = $mq->queue_declare(1, '', { passive => 0, durable => 0, exclusive => 0, auto_delete => 1 }); };
    is($@, '', "queue_declare");
    isnt($queuename, '', "queue_declare -> private name");
    eval { $mq->queue_bind(1, $queuename, "nr_test_x", "nr_test_q"); };
    is($@, '', "queue_bind");

    my $getr;
    eval { $getr = $mq->get(1, $queuename); };
    is($@, '', "get");
    is($getr, undef, "get should return empty");

    eval { $mq->publish(1, "nr_test_q", "Magic Transient Payload", { exchange => "nr_test_x" }); };

    eval { $getr = $mq->get(1, $queuename); };
    is($@, '', "get");
    $getr->{delivery_tag} =~ s/(.)/sprintf("%02x", ord($1))/esg;
    is_deeply($getr,
          {
            redelivered => 0,
            routing_key => 'nr_test_q',
            exchange => 'nr_test_x',
            message_count => 0,
            delivery_tag => $dtag,
            'props' => {},
            body => 'Magic Transient Payload',
          }, "get should see message");


    eval { $mq->publish(1, "nr_test_q", "Magic Transient Payload 2", 
                     { exchange => "nr_test_x" }, 
                     {
                       content_type => 'text/plain',
                       content_encoding => 'none',
                       correlation_id => '123',
                       reply_to => 'somequeue',
                       expiration => 1000,
                       message_id => 'ABC',
                       type => 'notmytype',
                       user_id => 'guest',
                       app_id => 'idd',
                       delivery_mode => 1,
                       priority => 2,
                       timestamp => 1271857990,
                     },
                     ); };

    eval { $getr = $mq->get(1, $queuename); };
    is($@, '', "get");
    $getr->{delivery_tag} =~ s/(.)/sprintf("%02x", ord($1))/esg;
    $dtag =~ s/1/2/;
    is_deeply($getr,
          {
            redelivered => 0,
            routing_key => 'nr_test_q',
            exchange => 'nr_test_x',
            message_count => 0,
            delivery_tag => $dtag,
            props => {
                content_type => 'text/plain',
                content_encoding => 'none',
                correlation_id => '123',
                reply_to => 'somequeue',
                expiration => 1000,
                message_id => 'ABC',
                type => 'notmytype',
                user_id => 'guest',
                app_id => 'idd',
                delivery_mode => 1,
                priority => 2,
                timestamp => 1271857990,
            },
            body => 'Magic Transient Payload 2',
          }, "get should see message");
}

1;
