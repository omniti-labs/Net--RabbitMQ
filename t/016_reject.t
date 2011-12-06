{\rtf1\ansi\ansicpg1252\cocoartf1138\cocoasubrtf230
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
\margl1440\margr1440\vieww21520\viewh14160\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural

\f0\fs24 \cf0 use Test::More tests => 16;\
use strict;\
\
my $dtag=(unpack("L",pack("N",1)) != 1)?'0100000000000000':'0000000000000001';\
my $host = $ENV\{'MQHOST'\} || "dev.rabbitmq.com";\
\
use_ok('Net::RabbitMQ');\
\
my $mq = Net::RabbitMQ->new();\
ok($mq);\
\
eval \{ $mq->connect($host, \{ user => "guest", password => "guest" \}); \};\
is($@, '', "connect");\
eval \{ $mq->channel_open(1); \};\
is($@, '', "channel_open");\
eval \{ $mq->queue_declare(1, "nr_test_reject", \{ passive => 0, durable => 1, exclusive => 0, auto_delete => 0 \}); \};\
is($@, '', "queue_declare");\
eval \{ $mq->queue_bind(1, "nr_test_reject", "nr_test_x", "nr_test_reject_route"); \};\
is($@, '', "queue_bind");\
eval \{ $mq->purge(1, "nr_test_reject"); \};\
is($@, '', "purge");\
eval \{ $mq->publish(1, "nr_test_reject_route", "Magic Payload $$", \{ exchange => "nr_test_x" \}); \};\
is($@, '', "publish");\
eval \{ $mq->consume(1, "nr_test_reject", \{ no_ack => 0, consumer_tag=>'ctag' \} ); \};\
is($@, '', "consuming");\
my $payload = \{\};\
eval \{ $payload = $mq->recv(); \};\
$payload->\{delivery_tag\} =~ s/(.)/sprintf("%02x", ord($1))/esg;\
is_deeply($payload,\
          \{\
          'body' => "Magic Payload $$",\
          'routing_key' => 'nr_test_reject_route',\
          'delivery_tag' => $dtag,\
          'exchange' => 'nr_test_x',\
          'consumer_tag' => 'ctag',\
          'props' => \{\},\
          \}, "payload");\
eval \{ $mq->disconnect(); \};\
is($@, '', "disconnect");\
\
eval \{ $mq->connect($host, \{ user => "guest", password => "guest" \}); \};\
is($@, '', "connect");\
eval \{ $mq->channel_open(1); \};\
is($@, '', "channel_open");\
eval \{ $mq->consume(1, "nr_test_reject", \{ no_ack => 0, consumer_tag=>'ctag' \} ); \};\
is($@, '', "consuming");\
$payload = \{\};\
eval \{ $payload = $mq->recv(); \};\
my $reject_tag = $payload->\{delivery_tag\};\
$payload->\{delivery_tag\} =~ s/(.)/sprintf("%02x", ord($1))/esg;\
is_deeply($payload,\
          \{\
          'body' => "Magic Payload $$",\
          'routing_key' => 'nr_test_reject_route',\
          'delivery_tag' => $dtag,\
          'exchange' => 'nr_test_x',\
          'consumer_tag' => 'ctag',\
          'props' => \{\},\
          \}, "payload");\
eval \{ $mq->reject(1, $reject_tag); \};\
is($@, '', "rejecting");\
\
1;}