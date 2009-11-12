#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "librabbitmq/amqp.h"
#include "librabbitmq/amqp_framing.h"

typedef amqp_connection_state_t Net__RabbitMQ__Connection;

void die_on_error(pTHX_ int x, char const *context) {
  if (x < 0) {
    Perl_croak(aTHX_ "%s: %s\n", context, strerror(-x));
  }
}

void die_on_amqp_error(pTHX_ amqp_rpc_reply_t x, char const *context) {
  switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      return;

    case AMQP_RESPONSE_NONE:
      Perl_croak(aTHX_ "%s: missing RPC reply type!", context);
      break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      Perl_croak(aTHX_ "%s: %s\n", context,
              x.library_errno ? strerror(x.library_errno) : "(end-of-stream)");
      break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
      switch (x.reply.id) {
        case AMQP_CONNECTION_CLOSE_METHOD: {
          amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
          Perl_croak(aTHX_ "%s: server connection error %d, message: %.*s",
                  context,
                  m->reply_code,
                  (int) m->reply_text.len, (char *) m->reply_text.bytes);
          break;
        }
        case AMQP_CHANNEL_CLOSE_METHOD: {
          amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
          Perl_croak(aTHX_ "%s: server channel error %d, message: %.*s",
                  context,
                  m->reply_code,
                  (int) m->reply_text.len, (char *) m->reply_text.bytes);
          break;
        }
        default:
          Perl_croak(aTHX_ "%s: unknown server error, method id 0x%08X", context, x.reply.id);
          break;
      }
      break;
  }
}

MODULE = Net::RabbitMQ::Connection PACKAGE = Net::RabbitMQ::Connection PREFIX = net_rabbitmq_c_

REQUIRE:        1.9505
PROTOTYPES:     DISABLE

int
net_rabbitmq_c_connect(conn, hostname, options)
  Net::RabbitMQ::Connection conn
  char *hostname
  HV *options
  PREINIT:
    int sockfd;
    char *user = "guest";
    char *pass = "guest";
    char *vhost = "/";
    int port = 5672;
    int channel_max = 0;
    int frame_max = 131072;
    int heartbeat = 0;
    SV **v;
  CODE:
    die_on_error(aTHX_ sockfd = amqp_open_socket(hostname, port), "Opening socket");
    amqp_set_sockfd(conn, sockfd);
    if(NULL != (v = hv_fetch(options, "user", strlen("user"), 0))) user = SvPV_nolen(*v);
    if(NULL != (v = hv_fetch(options, "pass", strlen("pass"), 0))) pass = SvPV_nolen(*v);
    if(NULL != (v = hv_fetch(options, "password", strlen("password"), 0))) pass = SvPV_nolen(*v);
    if(NULL != (v = hv_fetch(options, "vhost", strlen("vhost"), 0))) vhost = SvPV_nolen(*v);
    if(NULL != (v = hv_fetch(options, "channel_max", strlen("channel_max"), 0))) channel_max = SvIV(*v);
    if(NULL != (v = hv_fetch(options, "frame_max", strlen("frame_max"), 0))) frame_max = SvIV(*v);
    if(NULL != (v = hv_fetch(options, "heartbeat", strlen("heartbeat"), 0))) heartbeat = SvIV(*v);
    if(NULL != (v = hv_fetch(options, "port", strlen("port"), 0))) port = SvIV(*v);
    die_on_amqp_error(aTHX_ amqp_login(conn, vhost, channel_max, frame_max, heartbeat, AMQP_SASL_METHOD_PLAIN, user, pass),
                      "Logging in");
    RETVAL = sockfd;
  OUTPUT:
    RETVAL

void
net_rabbitmq_c_channel_open(conn, channel)
  Net::RabbitMQ::Connection conn
  int channel
  CODE:
    amqp_channel_open(conn, channel);
    die_on_amqp_error(aTHX_ amqp_rpc_reply, "Opening channel");

void
net_rabbitmq_c_channel_close(conn, channel)
  Net::RabbitMQ::Connection conn
  int channel
  CODE:
    die_on_amqp_error(aTHX_ amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS), "Closing channel");

void
net_rabbitmq_c_exchange_declare(conn, channel, exchange, exchange_type, passive, durable, auto_delete, args = NULL)
  Net::RabbitMQ::Connection conn
  int channel
  char *exchange
  char *exchange_type
  int passive
  int durable
  int auto_delete
  HV *args
  PREINIT:
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
  CODE:
    amqp_exchange_declare(conn, channel, amqp_cstring_bytes(exchange), amqp_cstring_bytes(exchange_type),
                          passive, durable, auto_delete, arguments);
    die_on_amqp_error(aTHX_ amqp_rpc_reply, "Declaring exchange");

SV *
net_rabbitmq_c_queue_declare(conn, channel, queuename, passive, durable, exclusive, auto_delete, args = NULL)
  Net::RabbitMQ::Connection conn
  int channel
  char *queuename
  int passive
  int durable
  int exclusive
  int auto_delete
  HV *args
  PREINIT:
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
    amqp_bytes_t queuename_b = AMQP_EMPTY_BYTES;
  CODE:
    if(queuename && strcmp(queuename, "")) queuename_b = amqp_cstring_bytes(queuename);
    amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, channel, queuename_b, passive,
                                                    durable, exclusive, auto_delete,
                                                    arguments);
    die_on_amqp_error(aTHX_ amqp_rpc_reply, "Declaring queue");
    RETVAL = newSVpvn(r->queue.bytes, r->queue.len);
  OUTPUT:
    RETVAL

void
net_rabbitmq_c_queue_bind(conn, channel, queuename, exchange, bindingkey, args = NULL)
  Net::RabbitMQ::Connection conn
  int channel
  char *queuename
  char *exchange
  char *bindingkey
  HV *args
  PREINIT:
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
  CODE:
    if(queuename == NULL || exchange == NULL || bindingkey == NULL)
      Perl_croak(aTHX_ "queuename, exchange and bindingkey must all be specified");
    amqp_queue_bind(conn, channel, amqp_cstring_bytes(queuename),
                    amqp_cstring_bytes(exchange),
                    amqp_cstring_bytes(bindingkey),
                    arguments);
    die_on_amqp_error(aTHX_ amqp_rpc_reply, "Binding queue");

void
net_rabbitmq_c_queue_unbind(conn, channel, queuename, exchange, bindingkey, args = NULL)
  Net::RabbitMQ::Connection conn
  int channel
  char *queuename
  char *exchange
  char *bindingkey
  HV *args
  PREINIT:
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
  CODE:
    if(queuename == NULL || exchange == NULL || bindingkey == NULL)
      Perl_croak(aTHX_ "queuename, exchange and bindingkey must all be specified");
    amqp_queue_unbind(conn, channel, amqp_cstring_bytes(queuename),
                      amqp_cstring_bytes(exchange),
                    amqp_cstring_bytes(bindingkey),
                    arguments);
    die_on_amqp_error(aTHX_ amqp_rpc_reply, "Unbinding queue");

SV *
net_rabbitmq_c_consume(conn, channel, queuename, consumer_tag = NULL, no_local = 0, no_ack = 1, exclusive = 0)
  Net::RabbitMQ::Connection conn
  int channel
  char *queuename
  char *consumer_tag
  int no_local
  int no_ack
  int exclusive
  PREINIT:
    amqp_basic_consume_ok_t *r;
  CODE:
    r = amqp_basic_consume(conn, channel, amqp_cstring_bytes(queuename),
                           consumer_tag ? amqp_cstring_bytes(consumer_tag) : AMQP_EMPTY_BYTES,
                           no_local, no_ack, exclusive);
    die_on_amqp_error(aTHX_ amqp_rpc_reply, "Consume queue");
    RETVAL = newSVpvn(r->consumer_tag.bytes, r->consumer_tag.len);
  OUTPUT:
    RETVAL

HV *
net_rabbitmq_c_recv(conn)
  Net::RabbitMQ::Connection conn
  PREINIT:
    amqp_frame_t frame;
    int result = 0;
    amqp_basic_deliver_t *d;
    amqp_basic_properties_t *p;
    size_t body_target;
    size_t body_received;
  CODE:
    RETVAL = newHV();
    while (1) {
      SV *payload;
      amqp_maybe_release_buffers(conn);
      result = amqp_simple_wait_frame(conn, &frame);
      if (result <= 0) break;
      if (frame.frame_type != AMQP_FRAME_METHOD) continue;
      if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) continue;

      d = (amqp_basic_deliver_t *) frame.payload.method.decoded;
      hv_store(RETVAL, "delivery_tag", strlen("delivery_tag"), newSViv(d->delivery_tag), 0);
      hv_store(RETVAL, "exchange", strlen("exchange"), newSVpvn(d->exchange.bytes, d->exchange.len), 0);
      hv_store(RETVAL, "routing_key", strlen("routing_key"), newSVpvn(d->routing_key.bytes, d->routing_key.len), 0);

      result = amqp_simple_wait_frame(conn, &frame);
      if (result <= 0) break;

      if (frame.frame_type != AMQP_FRAME_HEADER)
        Perl_croak(aTHX_ "Unexpected header %d!", frame.frame_type);

      p = (amqp_basic_properties_t *) frame.payload.properties.decoded;
      if (p->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
        hv_store(RETVAL, "content_type", strlen("content_type"),
                 newSVpvn(p->content_type.bytes, p->content_type.len), 0);
      }

      body_target = frame.payload.properties.body_size;
      body_received = 0;
      payload = newSVpvn("", 0);

      while (body_received < body_target) {
        result = amqp_simple_wait_frame(conn, &frame);
        if (result <= 0) break;

        if (frame.frame_type != AMQP_FRAME_BODY) {
          Perl_croak(aTHX_ "Expected fram body, got %d!", frame.frame_type);
        }

        body_received += frame.payload.body_fragment.len;
        assert(body_received <= body_target);

        sv_catpvn(payload, frame.payload.body_fragment.bytes, frame.payload.body_fragment.len);
      }

      if (body_received != body_target) {
        /* Can only happen when amqp_simple_wait_frame returns <= 0 */
        /* We break here to close the connection */
        Perl_croak(aTHX_ "Short read %llu != %llu", (long long unsigned int)body_received, (long long unsigned int)body_target);
      }
      hv_store(RETVAL, "body", strlen("body"), payload, 0);
      break;
    }
    if(result <= 0) Perl_croak(aTHX_ "Bad frame read.");
  OUTPUT:
    RETVAL

int
net_rabbitmq_c_publish(conn, channel, routing_key, body, options = NULL, props = NULL)
  Net::RabbitMQ::Connection conn
  int channel
  HV *options;
  char *routing_key
  SV *body
  HV *props
  PREINIT:
    SV **v;
    char *exchange = "amq.direct";
    amqp_boolean_t mandatory = 0;
    amqp_boolean_t immediate = 0;
    int rv;
    amqp_bytes_t exchange_b;
    amqp_bytes_t routing_key_b;
    amqp_bytes_t body_b;
    struct amqp_basic_properties_t_ const *properties = NULL;
    STRLEN len;
  CODE:
    routing_key_b = amqp_cstring_bytes(routing_key);
    body_b.bytes = SvPV(body, len);
    body_b.len = len;
    if(options) {
      if(NULL != (v = hv_fetch(options, "mandatory", strlen("mandatory"), 0))) mandatory = SvIV(*v) ? true : false;
      if(NULL != (v = hv_fetch(options, "immediate", strlen("immediate"), 0))) immediate = SvIV(*v) ? true : false;
      if(NULL != (v = hv_fetch(options, "exchange", strlen("exchange"), 0))) exchange_b = amqp_cstring_bytes(SvPV_nolen(*v));
    }
    rv = amqp_basic_publish(conn, channel, exchange_b, routing_key_b,
                            mandatory, immediate, properties, body_b);
    RETVAL = rv;
  OUTPUT:
    RETVAL

int
net_rabbitmq_c_get_channel_max(conn)
  Net::RabbitMQ::Connection conn
  CODE:
    RETVAL = amqp_get_channel_max(conn);
  OUTPUT:
    RETVAL

void
net_rabbitmq_c_disconnect(conn)
  Net::RabbitMQ::Connection conn
  CODE:
    die_on_amqp_error(aTHX_ amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");

void
net_rabbitmq_c_DESTROY(conn)
  Net::RabbitMQ::Connection conn
  CODE:
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);

MODULE = Net::RabbitMQ PACKAGE = Net::RabbitMQ PREFIX = net_rabbitmq_

REQUIRE:        1.9505
PROTOTYPES:     DISABLE

Net::RabbitMQ::Connection
net_rabbitmq_new(clazz)
  char *clazz
  PREINIT:
    Net__RabbitMQ__Connection conn;
  CODE:
    conn = amqp_new_connection();
    RETVAL = conn;
  OUTPUT:
    RETVAL

void
net_rabbitmq_DESTROY(obj)
  Net::RabbitMQ::Connection obj
  CODE:
    amqp_destroy_connection(obj);

