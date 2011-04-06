#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "amqp.h"
#include "amqp_framing.h"

typedef amqp_connection_state_t Net__RabbitMQ;

#define int_from_hv(hv,name) \
 do { SV **v; if(NULL != (v = hv_fetch(hv, #name, strlen(#name), 0))) name = SvIV(*v); } while(0)
#define str_from_hv(hv,name) \
 do { SV **v; if(NULL != (v = hv_fetch(hv, #name, strlen(#name), 0))) name = SvPV_nolen(*v); } while(0)

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

static void
internal_brcb(amqp_channel_t channel, amqp_basic_return_t *m, void *vsv) {
  HV *mp;
  SV *sv = (SV *)vsv;
  dSP;
  ENTER;
  SAVETMPS;
  PUSHMARK(SP);
  XPUSHs(sv_2mortal(newSViv(channel)));
  mp = newHV();
  hv_store(mp, "reply_code", strlen("reply_code"), newSViv(m->reply_code), 0);
  hv_store(mp, "reply_text", strlen("reply_text"),
           newSVpvn(m->reply_text.bytes, m->reply_text.len), 0);
  hv_store(mp, "exchange", strlen("exchange"),
           newSVpvn(m->exchange.bytes, m->exchange.len), 0);
  hv_store(mp, "routing_key", strlen("routing_key"),
           newSVpvn(m->routing_key.bytes, m->routing_key.len), 0);
  XPUSHs(sv_2mortal((SV *)newRV((SV *)mp)));
  PUTBACK;
  call_sv(sv, G_DISCARD);
  SPAGAIN;
  PUTBACK;
  FREETMPS;
  LEAVE;
}
int internal_recv(HV *RETVAL, amqp_connection_state_t conn, int piggyback) {
  amqp_frame_t frame;
  amqp_basic_deliver_t *d;
  amqp_basic_properties_t *p;
  size_t body_target;
  size_t body_received;
  int result;

  result = 0;
  while (1) {
    SV *payload;

    if(!piggyback) {
      amqp_maybe_release_buffers(conn);
      result = amqp_simple_wait_frame(conn, &frame);
      if (result <= 0) break;
      if (frame.frame_type != AMQP_FRAME_METHOD) continue;
      if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) continue;
      d = (amqp_basic_deliver_t *) frame.payload.method.decoded;
      hv_store(RETVAL, "delivery_tag", strlen("delivery_tag"), newSVpvn((const char *)&d->delivery_tag, sizeof(d->delivery_tag)), 0);
      hv_store(RETVAL, "exchange", strlen("exchange"), newSVpvn(d->exchange.bytes, d->exchange.len), 0);
      hv_store(RETVAL, "consumer_tag", strlen("consumer_tag"), newSVpvn(d->consumer_tag.bytes, d->consumer_tag.len), 0);
      hv_store(RETVAL, "routing_key", strlen("routing_key"), newSVpvn(d->routing_key.bytes, d->routing_key.len), 0);
      piggyback = 0;
    }

    result = amqp_simple_wait_frame(conn, &frame);
    if (result <= 0) break;

    if (frame.frame_type == AMQP_FRAME_HEARTBEAT) continue;
    if (frame.frame_type != AMQP_FRAME_HEADER)
      Perl_croak(aTHX_ "Unexpected header %d!", frame.frame_type);

    HV *props;
    props = newHV();
    hv_store(RETVAL, "props", strlen("props"), newRV_noinc((SV *)props), 0);

    p = (amqp_basic_properties_t *) frame.payload.properties.decoded;
    if (p->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
      hv_store(props, "content_type", strlen("content_type"),
               newSVpvn(p->content_type.bytes, p->content_type.len), 0);
    }
    if (p->_flags & AMQP_BASIC_CONTENT_ENCODING_FLAG) {
      hv_store(props, "content_encoding", strlen("content_encoding"),
               newSVpvn(p->content_encoding.bytes, p->content_encoding.len), 0);
    }
    if (p->_flags & AMQP_BASIC_CORRELATION_ID_FLAG) {
      hv_store(props, "correlation_id", strlen("correlation_id"),
               newSVpvn(p->correlation_id.bytes, p->correlation_id.len), 0);
    }
    if (p->_flags & AMQP_BASIC_REPLY_TO_FLAG) {
      hv_store(props, "reply_to", strlen("reply_to"),
               newSVpvn(p->reply_to.bytes, p->reply_to.len), 0);
    }
    if (p->_flags & AMQP_BASIC_EXPIRATION_FLAG) {
      hv_store(props, "expiration", strlen("expiration"),
               newSVpvn(p->expiration.bytes, p->expiration.len), 0);
    }
    if (p->_flags & AMQP_BASIC_MESSAGE_ID_FLAG) {
      hv_store(props, "message_id", strlen("message_id"),
               newSVpvn(p->message_id.bytes, p->message_id.len), 0);
    }
    if (p->_flags & AMQP_BASIC_TYPE_FLAG) {
      hv_store(props, "type", strlen("type"),
               newSVpvn(p->type.bytes, p->type.len), 0);
    }
    if (p->_flags & AMQP_BASIC_USER_ID_FLAG) {
      hv_store(props, "user_id", strlen("user_id"),
               newSVpvn(p->user_id.bytes, p->user_id.len), 0);
    }
    if (p->_flags & AMQP_BASIC_APP_ID_FLAG) {
      hv_store(props, "app_id", strlen("app_id"),
               newSVpvn(p->app_id.bytes, p->app_id.len), 0);
    }
    if (p->_flags & AMQP_BASIC_DELIVERY_MODE_FLAG) {
      hv_store(props, "delivery_mode", strlen("delivery_mode"),
               newSViv(p->delivery_mode), 0);
    }
    if (p->_flags & AMQP_BASIC_PRIORITY_FLAG) {
      hv_store(props, "priority", strlen("priority"),
               newSViv(p->priority), 0);
    }
    if (p->_flags & AMQP_BASIC_TIMESTAMP_FLAG) {
      hv_store(props, "timestamp", strlen("timestamp"),
               newSViv(p->timestamp), 0);
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
  return result;
}

MODULE = Net::RabbitMQ PACKAGE = Net::RabbitMQ PREFIX = net_rabbitmq_

REQUIRE:        1.9505
PROTOTYPES:     DISABLE

int
net_rabbitmq_connect(conn, hostname, options)
  Net::RabbitMQ conn
  char *hostname
  HV *options
  PREINIT:
    int sockfd;
    char *user = "guest";
    char *password = "guest";
    char *vhost = "/";
    int port = 5672;
    int channel_max = 0;
    int frame_max = 131072;
    int heartbeat = 0;
  CODE:
    die_on_error(aTHX_ sockfd = amqp_open_socket(hostname, port), "Opening socket");
    amqp_set_sockfd(conn, sockfd);
    str_from_hv(options, user);
    str_from_hv(options, password);
    str_from_hv(options, vhost);
    int_from_hv(options, channel_max);
    int_from_hv(options, frame_max);
    int_from_hv(options, heartbeat);
    int_from_hv(options, port);
    die_on_amqp_error(aTHX_ amqp_login(conn, vhost, channel_max, frame_max,
                                       heartbeat, AMQP_SASL_METHOD_PLAIN,
                                       user, password),
                      "Logging in");
    RETVAL = sockfd;
  OUTPUT:
    RETVAL

void
net_rabbitmq_basic_return(conn, code)
  Net::RabbitMQ conn
  SV *code
  CODE:
    SvREFCNT_inc(code);
    amqp_set_basic_return_cb(conn, internal_brcb, code);

void
net_rabbitmq_channel_open(conn, channel)
  Net::RabbitMQ conn
  int channel
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
  CODE:
    amqp_channel_open(conn, channel);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Opening channel");

void
net_rabbitmq_channel_close(conn, channel)
  Net::RabbitMQ conn
  int channel
  CODE:
    die_on_amqp_error(aTHX_ amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS), "Closing channel");

void
net_rabbitmq_exchange_declare(conn, channel, exchange, options = NULL, args = NULL)
  Net::RabbitMQ conn
  int channel
  char *exchange
  HV *options
  HV *args
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
    char *exchange_type = "direct";
    int passive = 0;
    int durable = 0;
    int auto_delete = 1;
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
  CODE:
    if(options) {
      str_from_hv(options, exchange_type);
      int_from_hv(options, passive);
      int_from_hv(options, durable);
      int_from_hv(options, auto_delete);
    }
    amqp_exchange_declare(conn, channel, amqp_cstring_bytes(exchange), amqp_cstring_bytes(exchange_type),
                          passive, durable, auto_delete, arguments);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Declaring exchange");

void
net_rabbitmq_exchange_delete(conn, channel, exchange, options = NULL)
  Net::RabbitMQ conn
  int channel
  char *exchange
  HV *options
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
    int if_unused = 1;
    int nowait = 0;
  CODE:
    if(options) {
      int_from_hv(options, if_unused);
      int_from_hv(options, nowait);
    }
    amqp_exchange_delete(conn, channel, amqp_cstring_bytes(exchange), if_unused, nowait);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Deleting exchange");

void
net_rabbitmq_queue_declare(conn, channel, queuename, options = NULL, args = NULL)
  Net::RabbitMQ conn
  int channel
  char *queuename
  HV *options
  HV *args
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
    int passive = 0;
    int durable = 0;
    int exclusive = 0;
    int auto_delete = 1;
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
    amqp_bytes_t queuename_b = AMQP_EMPTY_BYTES;
  PPCODE:
    if(queuename && strcmp(queuename, "")) queuename_b = amqp_cstring_bytes(queuename);
    if(options) {
      int_from_hv(options, passive);
      int_from_hv(options, durable);
      int_from_hv(options, exclusive);
      int_from_hv(options, auto_delete);
    }
    amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, channel, queuename_b, passive,
                                                    durable, exclusive, auto_delete,
                                                    arguments);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Declaring queue");
    XPUSHs(sv_2mortal(newSVpvn(r->queue.bytes, r->queue.len)));
    if(GIMME_V == G_ARRAY) {
      XPUSHs(sv_2mortal(newSVuv(r->message_count)));
      XPUSHs(sv_2mortal(newSVuv(r->consumer_count)));
    }

void
net_rabbitmq_queue_bind(conn, channel, queuename, exchange, bindingkey, args = NULL)
  Net::RabbitMQ conn
  int channel
  char *queuename
  char *exchange
  char *bindingkey
  HV *args
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
  CODE:
    if(queuename == NULL || exchange == NULL || bindingkey == NULL)
      Perl_croak(aTHX_ "queuename, exchange and bindingkey must all be specified");
    amqp_queue_bind(conn, channel, amqp_cstring_bytes(queuename),
                    amqp_cstring_bytes(exchange),
                    amqp_cstring_bytes(bindingkey),
                    arguments);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Binding queue");

void
net_rabbitmq_queue_unbind(conn, channel, queuename, exchange, bindingkey, args = NULL)
  Net::RabbitMQ conn
  int channel
  char *queuename
  char *exchange
  char *bindingkey
  HV *args
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
  CODE:
    if(queuename == NULL || exchange == NULL || bindingkey == NULL)
      Perl_croak(aTHX_ "queuename, exchange and bindingkey must all be specified");
    amqp_queue_unbind(conn, channel, amqp_cstring_bytes(queuename),
                      amqp_cstring_bytes(exchange),
                    amqp_cstring_bytes(bindingkey),
                    arguments);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Unbinding queue");

SV *
net_rabbitmq_consume(conn, channel, queuename, options = NULL)
  Net::RabbitMQ conn
  int channel
  char *queuename
  HV *options
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
    amqp_basic_consume_ok_t *r;
    char *consumer_tag = NULL;
    int no_local = 0;
    int no_ack = 1;
    int exclusive = 0;
  CODE:
    if(options) {
      str_from_hv(options, consumer_tag);
      int_from_hv(options, no_local);
      int_from_hv(options, no_ack);
      int_from_hv(options, exclusive);
    }
    r = amqp_basic_consume(conn, channel, amqp_cstring_bytes(queuename),
                           consumer_tag ? amqp_cstring_bytes(consumer_tag) : AMQP_EMPTY_BYTES,
                           no_local, no_ack, exclusive);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Consume queue");
    RETVAL = newSVpvn(r->consumer_tag.bytes, r->consumer_tag.len);
  OUTPUT:
    RETVAL

HV *
net_rabbitmq_recv(conn)
  Net::RabbitMQ conn
  PREINIT:
    int result = 0;
  CODE:
    RETVAL = newHV();
    sv_2mortal((SV*)RETVAL);
    result = internal_recv(RETVAL, conn, 0);
    if(result <= 0) Perl_croak(aTHX_ "Bad frame read.");
  OUTPUT:
    RETVAL

void
net_rabbitmq_ack(conn, channel, delivery_tag, multiple = 0)
  Net::RabbitMQ conn
  int channel
  SV *delivery_tag
  int multiple
  PREINIT:
    STRLEN len;
    uint64_t tag;
    unsigned char *l;
  CODE:
    l = SvPV(delivery_tag, len);
    if(len != sizeof(tag)) Perl_croak(aTHX_ "bad tag");
    memcpy(&tag, l, sizeof(tag));
    die_on_error(aTHX_ amqp_basic_ack(conn, channel, tag, multiple),
                 "ack");

void
net_rabbitmq_purge(conn, channel, queuename, no_wait = 0)
  Net::RabbitMQ conn
  int channel
  char *queuename
  int no_wait
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
  CODE:
    amqp_queue_purge(conn, channel, amqp_cstring_bytes(queuename), no_wait);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Purging queue");

int
net_rabbitmq_publish(conn, channel, routing_key, body, options = NULL, props = NULL)
  Net::RabbitMQ conn
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
    amqp_bytes_t exchange_b = { 0 };
    amqp_bytes_t routing_key_b;
    amqp_bytes_t body_b;
    struct amqp_basic_properties_t_ properties;
    STRLEN len;
  CODE:
    routing_key_b = amqp_cstring_bytes(routing_key);
    body_b.bytes = SvPV(body, len);
    body_b.len = len;
    if(options) {
      if(NULL != (v = hv_fetch(options, "mandatory", strlen("mandatory"), 0))) mandatory = SvIV(*v) ? 1 : 0;
      if(NULL != (v = hv_fetch(options, "immediate", strlen("immediate"), 0))) immediate = SvIV(*v) ? 1 : 0;
      if(NULL != (v = hv_fetch(options, "exchange", strlen("exchange"), 0))) exchange_b = amqp_cstring_bytes(SvPV_nolen(*v));
    }
    properties.headers = AMQP_EMPTY_TABLE;
    properties._flags = 0;
    if (props) {
      if (NULL != (v = hv_fetch(props, "content_type", strlen("content_type"), 0))) {
        properties.content_type     = amqp_cstring_bytes(SvPV_nolen(*v));
        properties._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "content_encoding", strlen("content_encoding"), 0))) {
        properties.content_encoding = amqp_cstring_bytes(SvPV_nolen(*v));
        properties._flags |= AMQP_BASIC_CONTENT_ENCODING_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "correlation_id", strlen("correlation_id"), 0))) {
        properties.correlation_id   =  amqp_cstring_bytes(SvPV_nolen(*v));
        properties._flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "reply_to", strlen("reply_to"), 0))) {
        properties.reply_to         = amqp_cstring_bytes(SvPV_nolen(*v));
        properties._flags |= AMQP_BASIC_REPLY_TO_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "expiration", strlen("expiration"), 0))) {
        properties.expiration       = amqp_cstring_bytes(SvPV_nolen(*v));
        properties._flags |= AMQP_BASIC_EXPIRATION_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "message_id", strlen("message_id"), 0))) {
        properties.message_id       = amqp_cstring_bytes(SvPV_nolen(*v));
        properties._flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "type", strlen("type"), 0))) {
        properties.type             = amqp_cstring_bytes(SvPV_nolen(*v));
        properties._flags |= AMQP_BASIC_TYPE_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "user_id", strlen("user_id"), 0))) {
        properties.user_id          = amqp_cstring_bytes(SvPV_nolen(*v));
        properties._flags |= AMQP_BASIC_USER_ID_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "app_id", strlen("app_id"), 0))) {
        properties.app_id           = amqp_cstring_bytes(SvPV_nolen(*v));
        properties._flags |= AMQP_BASIC_APP_ID_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "delivery_mode", strlen("delivery_mode"), 0))) {
        properties.delivery_mode    = (uint8_t) SvIV(*v);
        properties._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "priority", strlen("priority"), 0))) {
        properties.priority         = (uint8_t) SvIV(*v);
        properties._flags |= AMQP_BASIC_PRIORITY_FLAG;
      }
      if (NULL != (v = hv_fetch(props, "timestamp", strlen("timestamp"), 0))) {
        properties.timestamp        = (uint64_t) SvIV(*v);
        properties._flags |= AMQP_BASIC_TIMESTAMP_FLAG;
      }
    }
    rv = amqp_basic_publish(conn, channel, exchange_b, routing_key_b, mandatory, immediate, &properties, body_b);
    RETVAL = rv;
  OUTPUT:
    RETVAL

SV *
net_rabbitmq_get(conn, channel, queuename, options = NULL)
  Net::RabbitMQ conn
  int channel
  char *queuename
  HV *options
  PREINIT:
    amqp_rpc_reply_t r;
    int no_ack = 1;
  CODE:
    if(options)
      int_from_hv(options, no_ack);
    amqp_maybe_release_buffers(conn);
    r = amqp_basic_get(conn, channel, queuename ? amqp_cstring_bytes(queuename) : AMQP_EMPTY_BYTES, no_ack);
    die_on_amqp_error(aTHX_ r, "basic_get");
    if(r.reply.id == AMQP_BASIC_GET_OK_METHOD) {
      HV *hv;
      amqp_basic_get_ok_t *ok = (amqp_basic_get_ok_t *)r.reply.decoded;
      hv = newHV();
      hv_store(hv, "delivery_tag", strlen("delivery_tag"), newSVpvn((const char *)&ok->delivery_tag, sizeof(ok->delivery_tag)), 0);
      hv_store(hv, "redelivered", strlen("redelivered"), newSViv(ok->redelivered), 0);
      hv_store(hv, "exchange", strlen("exchange"), newSVpvn(ok->exchange.bytes, ok->exchange.len), 0);
      hv_store(hv, "routing_key", strlen("routing_key"), newSVpvn(ok->routing_key.bytes, ok->routing_key.len), 0);
      hv_store(hv, "message_count", strlen("message_count"), newSViv(ok->message_count), 0);
      if(amqp_data_in_buffer(conn)) {
        int rv;
        rv = internal_recv(hv, conn, 1);
        if(rv <= 0) Perl_croak(aTHX_ "Bad frame read.");
      }
      RETVAL = (SV *)newRV_noinc((SV *)hv);
    }
    else
      RETVAL = &PL_sv_undef;
  OUTPUT:
    RETVAL

int
net_rabbitmq_get_channel_max(conn)
  Net::RabbitMQ conn
  CODE:
    RETVAL = amqp_get_channel_max(conn);
  OUTPUT:
    RETVAL

void
net_rabbitmq_disconnect(conn)
  Net::RabbitMQ conn
  PREINIT:
    int sockfd;
  CODE:
    die_on_amqp_error(aTHX_ amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
    sockfd = amqp_get_sockfd(conn);
    if(sockfd >= 0) close(sockfd);
    amqp_set_sockfd(conn,-1);

Net::RabbitMQ
net_rabbitmq_new(clazz)
  char *clazz
  CODE:
    RETVAL = amqp_new_connection();
  OUTPUT:
    RETVAL

void
net_rabbitmq_DESTROY(conn)
  Net::RabbitMQ conn
  PREINIT:
    int sockfd;
  CODE:
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    sockfd = amqp_get_sockfd(conn);
    if(sockfd >= 0) close(sockfd);
    amqp_destroy_connection(conn);

void
net_rabbitmq_tx_select(conn, channel, args = NULL)
  Net::RabbitMQ conn
  int channel
  HV *args
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
  CODE:
    amqp_tx_select(conn, channel, arguments);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Selecting transaction");

void
net_rabbitmq_tx_commit(conn, channel, args = NULL)
  Net::RabbitMQ conn
  int channel
  HV *args
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
  CODE:
    amqp_tx_commit(conn, channel, arguments);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Commiting transaction");

void
net_rabbitmq_tx_rollback(conn, channel, args = NULL)
  Net::RabbitMQ conn
  int channel
  HV *args
  PREINIT:
    amqp_rpc_reply_t *amqp_rpc_reply;
    amqp_table_t arguments = AMQP_EMPTY_TABLE;
  CODE:
    amqp_tx_rollback(conn, channel, arguments);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Rolling Back transaction");

void
net_rabbitmq_basic_qos(conn, channel, args = NULL)
  Net::RabbitMQ conn
  int channel
  HV *args
  PREINIT:
    SV **v;
    amqp_rpc_reply_t *amqp_rpc_reply;
    uint32_t prefetch_size = 0;
    uint16_t prefetch_count = 0;
    amqp_boolean_t global = 0;
  CODE:
    if(args) {
      if(NULL != (v = hv_fetch(args, "prefetch_size", strlen("prefetch_size"), 0))) prefetch_size = SvIV(*v);
      if(NULL != (v = hv_fetch(args, "prefetch_count", strlen("prefetch_count"), 0))) prefetch_count = SvIV(*v);
      if(NULL != (v = hv_fetch(args, "global", strlen("global"), 0))) global = SvIV(*v) ? 1 : 0;
    }
    amqp_basic_qos(conn, channel, 
                   prefetch_size, prefetch_count, global);
    amqp_rpc_reply = amqp_get_rpc_reply();
    die_on_amqp_error(aTHX_ *amqp_rpc_reply, "Basic QoS");
