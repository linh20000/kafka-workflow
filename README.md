# wf/kafka

Laravel Kafka package — Transactional Outbox, Batch Relayer, Consumer Dispatcher với DLQ/EDL routing và Telegram alerting.

[![PHP](https://img.shields.io/badge/PHP-%5E8.2-blue)](https://php.net)
[![Laravel](https://img.shields.io/badge/Laravel-10%20%7C%2011-red)](https://laravel.com)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

## Yêu cầu

| Thành phần | Phiên bản |
|---|---|
| PHP | ^8.2 |
| Laravel | 10 hoặc 11 |
| librdkafka | ≥ 1.0 |
| ext-rdkafka | (cài qua PECL) |

> `ext-rdkafka` là C extension, **không thể cài qua Composer**. Xem hướng dẫn bên dưới.

---

## Cài đặt

### Bước 1 — Cài librdkafka + ext-rdkafka

Package cung cấp sẵn 3 cách:

**Cách A — Artisan command (sau khi đã `composer require`)**
```bash
php artisan kafka:install          # Detect OS → cài tự động
php artisan kafka:install --check  # Chỉ kiểm tra, không cài
php artisan kafka:install --force  # Không hỏi confirm
```

**Cách B — Shell script (CI/CD, server, trước khi có Laravel)**
```bash
bash vendor/wf/kafka/scripts/install-rdkafka.sh
bash vendor/wf/kafka/scripts/install-rdkafka.sh --check
```

**Cách C — Composer scripts shortcut**
```bash
composer kafka:install
composer kafka:check
```

**Cách D — Docker** (copy `scripts/Dockerfile.example` vào project của bạn)
```dockerfile
FROM php:8.2-fpm-alpine AS base

RUN apk add --no-cache librdkafka librdkafka-dev $PHPIZE_DEPS \
    && pecl install rdkafka \
    && docker-php-ext-enable rdkafka \
    && apk del --no-cache $PHPIZE_DEPS librdkafka-dev \
    && rm -rf /tmp/pear /var/cache/apk/*
```

OS được hỗ trợ tự động: **macOS** (Homebrew), **Ubuntu/Debian** (apt), **Alpine** (apk), **RHEL/CentOS/Amazon Linux** (yum/dnf).

---

### Bước 2 — Cài package

```bash
composer require wf/kafka
```

---

### Bước 3 — Publish config và migrate

```bash
# Publish config vào config/kafka.php của project
php artisan vendor:publish --tag=kafka-config

# Chạy migrations (tạo 3 bảng kafka_*)
php artisan migrate
```

> Nếu không publish config, package dùng giá trị mặc định từ nội bộ.  
> Nếu không publish migrations, package tự load migrations — không cần publish nếu không cần customize.

---

### Bước 4 — Thêm vào `.env`

```dotenv
# ── Kafka Connection ─────────────────────────────────────────────────────
KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094

# PLAINTEXT | SASL_PLAINTEXT | SASL_SSL
KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# Chỉ điền khi dùng SASL_PLAINTEXT hoặc SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=app_client
KAFKA_SASL_PASSWORD=your_password

# ── Producer ─────────────────────────────────────────────────────────────
KAFKA_PRODUCER_ACKS=-1              # -1 = all ISR replicas (At-Least-Once)
KAFKA_PRODUCER_TIMEOUT_MS=5000
KAFKA_PRODUCER_RETRIES=3
KAFKA_PRODUCER_RETRY_BACKOFF_MS=500

# ── Consumer ─────────────────────────────────────────────────────────────
KAFKA_CONSUMER_GROUP_ID=my-service-group
KAFKA_AUTO_OFFSET_RESET=latest      # latest | earliest
KAFKA_CONSUMER_POLL_TIMEOUT_MS=1000

# ── DLQ ──────────────────────────────────────────────────────────────────
KAFKA_DLQ_SUFFIX=.dlq               # order-events → order-events.dlq

# ── Outbox Relayer ────────────────────────────────────────────────────────
KAFKA_OUTBOX_MAX_ATTEMPTS=3
KAFKA_OUTBOX_BATCH_SIZE=100
KAFKA_OUTBOX_RELAY_INTERVAL=5

# ── Serialization ─────────────────────────────────────────────────────────
KAFKA_SERIALIZATION_DRIVER=json     # json (default) | avro

# Chỉ dùng khi KAFKA_SERIALIZATION_DRIVER=avro
KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_SCHEMA_REGISTRY_USERNAME=
KAFKA_SCHEMA_REGISTRY_PASSWORD=
KAFKA_AVRO_SUBJECT_STRATEGY=topic_name   # topic_name | record_name | topic_record
KAFKA_AVRO_SCHEMA_CACHE=true

# ── Telegram Alerting ─────────────────────────────────────────────────────
KAFKA_TELEGRAM_ENABLED=false
KAFKA_TELEGRAM_BOT_TOKEN=
KAFKA_TELEGRAM_CHAT_ID=
KAFKA_TELEGRAM_ALERT_ON=dlq,edl
```

---

## PRODUCER

Producer có **hai trách nhiệm duy nhất**: ghi vào Outbox và Relay lên Kafka.  
Không gọi Kafka trực tiếp từ request — không bao giờ.

### Ghi một message (trong DB transaction)

```php
use Wf\Kafka\Producer\OutboxWriter;
use Wf\Kafka\Producer\ProducerOptions;

DB::transaction(function () use ($order) {
    // Business logic của bạn
    $order = Order::create($orderData);

    // Ghi vào outbox — cùng transaction với business logic
    OutboxWriter::write(
        topic:     'order-events',
        eventType: 'ORDER_CREATED',
        payload:   $order->toArray(),
        options:   ProducerOptions::make()->withKey((string) $order->id)
    );
});
```

### Ghi nhiều message cùng topic (batch — một INSERT)

```php
DB::transaction(function () use ($orders) {
    // ...
    $payloads = collect($orders)->map->toArray()->all();

    OutboxWriter::writeBatch(
        topic:     'order-events',
        eventType: 'ORDER_CREATED',
        payloads:  $payloads
    );
    // Trả về: int — số bản ghi đã ghi
});
```

### Ghi nhiều message với topic/event_type khác nhau

```php
DB::transaction(function () {
    OutboxWriter::writeMulti([
        ['topic' => 'order-events',    'event_type' => 'ORDER_CREATED',   'payload' => [...]],
        ['topic' => 'payment-events',  'event_type' => 'PAYMENT_CHARGED', 'payload' => [...]],
        ['topic' => 'inventory-events','event_type' => 'STOCK_RESERVED',  'payload' => [...]],
    ]);
    // Trả về: int — số bản ghi đã ghi
});
```

### ProducerOptions — tuỳ chỉnh per-message, độc lập với core config

```php
$options = ProducerOptions::make()
    ->withKey('order-123')               // Partition key — đảm bảo ordering
    ->withHeaders(['source' => 'api'])   // Kafka message headers
    ->withFlushTimeoutMs(3000)           // Override flush timeout (ms)
    ->withPartition(2);                  // Gửi vào partition cụ thể (hiếm khi cần)
```

### Chạy Relayer

Relayer đọc `PENDING`/`FAILED` records từ `kafka_outbox` và publish lên Kafka theo batch — một `flush()` duy nhất mỗi batch, group theo topic.

```bash
# Chạy một lần (dùng với Laravel Scheduler)
php artisan kafka:outbox-relay

# Chạy liên tục mỗi N giây (dùng với Supervisor)
php artisan kafka:outbox-relay --loop --interval=3
```

**Dùng Laravel Scheduler:**
```php
// app/Console/Kernel.php
$schedule->command('kafka:outbox-relay')->everyFiveSeconds()->withoutOverlapping();
```

**Dùng Supervisor:**
```ini
[program:kafka-outbox-relay]
command=php /var/www/artisan kafka:outbox-relay --loop --interval=3
autostart=true
autorestart=true
numprocs=1
```

---

## CONSUMER

Consumer **không xử lý business logic**. Package đảm nhiệm:
- Poll message từ Kafka
- Deserialize binary → array gốc (JSON hoặc Avro)
- Kiểm tra idempotency (bảng `kafka_processed_events`)
- Giao `payload` đã deserialize cho `handle()` của bạn
- Tự động route lỗi → DLQ hoặc EDL
- Gửi Telegram alert
- Manual commit offset

### Cơ bản

```php
use Wf\Kafka\Consumer\ConsumerDispatcher;

app(ConsumerDispatcher::class)
    ->onTopic('order-events')
    ->withGroup('order-service-group')
    ->handle(function (array $payload, array $meta) {
        // $payload — dữ liệu gốc đã deserialize hoàn toàn
        // $meta    — ['event_id', 'event_type', 'topic', 'partition', 'offset']

        OrderService::process($payload);
    })
    ->listen(); // blocking loop
```

### Báo lỗi để package route đúng

```php
use Wf\Kafka\Exceptions\TransientInfraException;
use Wf\Kafka\Exceptions\PoisonPillException;

->handle(function (array $payload, array $meta) {
    // Lỗi tạm thời (mạng, API timeout, DB down)
    // → Package route vào DLQ, KHÔNG commit offset
    // → Message được re-deliver khi consumer restart
    if ($apiIsDown) {
        throw new TransientInfraException("Payment API timeout sau 3s.");
    }

    // Dữ liệu không thể xử lý được (Poison Pill)
    // → Package route vào EDL, commit offset (không block queue)
    if (!in_array($payload['status'], ['PENDING', 'COMPLETED'])) {
        throw new PoisonPillException("Unknown status: {$payload['status']}");
    }

    // Bất kỳ exception nào khác cũng → EDL
    OrderService::process($payload);
})
```

| Exception | Hành động | Commit offset? |
|---|---|---|
| `TransientInfraException` | → DLQ topic + DB log + Telegram | ❌ Không |
| `PoisonPillException` | → EDL DB log + Telegram | ✅ Có |
| Bất kỳ `\Throwable` nào khác | → EDL DB log + Telegram | ✅ Có |
| Duplicate `event_id` | Bỏ qua silently | ✅ Có |

### Hook onError (tuỳ chọn)

```php
->onError(function (string $type, array $envelope, \Throwable $e) {
    // $type = 'dlq' | 'edl'
    // Dùng để notify thêm, ghi log riêng, alert Sentry...
    Sentry::captureException($e);
})
```

### Tạo Artisan Command cho consumer

```php
// app/Console/Commands/OrderConsumerCommand.php
namespace App\Console\Commands;

use Illuminate\Console\Command;
use Wf\Kafka\Consumer\ConsumerDispatcher;

class OrderConsumerCommand extends Command
{
    protected $signature   = 'consumer:orders';
    protected $description = 'Consume order-events topic';

    public function handle(ConsumerDispatcher $dispatcher): void
    {
        $dispatcher
            ->onTopic('order-events')
            ->withGroup('order-service-group')
            ->handle(function (array $payload, array $meta) {
                OrderService::process($payload);
            })
            ->onError(function (string $type, array $envelope, \Throwable $e) {
                Log::channel('slack')->critical("[$type] {$e->getMessage()}");
            })
            ->listen();
    }
}
```

Chạy với Supervisor:
```ini
[program:consumer-orders]
command=php /var/www/artisan consumer:orders
autostart=true
autorestart=true
numprocs=2
stdout_logfile=/var/log/consumer-orders.log
```

---

## SERIALIZATION

Package hỗ trợ 2 driver, cùng interface — chuyển đổi chỉ cần đổi `.env`.

### JSON (mặc định)

```dotenv
KAFKA_SERIALIZATION_DRIVER=json
```

Wire format: JSON UTF-8 string. Không cần cấu hình thêm.

---

### Avro Binary (Confluent wire format)

```dotenv
KAFKA_SERIALIZATION_DRIVER=avro
KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_SCHEMA_REGISTRY_USERNAME=      # Để trống nếu không có auth
KAFKA_SCHEMA_REGISTRY_PASSWORD=
```

Wire format: `[0x00][schema_id: 4 bytes][avro binary]` — chuẩn Confluent.

**Dependency**: `flix-tech/avro-php ^5.1` đã có sẵn trong `composer.json require` — không cần cài thêm.

**Subject naming strategy:**
```dotenv
KAFKA_AVRO_SUBJECT_STRATEGY=topic_name   # order-events → order-events-value (default)
KAFKA_AVRO_SUBJECT_STRATEGY=record_name  # ORDER_CREATED → ORDER_CREATED-value
KAFKA_AVRO_SUBJECT_STRATEGY=topic_record # order-events-ORDER_CREATED-value
```

**Schema cache** (giảm HTTP call xuống Registry):
```dotenv
KAFKA_AVRO_SCHEMA_CACHE=true   # Cache 1 giờ (default: true)
```

**Local schema files** (không fetch từ Registry):
```php
// config/kafka.php
'serialization' => [
    'driver' => 'avro',
    'avro' => [
        'schemas' => [
            'order-events'   => base_path('avro/order.avsc'),
            'payment-events' => base_path('avro/payment.avsc'),
        ],
    ],
],
```

---

## REDRIVE

Đẩy lại message bị lỗi từ `kafka_failed_event_logs` vào outbox để Relayer publish lại.

```bash
# Xem trước — không thực sự redrive
php artisan kafka:redrive --type=dlq --dry-run

# Redrive tất cả DLQ records
php artisan kafka:redrive --type=dlq

# Redrive chỉ một topic cụ thể
php artisan kafka:redrive --type=dlq --topic=order-events

# Redrive EDL (Poison Pills đã được fix bằng tay)
php artisan kafka:redrive --type=edl --limit=50
```

Status flow của `kafka_failed_event_logs`:
```
PENDING_FIX → REDRIVEN (sau kafka:redrive)
           → FIXED     (sau khi consumer xử lý thành công lần redrive)
           → IGNORED   (đánh dấu thủ công, bỏ qua vĩnh viễn)
```

---

## TELEGRAM ALERTING

```dotenv
KAFKA_TELEGRAM_ENABLED=true
KAFKA_TELEGRAM_BOT_TOKEN=123456:ABC-DEF...   # Từ @BotFather
KAFKA_TELEGRAM_CHAT_ID=-1001234567890        # Group/channel/user ID
KAFKA_TELEGRAM_ALERT_ON=dlq,edl             # Chỉ nhận loại nào
```

Alert DLQ:
```
⚠️ DLQ ALERT

📌 Topic: `order-events`
🔑 Event ID: `evt_abc-123`
❗ Reason: Payment API timeout sau 3s.
Message sẽ được retry sau khi hạ tầng ổn định.
```

Alert EDL:
```
🚨 EDL ALERT — Poison Pill

📌 Topic: `order-events`
🔑 Event ID: `evt_xyz-456`
❗ Reason: Unknown status: GHOST
Message đã bị cách ly xuống DB. Cần can thiệp thủ công.
```

---

## DATABASE TABLES

Package tạo 3 bảng với prefix `kafka_` — không xung đột với schema của host project.

| Bảng | Mô tả |
|---|---|
| `kafka_outbox` | Message chờ Relayer publish. `status`: PENDING → SENT / FAILED |
| `kafka_processed_events` | Idempotency guard. `event_id` là primary key |
| `kafka_failed_event_logs` | DLQ + EDL records. `failure_type`: DLQ / EDL |

Publish migrations nếu cần customize:
```bash
php artisan vendor:publish --tag=kafka-migrations
```

---

## ARTISAN COMMANDS

| Command | Mô tả |
|---|---|
| `kafka:install` | Kiểm tra và cài librdkafka + ext-rdkafka |
| `kafka:install --check` | Chỉ kiểm tra môi trường |
| `kafka:install --force` | Cài không hỏi confirm |
| `kafka:outbox-relay` | Relay outbox records lên Kafka (một lần) |
| `kafka:outbox-relay --loop` | Relay liên tục (dùng với Supervisor) |
| `kafka:outbox-relay --loop --interval=3` | Loop mỗi 3 giây |
| `kafka:redrive --type=dlq` | Redrive DLQ records vào outbox |
| `kafka:redrive --type=edl` | Redrive EDL records vào outbox |
| `kafka:redrive --dry-run` | Xem trước không thực hiện |

---

## CẤU TRÚC PACKAGE

```
wf/kafka
├── composer.json
├── config/
│   └── kafka.php                        ← Publish vào project host
├── database/migrations/
│   ├── ..._create_kafka_outbox_table.php
│   ├── ..._create_kafka_processed_events_table.php
│   └── ..._create_kafka_failed_event_logs_table.php
├── scripts/
│   ├── install-rdkafka.sh               ← Shell installer (CI/CD)
│   └── Dockerfile.example               ← Docker reference
└── src/
    ├── KafkaServiceProvider.php
    ├── Alerting/
    │   └── TelegramNotifier.php         ← DLQ/EDL → Telegram
    ├── Console/Commands/
    │   ├── KafkaInstallCommand.php      ← kafka:install
    │   ├── OutboxRelayCommand.php       ← kafka:outbox-relay
    │   └── KafkaRedriveCommand.php      ← kafka:redrive
    ├── Consumer/
    │   └── ConsumerDispatcher.php       ← Poll → deserialize → handle() → DLQ/EDL
    ├── Exceptions/
    │   ├── TransientInfraException.php  ← Host throw → DLQ
    │   ├── PoisonPillException.php      ← Host throw → EDL
    │   └── DeserializationException.php ← Package throw → EDL
    ├── Facades/
    │   ├── KafkaProducer.php            ← Alias OutboxWriter
    │   └── KafkaConsumer.php            ← Alias ConsumerDispatcher
    ├── Kafka/
    │   └── KafkaConfig.php              ← Build RdKafka\Conf từ config array
    ├── Models/
    │   ├── OutboxEvent.php
    │   ├── ProcessedEvent.php
    │   └── FailedEventLog.php
    ├── Producer/
    │   ├── OutboxWriter.php             ← write() / writeBatch() / writeMulti()
    │   ├── OutboxRelayer.php            ← Batch relay outbox → Kafka
    │   └── ProducerOptions.php          ← Per-message options (key/headers/partition)
    └── Serialization/
        ├── MessageSerializer.php        ← Interface
        ├── JsonSerializer.php           ← Driver: json
        ├── AvroSerializer.php           ← Driver: avro (Confluent wire format)
        └── SerializerFactory.php        ← Tạo driver từ config
```

---

## FLOW TỔNG THỂ

```
[Host: business logic + DB::transaction()]
              │
              ▼  OutboxWriter::write/writeBatch/writeMulti()
              │  serialize(payload) → binary string
              ▼
    [kafka_outbox: PENDING]
              │
              ▼  OutboxRelayer (kafka:outbox-relay --loop)
              │  getRawPayload() → gửi thẳng, bit-perfect
              ▼
         [Kafka Topic]
              │
              ▼  ConsumerDispatcher (Artisan command + Supervisor)
              │  poll() → deserialize() → array gốc
              ▼
    [handle(payload, meta)]  ← Host xử lý business logic
              │
     ┌────────┴──────────────────────────────────────────┐
     │                                                   │
     ▼ OK                                       ▼ Exception
[commit offset]                    TransientInfraException
   [kafka_processed_events]                    │
                                        [DLQ topic]
                                    [kafka_failed_event_logs: DLQ]
                                    [Telegram ⚠️]
                                               │
                                    PoisonPillException / other
                                               │
                                    [kafka_failed_event_logs: EDL]
                                    [Telegram 🚨]
                                               │
                                    kafka:redrive → [kafka_outbox] ↩
```

---

## LICENSE

MIT © Linh NQ
