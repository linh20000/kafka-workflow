<?php

/*
|--------------------------------------------------------------------------
| wf/kafka — Package Configuration
|--------------------------------------------------------------------------
|
| Publish config vào project của bạn:
|   php artisan vendor:publish --tag=kafka-config
|
| Sau đó chỉnh các giá trị tại config/kafka.php của project host
| và thêm các biến tương ứng vào .env.
|
*/

return [

    // =========================================================================
    // KAFKA CONNECTION
    // =========================================================================

    /*
     | Danh sách Kafka broker, cách nhau bởi dấu phẩy.
     | .env: KAFKA_BROKERS
     */
    'brokers' => env('KAFKA_BROKERS', 'localhost:9092'),

    /*
     | PLAINTEXT       — Không mã hoá, không auth (local/dev)
     | SASL_PLAINTEXT  — Auth SASL, không mã hoá
     | SASL_SSL        — Auth SASL + TLS (production)
     | .env: KAFKA_SECURITY_PROTOCOL
     */
    'security_protocol' => env('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT'),

    /*
     | SASL credentials — chỉ dùng khi security_protocol != PLAINTEXT
     | .env: KAFKA_SASL_MECHANISM, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD
     */
    'sasl' => [
        'mechanism' => env('KAFKA_SASL_MECHANISM', 'SCRAM-SHA-512'),
        'username'  => env('KAFKA_SASL_USERNAME', ''),
        'password'  => env('KAFKA_SASL_PASSWORD', ''),
    ],

    // =========================================================================
    // PRODUCER — Outbox writer settings
    // =========================================================================
    'producer' => [
        /*
         | acks:
         |   0  = fire-and-forget
         |   1  = chỉ leader xác nhận
         |  -1  = tất cả ISR replica (At-Least-Once, recommended)
         | .env: KAFKA_PRODUCER_ACKS
         */
        'acks' => env('KAFKA_PRODUCER_ACKS', '-1'),

        /*
         | Timeout tổng cho một message được deliver (ms)
         | .env: KAFKA_PRODUCER_TIMEOUT_MS
         */
        'message_timeout_ms' => (int) env('KAFKA_PRODUCER_TIMEOUT_MS', 5000),

        /*
         | Số lần rdkafka tự retry khi gặp lỗi transient
         | .env: KAFKA_PRODUCER_RETRIES
         */
        'retries' => (int) env('KAFKA_PRODUCER_RETRIES', 3),

        /*
         | Delay giữa các lần retry (ms)
         | .env: KAFKA_PRODUCER_RETRY_BACKOFF_MS
         */
        'retry_backoff_ms' => (int) env('KAFKA_PRODUCER_RETRY_BACKOFF_MS', 500),
    ],

    // =========================================================================
    // CONSUMER — Dispatcher settings
    // =========================================================================
    'consumer' => [
        /*
         | Consumer group mặc định — mỗi service/topic nên dùng group riêng.
         | Khi dùng KafkaConsumer::withGroup() thì giá trị này bị override.
         | .env: KAFKA_CONSUMER_GROUP_ID
         */
        'group_id' => env('KAFKA_CONSUMER_GROUP_ID', 'laravel-consumer-group'),

        /*
         | latest   = chỉ đọc message mới từ lúc consumer join
         | earliest = đọc từ đầu topic
         | .env: KAFKA_AUTO_OFFSET_RESET
         */
        'auto_offset_reset' => env('KAFKA_AUTO_OFFSET_RESET', 'latest'),

        /*
         | Timeout mỗi lần poll (ms)
         | .env: KAFKA_CONSUMER_POLL_TIMEOUT_MS
         */
        'poll_timeout_ms' => (int) env('KAFKA_CONSUMER_POLL_TIMEOUT_MS', 1000),

        /*
         | Số message tối đa xử lý trong một batch trước khi commit
         | .env: KAFKA_CONSUMER_BATCH_SIZE
         */
        'batch_size' => (int) env('KAFKA_CONSUMER_BATCH_SIZE', 10),
    ],

    // =========================================================================
    // DLQ (Dead Letter Queue) — Topic nhận message lỗi transient
    // =========================================================================
    'dlq' => [
        /*
         | Suffix thêm vào tên topic gốc để tạo DLQ topic.
         | Ví dụ: 'order-events' → 'order-events.dlq'
         | .env: KAFKA_DLQ_SUFFIX
         */
        'suffix' => env('KAFKA_DLQ_SUFFIX', '.dlq'),
    ],

    // =========================================================================
    // OUTBOX RELAYER — Relayer service settings
    // =========================================================================
    'outbox' => [
        /*
         | Số lần retry publish trước khi bỏ hẳn bản ghi.
         | .env: KAFKA_OUTBOX_MAX_ATTEMPTS
         */
        'max_attempts' => (int) env('KAFKA_OUTBOX_MAX_ATTEMPTS', 3),

        /*
         | Số bản ghi xử lý mỗi lần relayer chạy (batch relay).
         | .env: KAFKA_OUTBOX_BATCH_SIZE
         */
        'batch_size' => (int) env('KAFKA_OUTBOX_BATCH_SIZE', 100),

        /*
         | Khoảng thời gian giữa các lần scan khi dùng --loop (giây).
         | .env: KAFKA_OUTBOX_RELAY_INTERVAL
         */
        'relay_interval_seconds' => (int) env('KAFKA_OUTBOX_RELAY_INTERVAL', 5),
    ],

    // =========================================================================
    // SERIALIZATION
    // =========================================================================
    'serialization' => [
        /*
         | Driver tuần tự hoá message trước khi ghi vào outbox và gửi lên Kafka.
         |
         | 'json'  — JSON UTF-8 (mặc định, không cần thêm dependency)
         | 'avro'  — Confluent Avro binary với Schema Registry
         |           Yêu cầu: composer require flix-tech/avro-serde-php
         |
         | .env: KAFKA_SERIALIZATION_DRIVER
         */
        'driver' => env('KAFKA_SERIALIZATION_DRIVER', 'json'),

        /*
         | Cấu hình Avro — chỉ dùng khi driver = 'avro'
         */
        'avro' => [
            /*
             | URL của Confluent Schema Registry.
             | .env: KAFKA_SCHEMA_REGISTRY_URL
             */
            'registry_url' => env('KAFKA_SCHEMA_REGISTRY_URL', 'http://localhost:8081'),

            /*
             | Basic auth cho Schema Registry (nếu có).
             | .env: KAFKA_SCHEMA_REGISTRY_USERNAME, KAFKA_SCHEMA_REGISTRY_PASSWORD
             */
            'registry_username' => env('KAFKA_SCHEMA_REGISTRY_USERNAME', ''),
            'registry_password' => env('KAFKA_SCHEMA_REGISTRY_PASSWORD', ''),

            /*
             | Subject naming strategy.
             | 'topic_name'   — <topic>-value  (Confluent default)
             | 'record_name'  — dùng tên record Avro
             | 'topic_record' — <topic>-<record>
             | .env: KAFKA_AVRO_SUBJECT_STRATEGY
             */
            'subject_strategy' => env('KAFKA_AVRO_SUBJECT_STRATEGY', 'topic_name'),

            /*
             | Cache schema cục bộ để giảm HTTP call xuống Registry.
             | .env: KAFKA_AVRO_SCHEMA_CACHE
             */
            'schema_cache' => (bool) env('KAFKA_AVRO_SCHEMA_CACHE', true),


            /*
             | Map topic → đường dẫn file .avsc (schema cục bộ).
             | Nếu không khai báo, package tự fetch từ Registry.
             |
             | Ví dụ:
             |   'schemas' => [
             |       'order-events' => base_path('avro/order.avsc'),
             |   ],
             */
            'schemas' => [],
        ],

    ],

    // =========================================================================
    // TELEGRAM ALERTING
    // =========================================================================
    'telegram' => [
        /*
         | Bật/tắt Telegram alerting.
         | .env: KAFKA_TELEGRAM_ENABLED
         */
        'enabled' => (bool) env('KAFKA_TELEGRAM_ENABLED', false),

        /*
         | Bot token từ @BotFather
         | .env: KAFKA_TELEGRAM_BOT_TOKEN
         */
        'bot_token' => env('KAFKA_TELEGRAM_BOT_TOKEN', ''),

        /*
         | Chat ID nhận alert (cá nhân, group, hoặc channel).
         | .env: KAFKA_TELEGRAM_CHAT_ID
         */
        'chat_id' => env('KAFKA_TELEGRAM_CHAT_ID', ''),

        /*
         | Chỉ gửi alert khi message vào các level này.
         | Các level hợp lệ: 'dlq', 'edl' (hoặc cả hai).
         | .env: KAFKA_TELEGRAM_ALERT_ON (comma-separated)
         */
        'alert_on' => explode(',', env('KAFKA_TELEGRAM_ALERT_ON', 'dlq,edl')),
    ],

];
