<?php

namespace Wf\Kafka\Console\Commands;

use Illuminate\Console\Command;

class KafkaDebeziumConfigCommand extends Command
{
    protected $signature = 'kafka:debezium-config';

    protected $description = '[wf-kafka] Sinh cấu hình Debezium Kafka Connect từ file config';

    public function handle(): int
    {
        $dbConnection = config('database.default');
        $dbConfig = config("database.connections.{$dbConnection}");
        
        $kafkaConfig = config('wf-kafka.debezium', []);
        
        $name = $kafkaConfig['connector_name'] ?? 'wf-kafka-outbox-connector';
        $class = $kafkaConfig['connector_class'] ?? 'io.debezium.connector.mysql.MySqlConnector';
        $dlqTopic = $kafkaConfig['dlq_topic'] ?? 'wf-kafka.outbox.dlq';
        
        $database = $dbConfig['database'] ?? 'your_database_name';
        $host = $dbConfig['host'] ?? '127.0.0.1';
        $port = $dbConfig['port'] ?? '3306';
        $user = $dbConfig['username'] ?? 'root';
        $password = $dbConfig['password'] ?? 'secret';

        $configArray = [
            'connector.class' => $class,
            'tasks.max' => '1',
            
            'database.hostname' => $host,
            'database.port' => $port,
            'database.user' => $user,
            'database.password' => $password,
            'database.server.id' => '12345',
            'database.server.name' => 'dbserver1',
            'database.include.list' => $database,
            
            'table.include.list' => "{$database}.kafka_outbox",
            
            // DLQ Configuration
            'errors.tolerance' => 'all',
            'errors.deadletterqueue.topic.name' => $dlqTopic,
            'errors.deadletterqueue.context.headers.enable' => 'true',
            
            // Outbox Event Router SMT
            'transforms' => 'outbox',
            'transforms.outbox.type' => 'io.debezium.transforms.outbox.EventRouter',
            'transforms.outbox.table.field.event.id' => 'event_id',
            'transforms.outbox.table.field.event.type' => 'event_type',
            'transforms.outbox.table.field.event.payload' => 'payload',
            'transforms.outbox.route.by.field' => 'topic',
            'transforms.outbox.route.topic.replacement' => '${routedByValue}',
            
            // Converter
            'key.converter' => 'org.apache.kafka.connect.storage.StringConverter',
            'value.converter' => 'org.apache.kafka.connect.converters.ByteArrayConverter',
        ];

        $content = json_encode([
            'name' => $name,
            'config' => $configArray
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);

        // Luôn in ra console, user có thể pipe ra file (ví dụ: > config.json) 
        // hoặc copy trực tiếp, tránh việc package tự ghi file vào host project.
        $this->line($content);

        return Command::SUCCESS;
    }
}
