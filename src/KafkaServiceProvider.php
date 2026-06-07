<?php

namespace Wf\Kafka;

use Illuminate\Support\ServiceProvider;
use Wf\Kafka\Alerting\TelegramNotifier;
use Wf\Kafka\Consumer\ConsumerDispatcher;
use Wf\Kafka\Kafka\KafkaConfig;
use Wf\Kafka\Producer\OutboxRelayer;
use Wf\Kafka\Producer\OutboxWriter;
use Wf\Kafka\Serialization\MessageSerializer;
use Wf\Kafka\Serialization\SerializerFactory;
use Wf\Kafka\Console\Commands\KafkaRedriveCommand;
use Wf\Kafka\Console\Commands\KafkaInstallCommand;
use Wf\Kafka\Console\Commands\OutboxRelayCommand;

class KafkaServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/wf-kafka.php', 'kafka');

        // ── KafkaConfig ─────────────────────────────────────────────────────
        $this->app->singleton(KafkaConfig::class, function ($app) {
            return new KafkaConfig($app['config']['kafka']);
        });

        // ── MessageSerializer ────────────────────────────────────────────────
        // Singleton: một serializer dùng chung cho cả producer lẫn consumer.
        // Driver được đọc từ config('kafka.serialization.driver').
        $this->app->singleton(MessageSerializer::class, function ($app) {
            return SerializerFactory::make(
                $app['config']['kafka']['serialization'] ?? ['driver' => 'json']
            );
        });

        // ── TelegramNotifier ─────────────────────────────────────────────────
        $this->app->singleton(TelegramNotifier::class, function ($app) {
            return new TelegramNotifier(
                $app['config']['kafka']['telegram'] ?? []
            );
        });

        // ── OutboxRelayer ────────────────────────────────────────────────────
        // Singleton: giữ cùng rdkafka producer instance với internal queue buffer.
        $this->app->singleton(OutboxRelayer::class, function ($app) {
            return new OutboxRelayer($app->make(KafkaConfig::class));
        });

        // ── OutboxWriter ─────────────────────────────────────────────────────
        // Stateless static class — bind để DI container có thể resolve,
        // đồng thời inject serializer vào static state khi boot.
        $this->app->bind(OutboxWriter::class, fn() => new OutboxWriter());

        // ── ConsumerDispatcher ───────────────────────────────────────────────
        // KHÔNG singleton — mỗi resolve tạo rdkafka consumer riêng.
        // Mỗi topic/group cần connection độc lập.
        $this->app->bind(ConsumerDispatcher::class, function ($app) {
            return new ConsumerDispatcher(
                $app->make(KafkaConfig::class),
                $app->make(TelegramNotifier::class),
                $app->make(MessageSerializer::class)
            );
        });
    }

    public function boot(): void
    {
        // ── Inject serializer vào OutboxWriter (static) ──────────────────────
        // Phải gọi sau khi container đã build xong MessageSerializer.
        OutboxWriter::setSerializer($this->app->make(MessageSerializer::class));

        // ── Publish config ────────────────────────────────────────────────────
        // php artisan vendor:publish --tag=kafka-config
        $this->publishes([
            __DIR__ . '/../config/wf-kafka.php' => config_path('kafka.php'),
        ], 'kafka-config');

        // ── Publish migrations ────────────────────────────────────────────────
        // php artisan vendor:publish --tag=kafka-migrations
        $this->publishes([
            __DIR__ . '/../database/migrations/' => database_path('migrations'),
        ], 'kafka-migrations');

        // Load migrations trực tiếp từ package (không cần publish nếu không custom)
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');

        // ── Artisan commands ──────────────────────────────────────────────────
        if ($this->app->runningInConsole()) {
            $this->commands([
                OutboxRelayCommand::class,
                KafkaRedriveCommand::class,
                KafkaInstallCommand::class,
            ]);
        }
    }
}
