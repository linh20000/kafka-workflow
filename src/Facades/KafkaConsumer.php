<?php

namespace Wf\Kafka\Facades;

use Illuminate\Support\Facades\Facade;
use Wf\Kafka\Consumer\ConsumerDispatcher;

/**
 * Facade cho ConsumerDispatcher.
 *
 * Sử dụng:
 *   use Wf\Kafka\Facades\KafkaConsumer;
 *
 *   KafkaConsumer::onTopic('order-events')
 *       ->withGroup('order-service')
 *       ->handle(function (array $payload, array $meta) {
 *           // business logic của bạn
 *       })
 *       ->listen();
 */
class KafkaConsumer extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return ConsumerDispatcher::class;
    }
}
