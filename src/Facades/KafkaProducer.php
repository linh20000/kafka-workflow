<?php

namespace Wf\Kafka\Facades;

use Illuminate\Support\Facades\Facade;
use Wf\Kafka\Producer\OutboxWriter;

/**
 * Facade cho OutboxWriter.
 *
 * Sử dụng:
 *   use Wf\Kafka\Facades\KafkaProducer;
 *
 *   KafkaProducer::write('order-events', 'ORDER_CREATED', $payload);
 *   KafkaProducer::writeBatch('order-events', 'ORDER_CREATED', $payloads);
 */
class KafkaProducer extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return OutboxWriter::class;
    }
}
