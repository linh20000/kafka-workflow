<?php

namespace Wf\Kafka\Serialization;

/**
 * Tạo MessageSerializer phù hợp dựa trên config serialization.driver.
 */
class SerializerFactory
{
    public static function make(array $serializationConfig): MessageSerializer
    {
        $driver = strtolower($serializationConfig['driver'] ?? 'json');

        return match ($driver) {
            'avro'  => new AvroSerializer($serializationConfig['avro'] ?? []),
            'json'  => new JsonSerializer(),
            default => throw new \InvalidArgumentException(
                "[wf-kafka] Unknown serialization driver: '{$driver}'. Supported: json, avro."
            ),
        };
    }
}
