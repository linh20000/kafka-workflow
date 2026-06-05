<?php

namespace Wf\Kafka\Serialization;

use Wf\Kafka\Exceptions\DeserializationException;

/**
 * JSON serializer — mặc định khi KAFKA_SERIALIZATION_DRIVER=json.
 *
 * Wire format:
 *   {"event_id":"evt_xxx","event_type":"ORDER_CREATED","payload":{...}}
 */
class JsonSerializer implements MessageSerializer
{
    public function serialize(array $payload, string $topic, string $eventType): string
    {
        return json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
    }

    public function deserialize(string $data, string $topic): array
    {
        try {
            $decoded = json_decode($data, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new DeserializationException(
                "JSON decode failed: {$e->getMessage()}",
                previous: $e
            );
        }

        if (!is_array($decoded)) {
            throw new DeserializationException("Decoded value is not an array.");
        }

        return $decoded;
    }
}
