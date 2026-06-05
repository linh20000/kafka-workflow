<?php

namespace Wf\Kafka\Serialization;

/**
 * Contract cho serializer hai chiều.
 *
 * Serialize: array payload → binary string (ghi vào outbox + gửi lên Kafka)
 * Deserialize: binary string từ Kafka → array payload gốc (trả cho handle())
 */
interface MessageSerializer
{
    /**
     * Chuyển array payload thành binary string để ghi vào outbox / gửi lên Kafka.
     *
     * @param array  $payload   Dữ liệu gốc
     * @param string $topic     Tên topic (Avro cần để resolve subject/schema)
     * @param string $eventType Tên event type (Avro record name)
     * @return string           Binary string (JSON string hoặc Avro binary)
     */
    public function serialize(array $payload, string $topic, string $eventType): string;

    /**
     * Chuyển binary string từ Kafka về array payload gốc.
     *
     * @param string $data   Binary string nhận được từ Kafka
     * @param string $topic  Tên topic (Avro cần để resolve schema)
     * @return array         Payload gốc dưới dạng array
     *
     * @throws \Wf\Kafka\Exceptions\DeserializationException nếu không decode được
     */
    public function deserialize(string $data, string $topic): array;
}
