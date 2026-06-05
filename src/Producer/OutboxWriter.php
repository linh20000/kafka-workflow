<?php

namespace Wf\Kafka\Producer;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Wf\Kafka\Models\OutboxEvent;
use Wf\Kafka\Serialization\MessageSerializer;
use Wf\Kafka\Serialization\SerializerFactory;

/**
 * OutboxWriter — serialize payload rồi ghi vào bảng kafka_outbox.
 *
 * Payload trong DB được lưu dưới dạng đã serialize (JSON string hoặc Avro base64).
 * OutboxRelayer sẽ đọc raw bytes này và gửi thẳng lên Kafka — không decode lại.
 *
 * Luôn gọi trong DB::transaction() của host project.
 */
class OutboxWriter
{
    private static ?MessageSerializer $serializer = null;

    /**
     * Inject serializer — được gọi bởi ServiceProvider.
     * Host project không cần gọi trực tiếp.
     */
    public static function setSerializer(MessageSerializer $serializer): void
    {
        self::$serializer = $serializer;
    }

    private static function getSerializer(): MessageSerializer
    {
        // Fallback về JSON nếu chưa được inject (unit test, standalone)
        return self::$serializer ??= SerializerFactory::make(['driver' => 'json']);
    }

    // ── Single write ───────────────────────────────────────────────────────

    /**
     * Ghi một message vào outbox.
     *
     * @param string              $topic
     * @param string              $eventType
     * @param array               $payload     Dữ liệu gốc — sẽ được serialize theo driver
     * @param ProducerOptions|null $options
     */
    public static function write(
        string           $topic,
        string           $eventType,
        array            $payload,
        ?ProducerOptions $options = null
    ): OutboxEvent {
        $serialized = self::getSerializer()->serialize($payload, $topic, $eventType);

        return OutboxEvent::create([
            'event_id'   => 'evt_' . Str::uuid()->toString(),
            'topic'      => $topic,
            'event_type' => $eventType,
            'payload'    => $serialized,   // lưu đã serialize
            'status'     => 'PENDING',
            'attempts'   => 0,
        ]);
    }

    // ── Batch write — cùng topic/eventType ────────────────────────────────

    /**
     * Ghi nhiều message cùng topic trong một INSERT duy nhất.
     *
     * @param string              $topic
     * @param string              $eventType
     * @param array               $payloads    Mảng các payload array
     * @param ProducerOptions|null $options
     * @return int  Số bản ghi đã ghi
     */
    public static function writeBatch(
        string           $topic,
        string           $eventType,
        array            $payloads,
        ?ProducerOptions $options = null
    ): int {
        if (empty($payloads)) {
            return 0;
        }

        $serializer = self::getSerializer();
        $now        = now();

        $rows = array_map(fn(array $p) => [
            'event_id'   => 'evt_' . Str::uuid()->toString(),
            'topic'      => $topic,
            'event_type' => $eventType,
            'payload'    => $serializer->serialize($p, $topic, $eventType),
            'status'     => 'PENDING',
            'attempts'   => 0,
            'created_at' => $now,
            'updated_at' => $now,
        ], $payloads);

        DB::table('kafka_outbox')->insert($rows);

        return count($rows);
    }

    // ── Multi write — nhiều topic/eventType khác nhau ─────────────────────

    /**
     * Ghi nhiều message với topic/eventType khác nhau trong một call.
     *
     * @param array $messages  [
     *   ['topic' => '...', 'event_type' => '...', 'payload' => [...]],
     *   ...
     * ]
     * @return int  Số bản ghi đã ghi
     */
    public static function writeMulti(array $messages): int
    {
        if (empty($messages)) {
            return 0;
        }

        $serializer = self::getSerializer();
        $now        = now();

        $rows = array_map(fn(array $m) => [
            'event_id'   => 'evt_' . Str::uuid()->toString(),
            'topic'      => $m['topic'],
            'event_type' => $m['event_type'],
            'payload'    => $serializer->serialize($m['payload'], $m['topic'], $m['event_type']),
            'status'     => 'PENDING',
            'attempts'   => 0,
            'created_at' => $now,
            'updated_at' => $now,
        ], $messages);

        DB::table('kafka_outbox')->insert($rows);

        return count($rows);
    }
}
