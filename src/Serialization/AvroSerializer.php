<?php

namespace Wf\Kafka\Serialization;

use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Wf\Kafka\Exceptions\DeserializationException;

/**
 * Avro serializer theo chuẩn Confluent wire format.
 *
 * Wire format (binary):
 *   [0x00][schema_id: 4 bytes big-endian][avro binary payload]
 *
 * Dependency: flix-tech/avro-php ^5.1 (đã khai báo trong composer.json require)
 *   Class map (global namespace, không có PSR-4):
 *     \AvroSchema            — parse schema JSON
 *     \AvroStringIO          — in-memory IO buffer
 *     \AvroIODatumWriter     — write array → Avro binary
 *     \AvroIOBinaryEncoder   — binary encoder wrapping StringIO
 *     \AvroIODatumReader     — read Avro binary → array
 *     \AvroIOBinaryDecoder   — binary decoder wrapping StringIO
 *
 * Flow serialize:
 *   1. Resolve subject name (topic_name / record_name / topic_record strategy)
 *   2. Fetch schema_id + schema JSON từ Confluent Schema Registry (có cache)
 *   3. AvroIODatumWriter encode array → binary
 *   4. Prepend magic byte (0x00) + schema_id (4 bytes) = Confluent wire format
 *
 * Flow deserialize:
 *   1. Đọc magic byte (assert 0x00)
 *   2. Đọc schema_id (4 bytes)
 *   3. Fetch schema JSON từ Registry bằng schema_id (có cache)
 *   4. AvroIODatumReader decode binary → array gốc
 */
class AvroSerializer implements MessageSerializer
{
    private const MAGIC_BYTE   = 0x00;
    private const HEADER_SIZE  = 5; // 1 magic + 4 schema_id
    private const CACHE_PREFIX = 'wf_kafka_avro_';
    private const CACHE_TTL    = 3600; // 1 giờ

    private string $registryUrl;
    private array  $authHeader;
    private string $subjectStrategy;
    private bool   $useCache;
    /** @var array<string, string> topic → local .avsc file path */
    private array  $localSchemas;

    public function __construct(array $avroConfig)
    {
        $this->registryUrl     = rtrim($avroConfig['registry_url']      ?? 'http://localhost:8081', '/');
        $this->subjectStrategy = $avroConfig['subject_strategy']        ?? 'topic_name';
        $this->useCache        = (bool) ($avroConfig['schema_cache']    ?? true);
        $this->localSchemas    = $avroConfig['schemas']                 ?? [];

        $user = $avroConfig['registry_username'] ?? '';
        $pass = $avroConfig['registry_password'] ?? '';
        $this->authHeader = $user !== ''
            ? ['Authorization' => 'Basic ' . base64_encode("{$user}:{$pass}")]
            : [];
    }

    // ── Serialize: array → Confluent Avro binary ───────────────────────────

    public function serialize(array $payload, string $topic, string $eventType): string
    {
        $this->assertAvroLibLoaded();

        $subject  = $this->resolveSubject($topic, $eventType);
        [$schemaId, $schemaJson] = $this->fetchBySubject($subject, $topic);

        $binary = $this->avroBinaryEncode($schemaJson, $payload);

        // Confluent wire format: magic(1 byte) + schema_id(4 bytes BE) + avro binary
        return pack('CN', self::MAGIC_BYTE, $schemaId) . $binary;
    }

    // ── Deserialize: Confluent Avro binary → array ─────────────────────────

    public function deserialize(string $data, string $topic): array
    {
        $this->assertAvroLibLoaded();

        if (strlen($data) < self::HEADER_SIZE) {
            throw new DeserializationException(
                "Message too short (" . strlen($data) . " bytes) to be Confluent Avro wire format."
            );
        }

        /** @var array{magic: int, schema_id: int} $header */
        $header   = unpack('Cmagic/Nschema_id', substr($data, 0, self::HEADER_SIZE));
        $magic    = $header['magic'];
        $schemaId = $header['schema_id'];

        if ($magic !== self::MAGIC_BYTE) {
            throw new DeserializationException(sprintf(
                "Invalid magic byte: expected 0x%02X, got 0x%02X. Message may not be Avro-encoded.",
                self::MAGIC_BYTE,
                $magic
            ));
        }

        $schemaJson = $this->fetchById($schemaId);
        $avroBinary = substr($data, self::HEADER_SIZE);

        return $this->avroBinaryDecode($schemaJson, $avroBinary);
    }

    // ── Schema resolution ──────────────────────────────────────────────────

    /**
     * Resolve Confluent subject name theo strategy.
     */
    private function resolveSubject(string $topic, string $eventType): string
    {
        return match ($this->subjectStrategy) {
            'record_name'  => $eventType . '-value',
            'topic_record' => "{$topic}-{$eventType}-value",
            default        => "{$topic}-value",   // 'topic_name' — Confluent default
        };
    }

    /**
     * Fetch schema_id + schema JSON theo subject (dùng khi serialize).
     * Ưu tiên local .avsc file nếu được khai báo trong config.
     *
     * @return array{int, string} [schema_id, schema_json]
     */
    private function fetchBySubject(string $subject, string $topic): array
    {
        // Local file override — fetch schema_id vẫn cần từ Registry
        $localJson = $this->loadLocalSchema($topic);

        $cacheKey = self::CACHE_PREFIX . 'subject_' . md5($subject);

        if ($this->useCache && Cache::has($cacheKey)) {
            [$schemaId] = Cache::get($cacheKey);
            $schemaJson = $localJson ?? $this->fetchById($schemaId);
            return [$schemaId, $schemaJson];
        }

        $url      = "{$this->registryUrl}/subjects/{$subject}/versions/latest";
        $response = Http::withHeaders($this->authHeader)->get($url);

        if (!$response->successful()) {
            throw new \RuntimeException(
                "[wf-kafka][Avro] Registry error fetching subject '{$subject}': "
                . $response->status() . ' — ' . $response->body()
            );
        }

        $schemaId   = (int)    $response->json('id');
        $schemaJson = $localJson ?? $response->json('schema');

        if ($this->useCache) {
            Cache::put($cacheKey, [$schemaId, $schemaJson], self::CACHE_TTL);
        }

        return [$schemaId, $schemaJson];
    }

    /**
     * Fetch schema JSON theo schema_id (dùng khi deserialize).
     */
    private function fetchById(int $schemaId): string
    {
        $cacheKey = self::CACHE_PREFIX . 'id_' . $schemaId;

        if ($this->useCache && Cache::has($cacheKey)) {
            return Cache::get($cacheKey);
        }

        $url      = "{$this->registryUrl}/schemas/ids/{$schemaId}";
        $response = Http::withHeaders($this->authHeader)->get($url);

        if (!$response->successful()) {
            throw new \RuntimeException(
                "[wf-kafka][Avro] Registry error fetching schema id={$schemaId}: "
                . $response->status() . ' — ' . $response->body()
            );
        }

        $schemaJson = $response->json('schema');

        if ($this->useCache) {
            Cache::put($cacheKey, $schemaJson, self::CACHE_TTL);
        }

        return $schemaJson;
    }

    /**
     * Đọc schema từ local .avsc file nếu topic được khai báo trong config.avro.schemas.
     */
    private function loadLocalSchema(string $topic): ?string
    {
        if (!isset($this->localSchemas[$topic])) {
            return null;
        }

        $path = $this->localSchemas[$topic];

        if (!file_exists($path)) {
            throw new \RuntimeException("[wf-kafka][Avro] Local schema file not found: {$path}");
        }

        return file_get_contents($path);
    }

    // ── Avro binary encode / decode (flix-tech/avro-php global classes) ────

    /**
     * array → Avro binary string.
     * Dùng các class global namespace của flix-tech/avro-php:
     *   AvroSchema, AvroStringIO, AvroIOBinaryEncoder, AvroIODatumWriter
     */
    private function avroBinaryEncode(string $schemaJson, array $data): string
    {
        $schema  = \AvroSchema::parse($schemaJson);
        $io      = new \AvroStringIO();
        $encoder = new \AvroIOBinaryEncoder($io);
        $writer  = new \AvroIODatumWriter($schema);
        $writer->write($data, $encoder);
        return $io->string();
    }

    /**
     * Avro binary string → array.
     * Dùng các class global namespace của flix-tech/avro-php:
     *   AvroSchema, AvroStringIO, AvroIOBinaryDecoder, AvroIODatumReader
     */
    private function avroBinaryDecode(string $schemaJson, string $binary): array
    {
        try {
            $schema  = \AvroSchema::parse($schemaJson);
            $io      = new \AvroStringIO($binary);
            $decoder = new \AvroIOBinaryDecoder($io);
            $reader  = new \AvroIODatumReader($schema, $schema);
            $result  = $reader->read($decoder);
            return is_array($result) ? $result : (array) $result;
        } catch (\Throwable $e) {
            throw new DeserializationException(
                "[wf-kafka][Avro] Binary decode failed: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    // ── Guard ──────────────────────────────────────────────────────────────

    /**
     * Kiểm tra flix-tech/avro-php đã được load chưa.
     * Nếu chưa, ném exception rõ ràng hướng dẫn cài đặt.
     */
    private function assertAvroLibLoaded(): void
    {
        if (!class_exists(\AvroSchema::class)) {
            throw new \RuntimeException(
                "[wf-kafka][Avro] Class \\AvroSchema not found.\n"
                . "flix-tech/avro-php is listed in composer.json require — run: composer install\n"
                . "If you are not using Avro, set KAFKA_SERIALIZATION_DRIVER=json in your .env."
            );
        }
    }
}
