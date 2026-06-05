<?php

namespace Wf\Kafka\Producer;

/**
 * Options cho từng lần publish — hoàn toàn độc lập với config core.
 *
 * Sử dụng:
 *   $options = ProducerOptions::make()
 *       ->withKey('order-123')
 *       ->withHeaders(['source' => 'checkout-service'])
 *       ->withFlushTimeoutMs(3000);
 */
class ProducerOptions
{
    private ?string $key           = null;
    private array   $headers       = [];
    private int     $flushTimeout  = 5000;
    private ?int    $partition      = null;

    private function __construct() {}

    public static function make(): static
    {
        return new static();
    }

    // ── Setters (fluent) ───────────────────────────────────────────────────

    /**
     * Partition key — Kafka dùng key để route message vào partition nhất định.
     * Đảm bảo ordering cho tất cả message có cùng key.
     */
    public function withKey(string $key): static
    {
        $this->key = $key;
        return $this;
    }

    /**
     * Kafka message headers (key-value string pairs).
     */
    public function withHeaders(array $headers): static
    {
        $this->headers = $headers;
        return $this;
    }

    /**
     * Override flush timeout (ms) cho lần publish này.
     */
    public function withFlushTimeoutMs(int $ms): static
    {
        $this->flushTimeout = $ms;
        return $this;
    }

    /**
     * Gửi vào partition cụ thể thay vì để Kafka tự chọn.
     * Thường không cần — chỉ dùng khi có yêu cầu đặc biệt.
     */
    public function withPartition(int $partition): static
    {
        $this->partition = $partition;
        return $this;
    }

    // ── Getters ────────────────────────────────────────────────────────────

    public function getKey(): ?string         { return $this->key; }
    public function getHeaders(): array       { return $this->headers; }
    public function getFlushTimeoutMs(): int  { return $this->flushTimeout; }
    public function getPartition(): int       { return $this->partition ?? RD_KAFKA_PARTITION_UA; }
}
