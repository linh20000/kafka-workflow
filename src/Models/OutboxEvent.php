<?php

namespace Wf\Kafka\Models;

use Illuminate\Database\Eloquent\Model;

class OutboxEvent extends Model
{
    protected $table = 'kafka_outbox';

    protected $fillable = [
        'event_id', 'topic', 'event_type',
        'payload', 'status', 'attempts', 'last_error', 'sent_at',
    ];

    protected $casts = [
        'sent_at' => 'datetime',
        // payload KHÔNG cast sang array — lưu raw string (JSON hoặc Avro binary base64)
        // để OutboxRelayer gửi thẳng lên Kafka không qua encode/decode
    ];

    /**
     * Lấy raw bytes của payload (string) — dùng cho OutboxRelayer.
     * Không decode, không cast.
     */
    public function getRawPayload(): string
    {
        return $this->attributes['payload'] ?? '';
    }
}
