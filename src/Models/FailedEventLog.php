<?php

namespace Wf\Kafka\Models;

use Illuminate\Database\Eloquent\Model;

class FailedEventLog extends Model
{
    protected $table = 'kafka_failed_event_logs';

    protected $fillable = [
        'event_id', 'event_type', 'original_topic',
        'failure_type', 'payload', 'failure_reason', 'status', 'alerted_at',
    ];

    protected $casts = [
        'payload'    => 'array',
        'alerted_at' => 'datetime',
    ];
}
