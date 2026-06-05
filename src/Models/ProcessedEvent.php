<?php

namespace Wf\Kafka\Models;

use Illuminate\Database\Eloquent\Model;

class ProcessedEvent extends Model
{
    protected $table      = 'kafka_processed_events';
    public    $timestamps = false;
    protected $primaryKey = 'event_id';
    public    $incrementing = false;
    protected $keyType    = 'string';

    protected $fillable = ['event_id', 'event_type', 'topic', 'processed_at'];

    protected $casts = ['processed_at' => 'datetime'];
}
