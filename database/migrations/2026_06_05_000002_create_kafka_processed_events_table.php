<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('kafka_processed_events', function (Blueprint $table) {
            // event_id là primary key — duplicate insert sẽ throw QueryException (idempotency guard)
            $table->string('event_id', 100)->primary();
            $table->string('event_type', 100);
            $table->string('topic')->nullable();
            $table->timestamp('processed_at')->useCurrent();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('kafka_processed_events');
    }
};
