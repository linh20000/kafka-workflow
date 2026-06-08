<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('kafka_outbox', function (Blueprint $table) {
            $table->id();
            $table->string('event_id', 100)->unique();
            $table->string('topic');
            $table->string('event_type', 100);
            $table->binary('payload');
            $table->string('status', 20)->default('PENDING'); // PENDING | SENT | FAILED
            $table->unsignedTinyInteger('attempts')->default(0);
            $table->text('last_error')->nullable();
            $table->timestamp('sent_at')->nullable();
            $table->timestamps();

            $table->index(['status', 'id'], 'kafka_outbox_relay_idx');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('kafka_outbox');
    }
};
