<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('kafka_failed_event_logs', function (Blueprint $table) {
            $table->id();
            $table->string('event_id', 100)->index();
            $table->string('event_type', 100);
            $table->string('original_topic');
            $table->string('failure_type', 10); // DLQ | EDL
            $table->json('payload');
            $table->text('failure_reason');

            // PENDING_FIX → đang chờ xử lý
            // REDRIVEN    → đã được đẩy lại vào outbox
            // FIXED       → đã xử lý thành công sau redrive
            // IGNORED     → đã xác nhận bỏ qua
            $table->string('status', 20)->default('PENDING_FIX');

            $table->timestamp('alerted_at')->nullable(); // Thời điểm Telegram alert gửi đi
            $table->timestamps();

            // Index cho redrive query: lọc theo loại và trạng thái
            $table->index(['failure_type', 'status'], 'kafka_fel_type_status_idx');
            // Index cho lookup theo event_id + status
            $table->index(['event_id', 'status'], 'kafka_fel_event_status_idx');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('kafka_failed_event_logs');
    }
};
