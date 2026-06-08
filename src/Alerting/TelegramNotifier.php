<?php

namespace Wf\Kafka\Alerting;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * Gửi alert lên Telegram khi message bị route vào DLQ hoặc EDL.
 *
 * Config cần thiết trong kafka.telegram:
 *   enabled    : true/false
 *   bot_token  : token từ @BotFather
 *   chat_id    : chat/group/channel ID
 *   alert_on   : ['dlq', 'edl'] — chỉ alert các loại được bật
 */
class TelegramNotifier
{
    private bool   $enabled;
    private string $botToken;
    private string $chatId;
    private array  $alertOn;

    public function __construct(array $telegramConfig)
    {
        $this->enabled   = (bool)   ($telegramConfig['enabled']    ?? false);
        $this->botToken  = (string) ($telegramConfig['bot_token']  ?? '');
        $this->chatId    = (string) ($telegramConfig['chat_id']    ?? '');
        $this->alertOn   = (array)  ($telegramConfig['alert_on']   ?? ['dlq', 'edl']);
    }

    /**
     * Gửi alert DLQ — lỗi tạm thời, message sẽ được retry.
     */
    public function alertDlq(string $topic, string $eventId, string $reason): void
    {
        if (!$this->shouldAlert('dlq')) return;

        $topicHtml   = htmlspecialchars($topic, ENT_QUOTES | ENT_HTML5);
        $eventIdHtml = htmlspecialchars($eventId, ENT_QUOTES | ENT_HTML5);
        $reasonHtml  = htmlspecialchars($reason, ENT_QUOTES | ENT_HTML5);

        $this->send(
            "⚠️ <b>DLQ ALERT</b>\n\n"
            . "📌 Topic: <code>{$topicHtml}</code>\n"
            . "🔑 Event ID: <code>{$eventIdHtml}</code>\n"
            . "❗ Reason: {$reasonHtml}\n"
            . "<i>Message sẽ được retry sau khi hạ tầng ổn định.</i>"
        );
    }

    /**
     * Gửi alert EDL — Poison Pill, cần can thiệp thủ công.
     */
    public function alertEdl(string $topic, string $eventId, string $reason): void
    {
        if (!$this->shouldAlert('edl')) return;

        $topicHtml   = htmlspecialchars($topic, ENT_QUOTES | ENT_HTML5);
        $eventIdHtml = htmlspecialchars($eventId, ENT_QUOTES | ENT_HTML5);
        $reasonHtml  = htmlspecialchars($reason, ENT_QUOTES | ENT_HTML5);

        $this->send(
            "🚨 <b>EDL ALERT — Poison Pill</b>\n\n"
            . "📌 Topic: <code>{$topicHtml}</code>\n"
            . "🔑 Event ID: <code>{$eventIdHtml}</code>\n"
            . "❗ Reason: {$reasonHtml}\n"
            . "<i>Message đã bị cách ly xuống DB. Cần can thiệp thủ công.</i>"
        );
    }

    // ── Internal ───────────────────────────────────────────────────────────

    private function shouldAlert(string $type): bool
    {
        return $this->enabled
            && !empty($this->botToken)
            && !empty($this->chatId)
            && in_array($type, $this->alertOn, true);
    }

    private function send(string $text): void
    {
        try {
            $url = "https://api.telegram.org/bot{$this->botToken}/sendMessage";

            $response = Http::timeout(5)->post($url, [
                'chat_id'    => $this->chatId,
                'text'       => $text,
                'parse_mode' => 'HTML',
            ]);

            if (!$response->successful()) {
                Log::warning('[wf-kafka][Telegram] Alert failed', [
                    'status' => $response->status(),
                    'body'   => $response->body(),
                ]);
            }
        } catch (\Throwable $e) {
            // Alert failure không được ảnh hưởng tới luồng chính
            Log::error('[wf-kafka][Telegram] Exception during alert', [
                'error' => $e->getMessage(),
            ]);
        }
    }
}
