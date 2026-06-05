<?php

namespace Wf\Kafka\Exceptions;

/**
 * Lỗi hạ tầng tạm thời — consumer nên throw exception này để package tự route → DLQ.
 *
 * Ví dụ: timeout gọi external API, connection refused, circuit breaker open.
 *
 *   throw new TransientInfraException("GHTK API timeout sau 3s.");
 */
class TransientInfraException extends \RuntimeException {}
