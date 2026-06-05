<?php

namespace Wf\Kafka\Exceptions;

/**
 * Dữ liệu không thể xử lý được (Poison Pill) — package sẽ route → EDL.
 *
 * Ví dụ: schema không hợp lệ, business rule vĩnh viễn bị vi phạm.
 *
 *   throw new PoisonPillException("Trạng thái đơn hàng 'GHOST' không tồn tại.");
 */
class PoisonPillException extends \DomainException {}
