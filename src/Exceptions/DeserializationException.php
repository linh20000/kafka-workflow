<?php

namespace Wf\Kafka\Exceptions;

/**
 * Không thể deserialize message từ Kafka.
 * Package sẽ route message này → EDL.
 */
class DeserializationException extends \RuntimeException {}
