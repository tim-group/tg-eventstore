package com.timgroup.eventstore.api

class IdempotentWriteFailure(msg: String) extends RuntimeException(msg)
