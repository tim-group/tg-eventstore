package com.timgroup.eventstore.api

class OptimisticConcurrencyFailure(cause: Option[Throwable]) extends RuntimeException(cause.orNull)
