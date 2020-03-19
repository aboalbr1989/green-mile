package com.syriatel.d3m.greenmile.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


fun dateValue(str: String): LocalDateTime = LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

val indexArray: Array<Int> = (0..40).toList().toTypedArray()
