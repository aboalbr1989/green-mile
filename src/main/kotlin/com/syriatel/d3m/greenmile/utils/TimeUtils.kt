package com.syriatel.d3m.greenmile.utils

import org.apache.kafka.streams.kstream.Window
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.internals.TimeWindow
import java.time.*
import java.time.temporal.TemporalAdjusters

fun timestampOf(date: String) =
        LocalDateTime.parse(date).timestamp()


fun LocalDateTime.timestamp(): Instant = atZone(ZoneId.systemDefault()).toInstant()

fun Long.dateTime(): LocalDateTime = Instant.ofEpochMilli(this).atZone(ZoneId.systemDefault()).toLocalDateTime()

val Window.daily
    get() =
        TimeWindow(
                start().dateTime().toLocalDate().atStartOfDay().timestamp().toEpochMilli(),
                start().dateTime().toLocalDate().atTime(LocalTime.MAX).timestamp().toEpochMilli()
        )

val LocalDate.dailyWindow
    get() =
        TimeWindow(
                atStartOfDay().timestamp().toEpochMilli(),
                atTime(LocalTime.MAX).timestamp().toEpochMilli()
        )

val LocalDate.monthlyWindow
    get() =
        TimeWindow(
                atStartOfDay().timestamp().toEpochMilli(),
                atTime(LocalTime.MAX).timestamp().toEpochMilli()
        )


infix fun <T> TimeWindow.`for`(key: T): Windowed<T> = Windowed(key, this)

val Window.monthly
    get() =
        TimeWindow(
                start().dateTime().toLocalDate().withDayOfMonth(1).atStartOfDay().timestamp().toEpochMilli(),
                start().dateTime().toLocalDate().let {
                    it.withDayOfMonth(it.lengthOfMonth())
                }.atTime(LocalTime.MAX).timestamp().toEpochMilli()
        )

val Window.weekly
    get() =
        TimeWindow(
                start().dateTime().toLocalDate().with(TemporalAdjusters.previousOrSame(DayOfWeek.SATURDAY)).atStartOfDay().timestamp().toEpochMilli(),
                start().dateTime().toLocalDate().with(TemporalAdjusters.nextOrSame(DayOfWeek.FRIDAY)).atTime(LocalTime.MAX).timestamp().toEpochMilli()
        )