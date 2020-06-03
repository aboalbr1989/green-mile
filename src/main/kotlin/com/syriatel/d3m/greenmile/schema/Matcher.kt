package com.syriatel.d3m.greenmile.schema

interface Matcher<T> {
    fun matches(t1: T): Boolean
}

class ReqExMatcher(val reg: String) : Matcher<String> {
    override fun matches(t1: String): Boolean =
            t1 matches Regex(reg)
}

class DateTimeFormatMatcher(
        private val format: String
) : Matcher<String> {
    override fun matches(t1: String): Boolean =
            when (format) {
                "date-time" -> t1 matches Regex("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}")
                "date" -> t1 matches Regex("\\d{4}-\\d{2}-\\d{2}")
                "time" -> t1 matches Regex("\\d{2}:\\d{2}:\\d{2}.\\d{3}")
                else -> false
            }
}

class LengthMatcher(
        val minLength: Int,
        val maxLength: Int
) : Matcher<String> {
    override fun matches(t1: String): Boolean =
            t1.length in minLength..maxLength
}
