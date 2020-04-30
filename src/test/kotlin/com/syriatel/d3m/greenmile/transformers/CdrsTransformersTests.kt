package com.syriatel.d3m.greenmile.transformers

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

@DisplayName("Rec Deserializer Tests")
class RecTransformerTests {
    @Test
    fun `should transform rec cdr to action when the cdr is valid`() {

        val fields = arrayOf("20200101121000", "0933886839", "020202020202", "030303", "not used",
                "20200101121011", "0933886839", "46", "60", "090909090909", "transactionType", "11111111", "1000.0",
                "13.0", "0.0", "0.0", "1", "0993995060", "181818", "1919191919", "2020202020", "21212121", "222222",
                "23232323", "24242424", "25.0", "262626", "2727272727", "2828282828", "29292929", "3030303030",
                "31313131", "32323232", "3333333", "34343434", "35353535")

        val expected = Action(timeStamp = LocalDateTime.of(2020, 1, 1, 12, 10,
                0),
                performedBy = "0933886839",
                performerCell = "020202020202",
                type = ActionType.Call,
                offer = "030303",
                cost = 13.0,
                map = mutableMapOf(
                        "startedAt" to LocalDateTime.of(2020, 1, 1, 12, 10,
                                0),
                        "finishedAt" to LocalDateTime.of(2020, 1, 1, 12, 10,
                                11),
                        "deductedFrom" to "0933886839",
                        "actualDuration" to 46,
                        "systemDuration" to 60,
                        "group" to "090909090909",
                        "transactionType" to "transactionType",
                        "account" to "11111111",
                        "balance" to 1000.0,
                        "debitFromPrepaid" to 13.0,
                        "debitFromPostpaid" to 0.0,
                        "debitFromCredit" to 0.0,
                        "bfcOperation" to "1",
                        "receivedBy" to "0993995060",
                        "originalRecipient" to "181818",
                        "process" to "1919191919",
                        "receiverCell" to "2020202020",
                        "simType" to "21212121",
                        "payType" to "222222",
                        "roamState" to "23232323",
                        "sessionType" to "24242424",
                        "systemCost" to 25.0,
                        "prepaidBalance" to 262626L,
                        "postpaidBalance" to 2727272727L,
                        "splitIndicator" to "2828282828",
                        "usageServiceType" to "29292929",
                        "performerCugno" to "3030303030",
                        "receiverCugno" to "31313131",
                        "performerVpn" to "32323232",
                        "receiverVpn" to "3333333",
                        "discountedValue1" to 34343434,
                        "discountedValue2" to 35353535

                )
        )
        processRec(fields).let {
            Assertions.assertNotSame(expected, it)
            Assertions.assertEquals(expected, it)
        }
    }
}

@DisplayName("SMS Deserializer Tests")
class SmsTransformerTests {
    @Test
    fun `should transform sms cdr to action`() {
        val fields = arrayOf("20200101121000", "0933886839", "020202020202", "030303", "not used",
                "20200101121011", "0933886839", "46", "60", "090909090909", "transactionType", "11111111", "1000.0",
                "13.0", "0.0", "0.0", "1", "0993995060", "181818", "1919191919", "2020202020", "21212121", "222222",
                "23232323", "24242424", "25.0", "262626", "2727272727", "2828282828", "29292929", "3030303030",
                "31313131", "32323232", "3333333", "34343434", "35353535")
        val expected = Action(timeStamp = LocalDateTime.of(2020, 1, 1, 12, 10,
                0),
                performedBy = "0933886839",
                performerCell = "020202020202",
                type = ActionType.SMS,
                offer = "030303",
                cost = 13.0,
                map = mutableMapOf(
                        "startedAt" to LocalDateTime.of(2020, 1, 1, 12, 10,
                                0),
                        "finishedAt" to LocalDateTime.of(2020, 1, 1, 12, 10,
                                11),
                        "deductedFrom" to "0933886839",
                        "actualDuration" to 46,
                        "systemDuration" to 60,
                        "group" to "090909090909",
                        "transactionType" to "transactionType",
                        "account" to "11111111",
                        "balance" to 1000.0,
                        "debitFromPrepaid" to 13.0,
                        "debitFromPostpaid" to 0.0,
                        "debitFromCredit" to 0.0,
                        "bfcOperation" to "1",
                        "receivedBy" to "0993995060",
                        "originalRecipient" to "181818",
                        "process" to "1919191919",
                        "receiverCell" to "2020202020",
                        "simType" to "21212121",
                        "payType" to "222222",
                        "roamState" to "23232323",
                        "sessionType" to "24242424",
                        "systemCost" to 25.0,
                        "prepaidBalance" to 262626L,
                        "postpaidBalance" to 2727272727L,
                        "splitIndicator" to "2828282828",
                        "usageServiceType" to "29292929",
                        "performerCugno" to "3030303030",
                        "receiverCugno" to "31313131",
                        "performerVpn" to "32323232",
                        "receiverVpn" to "3333333",
                        "discountedValue1" to 34343434,
                        "discountedValue2" to 35353535

                ))

        processSms(fields).let {
            Assertions.assertNotSame(expected, it)
            Assertions.assertEquals(expected, it)
        }

    }
}

@DisplayName("Data Deserializer Tests")
class DataTransformerTests {
    @Test
    fun `should transform data cdr to action`() {

        val fields = arrayOf("20200101121000", "0933886839", "020202020202", "030303", "not used",
                "20200101121011", "0933886839", "46", "60", "090909090909", "transactionType", "11111111", "1000.0",
                "13.0", "0.0", "0.0", "1", "0993995060", "181818", "1919191919", "2020202020", "21212121", "222222",
                "23232323", "24242424", "25.0", "262626", "2727272727", "2828282828", "29292929", "3030303030",
                "31313131", "32323232", "3333333", "34343434", "35353535", "36363636", "37373737")

        val expected = Action(timeStamp = LocalDateTime.of(2020, 1, 1, 12, 10,
                0),
                performedBy = "0933886839",
                performerCell = "020202020202",
                type = ActionType.DataSession,
                offer = "030303",
                cost = 13.0,
                map = mutableMapOf(
                        "startedAt" to LocalDateTime.of(2020, 1, 1, 12, 10,
                                0),
                        "finishedAt" to LocalDateTime.of(2020, 1, 1, 12, 10,
                                11),
                        "deductedFrom" to "0933886839",
                        "actualDuration" to 46,
                        "systemDuration" to 60,
                        "group" to "090909090909",
                        "transactionType" to "transactionType",
                        "account" to "11111111",
                        "balance" to 1000.0,
                        "debitFromPrepaid" to 13.0,
                        "debitFromPostpaid" to 0.0,
                        "debitFromCredit" to 0.0,
                        "bfcOperation" to "1",
                        "receivedBy" to "0993995060",
                        "originalRecipient" to "181818",
                        "process" to "1919191919",
                        "receiverCell" to "2020202020",
                        "simType" to "21212121",
                        "payType" to "222222",
                        "roamState" to "23232323",
                        "sessionType" to "24242424",
                        "systemCost" to 25.0,
                        "prepaidBalance" to 262626L,
                        "postpaidBalance" to 2727272727L,
                        "splitIndicator" to "2828282828",
                        "usageServiceType" to "29292929",
                        "performerCugno" to "3030303030",
                        "receiverCugno" to "31313131",
                        "performerVpn" to "32323232",
                        "receiverVpn" to "3333333",
                        "discountedValue1" to 34343434,
                        "discountedValue2" to 35353535,
                        "actualByte" to 36363636L,
                        "systemByte" to 37373737L

                ))
        processData(fields).let {
            Assertions.assertNotSame(expected, it)
            Assertions.assertEquals(expected, it)
        }

    }
}

