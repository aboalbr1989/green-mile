package com.syriatel.d3m.greenmile


import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*


@SpringBootApplication
class GreenMile

fun main(args: Array<String>) {
    runApplication<GreenMile>(*args)
}


@Configuration
@EnableConfigurationProperties(KafkaProperties::class)
class StreamsApp {


    @Bean(name = ["greenMileTopology"])
    fun topology() = StreamsBuilder().apply {
        ActionType.values().map {
            stream<String, String>(it.topic).mapValues { _, v ->
                it.toAction(v.split(",").toTypedArray())
            }
        }.reduce { acc, kStream ->
            acc.merge(kStream)
        }
    }

    @Bean
    fun kafkaStreams(streamsBuilder: StreamsBuilder, properties: KafkaProperties) =
            KafkaStreams(streamsBuilder.build(), Properties().apply {
                putAll(properties.buildStreamsProperties())
            }).apply {
                start()
            }
}


fun <K, V> KafkaProducer<K, V>.produceMessage(topic: String, key: K? = null, value: V) {
    send(ProducerRecord(topic, key, value))
}

enum class ActionType(val topic: String, val toAction: (Array<String>) -> Action) {
    Call("rec", processRec),
    Msg("sms", processSms),
    DataSession("data", processData),
    ActivateBundle("mon", processMon),
}


data class Action(
        val timeStamp: LocalDateTime = LocalDateTime.now(),
        val performedBy: String = "",
        val performerCell: Long? = null,
        val type: ActionType,
        val offer: Long? = null,
        val cost: Double? = null,
        val map:MutableMap<String,Any?> = mutableMapOf()

) : MutableMap<String, Any?> by map

fun dateValue(str: String): LocalDateTime = LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

val indexArray: Array<Int> = (0..34).toList().toTypedArray()
/**
 * cust_local_start_date4 -> startedAt
 * callingpartynumber38 -> performedBy
 * callingcellid47 -> performerCell
 * lasteffectoroffering61 -> offer
 * chg_amount35 -> cost
 * cust_local_end_date5 -> finishedAt
 * pri_identity7 -> deductedFrom
 * actual_usage11 -> actualDuration
 * rate_usage12 -> systemDuration
 * group_code24 -> group
 * object_type32 -> transactionType
 * object_type_id34 -> account
 * total_current_amount37 -> balance
 * debit_from_advance_prepaid15 -> debitFromPrepaid
 * debit_from_advance_postpaid -> debitFromPostpaid
 * debit_from_advance_credit_postpaid17 -> debitFromCredit
 * oper_type36 -> bfcOperation
 * calledpartynumber39 -> receivedBy
 * orginalcalledparty43 -> originalRecipient
 * serviceflow44 -> process
 * calledcellid49 -> receiverCell
 * mainofferingid53 -> simType
 * paytype54 -> payType
 * roamstat355 -> roamState
 * rattype62 -> sessionType
 * tarrif1470 -> systemCost
 * prepaid_balance91 -> prepaidBalance
 * postpaid_balance92 -> postpaidBalance
 * split_indicator94 -> splitIndicator
 * usage_service_type96 -> usageServiceType
 * callingcugno99 -> performerCugno
 * calledcugno100 -> receiverCugno
 * callingvpngroupnumber97 -> performerVpn
 * calledvpngroupnumber98 -> receiverVpn
 * discount1272 -> discountedValue1
 * discount1474 -> discountedValue2
 */

val processRec: (Array<String>) -> Action = {
    Action(
            timeStamp = dateValue(it[indexArray[0]]),
            performedBy = it[indexArray[1]],
            performerCell = it[indexArray[2]].toLong(),
            type = ActionType.Call,
            offer = it[indexArray[3]].toLong(),
            cost = it[indexArray[4]].toDouble()

    ).apply {
        putAll(
                mapOf(
                        "startedAt" to dateValue(it[indexArray[0]]),
                        "finishedAt" to dateValue(it[indexArray[5]]),
                        "deductedFrom" to it[indexArray[6]].toIntOrNull(),
                        "actualDuration" to it[indexArray[7]].toIntOrNull(),
                        "systemDuration" to it[indexArray[8]].toIntOrNull(),
                        "group" to it[indexArray[9]].toLongOrNull(),
                        "transactionType" to it[indexArray[10]],
                        "account" to it[indexArray[11]].toLongOrNull(),
                        "balance" to it[indexArray[12]].toDoubleOrNull(),
                        "debitFromPrepaid" to it[indexArray[13]].toDoubleOrNull(),
                        "debitFromPostpaid" to it[indexArray[14]].toDoubleOrNull(),
                        "debitFromCredit" to it[indexArray[15]].toDoubleOrNull(),
                        "bfcOperation" to it[indexArray[16]].toIntOrNull(),
                        "receivedBy" to it[indexArray[17]],
                        "originalRecipient" to it[indexArray[18]],
                        "process" to it[indexArray[19]].toIntOrNull(),
                        "receiverCell" to it[indexArray[20]].toLongOrNull(),
                        "simType" to it[indexArray[21]].toLongOrNull(),
                        "payType" to it[indexArray[22]].toIntOrNull(),
                        "roamState" to it[indexArray[23]].toIntOrNull(),
                        "sessionType" to it[indexArray[24]].toIntOrNull(),
                        "systemCost" to it[indexArray[25]].toDoubleOrNull(),
                        "prepaidBalance" to it[indexArray[26]].toLongOrNull(),
                        "postpaidBalance" to it[indexArray[27]].toLongOrNull(),
                        "splitIndicator" to it[indexArray[28]].toIntOrNull(),
                        "usageServiceType" to it[indexArray[29]].toIntOrNull(),
                        "performerCugno" to it[indexArray[30]].toLongOrNull(),
                        "receiverCugno" to it[indexArray[31]].toLongOrNull(),
                        "performerVpn" to it[indexArray[32]].toLongOrNull(),
                        "receiverVpn" to it[indexArray[33]].toLongOrNull(),
                        "discountedValue1" to it[indexArray[34]].toIntOrNull(),
                        "discountedValue2" to it[indexArray[35]].toIntOrNull()

                )
        )

    }
}
/**
 * cust_local_start_date4 -> startedAt
 * callingpartynumber38 -> performedBy
 * callingcellid47 -> performerCell
 * lasteffectoroffering61 -> offer
 * chg_amount35 -> cost
 * cust_local_end_date5 -> finishedAt
 * pri_identity7 -> deductedFrom
 * actual_usage11 -> actualDuration
 * rate_usage12 -> systemDuration
 * group_code24 -> group
 * object_type32 -> transactionType
 * object_type_id34 -> account
 * total_current_amount37 -> balance
 * debit_from_advance_prepaid15 -> debitFromPrepaid
 * debit_from_advance_postpaid -> debitFromPostpaid
 * debit_from_advance_credit_postpaid17 -> debitFromCredit
 * oper_type36 -> bfcOperation
 * calledpartynumber39 -> receivedBy
 * orginalcalledparty43 -> originalRecipient
 * serviceflow44 -> process
 * calledcellid49 -> receiverCell
 * mainofferingid53 -> simType
 * paytype54 -> payType
 * roamstat355 -> roamState
 * rattype62 -> sessionType
 * tarrif1470 -> systemCost
 * prepaid_balance91 -> prepaidBalance
 * postpaid_balance92 -> postpaidBalance
 * split_indicator94 -> splitIndicator
 * usage_service_type96 -> usageServiceType
 * callingcugno99 -> performerCugno
 * calledcugno100 -> receiverCugno
 * callingvpngroupnumber97 -> performerVpn
 * calledvpngroupnumber98 -> receiverVpn
 * discount1272 -> discountedValue1
 * discount1474 -> discountedValue2
 */
val processSms: (Array<String>) -> Action = {
    Action(
            timeStamp = dateValue(it[indexArray[0]]),
            performedBy = it[indexArray[1]],
            performerCell = it[indexArray[2]].toLong(),
            type = ActionType.Call,
            offer = it[indexArray[3]].toLong(),
            cost = it[indexArray[4]].toDouble()

    ).apply {
        putAll(
                mapOf(
                        "finishedAt" to dateValue(it[indexArray[5]]),
                        "deductedFrom" to it[indexArray[6]].toIntOrNull(),
                        "actualDuration" to it[indexArray[7]].toIntOrNull(),
                        "systemDuration" to it[indexArray[8]].toIntOrNull(),
                        "group" to it[indexArray[9]].toLongOrNull(),
                        "transactionType" to it[indexArray[10]],
                        "account" to it[indexArray[11]].toLongOrNull(),
                        "balance" to it[indexArray[12]].toDoubleOrNull(),
                        "debitFromPrepaid" to it[indexArray[13]].toDoubleOrNull(),
                        "debitFromPostpaid" to it[indexArray[14]].toDoubleOrNull(),
                        "debitFromCredit" to it[indexArray[15]].toDoubleOrNull(),
                        "bfcOperation" to it[indexArray[16]].toIntOrNull(),
                        "receivedBy" to it[indexArray[17]],
                        "originalRecipient" to it[indexArray[18]],
                        "process" to it[indexArray[19]].toIntOrNull(),
                        "receiverCell" to it[indexArray[20]].toLongOrNull(),
                        "simType" to it[indexArray[21]].toLongOrNull(),
                        "payType" to it[indexArray[22]].toIntOrNull(),
                        "roamState" to it[indexArray[23]].toIntOrNull(),
                        "sessionType" to it[indexArray[24]].toIntOrNull(),
                        "systemCost" to it[indexArray[25]].toDoubleOrNull(),
                        "prepaidBalance" to it[indexArray[26]].toLongOrNull(),
                        "postpaidBalance" to it[indexArray[27]].toLongOrNull(),
                        "splitIndicator" to it[indexArray[28]].toIntOrNull(),
                        "usageServiceType" to it[indexArray[29]].toIntOrNull(),
                        "performerCugno" to it[indexArray[30]].toLongOrNull(),
                        "receiverCugno" to it[indexArray[31]].toLongOrNull(),
                        "performerVpn" to it[indexArray[32]].toLongOrNull(),
                        "receiverVpn" to it[indexArray[33]].toLongOrNull(),
                        "discountedValue1" to it[indexArray[34]].toIntOrNull(),
                        "discountedValue2" to it[indexArray[35]].toIntOrNull()

                )
        )

    }
}

/**
 * cust_local_start_date4 -> startedAt
 * callingpartynumber38 -> performedBy
 * callingcellid47 -> performerCell
 * lasteffectoroffering61 -> offer
 * chg_amount35 -> cost
 * cust_local_end_date5 -> finishedAt
 * pri_identity7 -> deductedFrom
 * actual_usage11 -> actualDuration
 * rate_usage12 -> systemDuration
 * group_code24 -> group
 * object_type32 -> transactionType
 * object_type_id34 -> account
 * total_current_amount37 -> balance
 * debit_from_advance_prepaid15 -> debitFromPrepaid
 * debit_from_advance_postpaid -> debitFromPostpaid
 * debit_from_advance_credit_postpaid17 -> debitFromCredit
 * oper_type36 -> bfcOperation
 * calledpartynumber39 -> receivedBy
 * orginalcalledparty43 -> originalRecipient
 * serviceflow44 -> process
 * calledcellid49 -> receiverCell
 * mainofferingid53 -> simType
 * paytype54 -> payType
 * roamstat355 -> roamState
 * rattype62 -> sessionType
 * tarrif1470 -> systemCost
 * prepaid_balance91 -> prepaidBalance
 * postpaid_balance92 -> postpaidBalance
 * split_indicator94 -> splitIndicator
 * usage_service_type96 -> usageServiceType
 * callingcugno99 -> performerCugno
 * calledcugno100 -> receiverCugno
 * callingvpngroupnumber97 -> performerVpn
 * calledvpngroupnumber98 -> receiverVpn
 * discount1272 -> discountedValue1
 * discount1474 -> discountedValue2
 */
val processData: (Array<String>) -> Action = {
    Action(
            timeStamp = dateValue(it[indexArray[0]]),
            performedBy = it[indexArray[1]],
            performerCell = it[indexArray[2]].toLong(),
            type = ActionType.Call,
            offer = it[indexArray[3]].toLong(),
            cost = it[indexArray[4]].toDouble()

    ).apply {
        putAll(
                mapOf(
                        "finishedAt" to dateValue(it[indexArray[5]]),
                        "deductedFrom" to it[indexArray[6]].toIntOrNull(),
                        "actualDuration" to it[indexArray[7]].toIntOrNull(),
                        "systemDuration" to it[indexArray[8]].toIntOrNull(),
                        "group" to it[indexArray[9]].toLongOrNull(),
                        "transactionType" to it[indexArray[10]],
                        "account" to it[indexArray[11]].toLongOrNull(),
                        "balance" to it[indexArray[12]].toDoubleOrNull(),
                        "debitFromPrepaid" to it[indexArray[13]].toDoubleOrNull(),
                        "debitFromPostpaid" to it[indexArray[14]].toDoubleOrNull(),
                        "debitFromCredit" to it[indexArray[15]].toDoubleOrNull(),
                        "bfcOperation" to it[indexArray[16]].toIntOrNull(),
                        "receivedBy" to it[indexArray[17]],
                        "originalRecipient" to it[indexArray[18]],
                        "process" to it[indexArray[19]].toIntOrNull(),
                        "receiverCell" to it[indexArray[20]].toLongOrNull(),
                        "simType" to it[indexArray[21]].toLongOrNull(),
                        "payType" to it[indexArray[22]].toIntOrNull(),
                        "roamState" to it[indexArray[23]].toIntOrNull(),
                        "sessionType" to it[indexArray[24]].toIntOrNull(),
                        "systemCost" to it[indexArray[25]].toDoubleOrNull(),
                        "prepaidBalance" to it[indexArray[26]].toLongOrNull(),
                        "postpaidBalance" to it[indexArray[27]].toLongOrNull(),
                        "splitIndicator" to it[indexArray[28]].toIntOrNull(),
                        "usageServiceType" to it[indexArray[29]].toIntOrNull(),
                        "performerCugno" to it[indexArray[30]].toLongOrNull(),
                        "receiverCugno" to it[indexArray[31]].toLongOrNull(),
                        "performerVpn" to it[indexArray[32]].toLongOrNull(),
                        "receiverVpn" to it[indexArray[33]].toLongOrNull(),
                        "discountedValue1" to it[indexArray[34]].toIntOrNull(),
                        "discountedValue2" to it[indexArray[35]].toIntOrNull()

                )
        )

    }
}

/**
 * START_DATE7 -> startedAt
 * PRI_IDENTITY21 ->  performedBy
 * offeringCode418 -> offer
 * debitamount36 -> cost
 * END_DATE8 -> finishedAt
 * OBJECT_TYPE106 -> transactionType
 * current_amount52 -> balance
 * debit_from_advance_prepaid15 -> debitFromPrepaid
 * debit_from_advance_postpaid -> debitFromPostpaid
 * debit_from_advance_credit_postpaid17 -> debitFromCredit
 * oper_type36 -> bfcOperation
 * MainOfferingID408 -> simType
 * payType409 -> payType
 * IMSI431 -> imsi
 * OfferingID419 -> offeringID
 * CREATE_DATE6 -> createdAt
 * CycleBeginTime411 -> cycleBeginTime
 * CycleEndTime412 -> cycleEndTime
 * CycleLength414 -> cycleLength
 */
val processMon: (Array<String>) -> Action = {
    Action(
            timeStamp = LocalDateTime.parse(it[indexArray[0]], DateTimeFormatter.ofPattern("yyyyMMddHHmmss")),
            performedBy = it[indexArray[1]],
            performerCell = null,
            type = ActionType.ActivateBundle,
            offer = it[indexArray[2]].toLongOrNull(),
            cost = it[indexArray[3]].toDoubleOrNull()
    ).apply {
        putAll(
                mapOf(
                        "finishedAt" to dateValue(it[indexArray[4]]),
                        "transactionType" to it[indexArray[5]],
                        "balance" to it[indexArray[6]].toDoubleOrNull(),
                        "debitFromPrepaid" to it[indexArray[7]].toDoubleOrNull(),
                        "debitFromPostpaid" to it[indexArray[8]].toDoubleOrNull(),
                        "debitFromCredit" to it[indexArray[9]].toDoubleOrNull(),
                        "bfcOperation" to it[indexArray[10]].toIntOrNull(),
                        "simType" to it[indexArray[11]].toLongOrNull(),
                        "payType" to it[indexArray[12]].toIntOrNull(),
                        "imsi" to it[indexArray[13]],
                        "offeringID" to it[indexArray[14]],
                        "createdAt" to it[indexArray[15]],
                        "cycleBeginTime" to dateValue(it[indexArray[16]]),
                        "cycleEndTime" to dateValue(it[indexArray[17]]),
                        "cycleLength" to it[indexArray[18]].toLongOrNull()
                )
        )
    }
}


/*

val processMgr: (Array<String>) -> Action = {
    Action(
            //timeStamp
            startedAt = LocalDateTime.parse(it[indexArray[0]], DateTimeFormatter.ofPattern("yyyyMMddHHmmss")),
            //PRI_IDENTITY
            performedBy = it[indexArray[1]],
            performerCell = null,
            type = ActionType.Other,
            //offeringCode418
            offer = it[indexArray[3]].toLong(),
            //debitamount36
            cost = it[indexArray[4]].toDouble()
    ).apply {
        put("")
    }
}


val processVou: (Array<String>) -> Action = {
    Action(
            //Test_5
            startedAt = LocalDateTime.parse(it[indexArray[0]], DateTimeFormatter.ofPattern("yyyyMMddHHmmss")),
            //Test2
            performedBy = it[indexArray[1]],
            performerCell = null,
            type = ActionType.ActivateBundle,
            //offeringCode418
            offer = it[indexArray[3]].toLong(),
            //debitamount36
            cost = it[indexArray[4]].toDouble()
    )
}
*/

