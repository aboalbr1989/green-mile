package com.syriatel.d3m.greenmile.transformers


import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.utils.dateValue
import com.syriatel.d3m.greenmile.utils.indexArray


/**
 * cust_local_start_date4 -> startedAt
 * callingpartynumber38 -> performedBy
 * callingcellid47 -> performerCell
 * lasteffectoroffering61 -> offer
 * "debit_from_advance_prepaid15" + "debit_from_advance_postpaid" + "debit_from_advance_credit_postpaid17"  -> cost
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
            performerCell = it[indexArray[2]],
            type = ActionType.SMS,
            offer = it[indexArray[3]],
            cost = (it[indexArray[13]].toDoubleOrNull() ?: 0.0) + (it[indexArray[14]].toDoubleOrNull() ?: 0.0)
                    + (it[indexArray[15]].toDoubleOrNull() ?: 0.0),
            map = mutableMapOf(
                    "startedAt" to dateValue(it[indexArray[0]]),
                    "finishedAt" to dateValue(it[indexArray[5]]),
                    "deductedFrom" to it[indexArray[6]],
                    "actualDuration" to it[indexArray[7]].toIntOrNull(),
                    "systemDuration" to it[indexArray[8]].toIntOrNull(),
                    "group" to it[indexArray[9]],
                    "transactionType" to it[indexArray[10]],
                    "account" to it[indexArray[11]],
                    "balance" to it[indexArray[12]].toDoubleOrNull(),
                    "debitFromPrepaid" to it[indexArray[13]].toDoubleOrNull(),
                    "debitFromPostpaid" to it[indexArray[14]].toDoubleOrNull(),
                    "debitFromCredit" to it[indexArray[15]].toDoubleOrNull(),
                    "bfcOperation" to it[indexArray[16]],
                    "receivedBy" to it[indexArray[17]],
                    "originalRecipient" to it[indexArray[18]],
                    "process" to it[indexArray[19]],
                    "receiverCell" to it[indexArray[20]],
                    "simType" to it[indexArray[21]],
                    "payType" to it[indexArray[22]],
                    "roamState" to it[indexArray[23]],
                    "sessionType" to it[indexArray[24]],
                    "systemCost" to it[indexArray[25]].toDoubleOrNull(),
                    "prepaidBalance" to it[indexArray[26]].toLongOrNull(),
                    "postpaidBalance" to it[indexArray[27]].toLongOrNull(),
                    "splitIndicator" to it[indexArray[28]],
                    "usageServiceType" to it[indexArray[29]],
                    "performerCugno" to it[indexArray[30]],
                    "receiverCugno" to it[indexArray[31]],
                    "performerVpn" to it[indexArray[32]],
                    "receiverVpn" to it[indexArray[33]],
                    "discountedValue1" to it[indexArray[34]].toIntOrNull(),
                    "discountedValue2" to it[indexArray[35]].toIntOrNull()

            )
    )


}
