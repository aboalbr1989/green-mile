package com.syriatel.d3m.greenmile.transformers

import com.syriatel.d3m.greenmile.domain.Action
import com.syriatel.d3m.greenmile.domain.ActionType
import com.syriatel.d3m.greenmile.utils.dateValue
import com.syriatel.d3m.greenmile.utils.indexArray


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
            timeStamp = dateValue(it[indexArray[0]]),
            performedBy = it[indexArray[1]],
            type = ActionType.ActivateBundle,
            offer = it[indexArray[2]],
            cost = (it[indexArray[13]].toDoubleOrNull() ?: 0.0) + (it[indexArray[14]].toDoubleOrNull() ?: 0.0)
                    + (it[indexArray[15]].toDoubleOrNull() ?: 0.0)
    ).apply {
        putAll(
                mapOf(
                        "startedAt" to dateValue(it[indexArray[0]]),
                        "finishedAt" to dateValue(it[indexArray[4]]),
                        "transactionType" to it[indexArray[5]],
                        "balance" to it[indexArray[6]].toDoubleOrNull(),
                        "debitFromPrepaid" to it[indexArray[7]].toDoubleOrNull(),
                        "debitFromPostpaid" to it[indexArray[8]].toDoubleOrNull(),
                        "debitFromCredit" to it[indexArray[9]].toDoubleOrNull(),
                        "bfcOperation" to it[indexArray[10]],
                        "simType" to it[indexArray[11]],
                        "payType" to it[indexArray[12]],
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



