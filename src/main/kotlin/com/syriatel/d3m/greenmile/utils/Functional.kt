package com.syriatel.d3m.greenmile.utils

fun <A, B, C> Function1<A, B>.compose(fn: (B) -> C): (A) -> C = {
    fn(invoke(it))
}

/* function decomposition */
fun <A, B, C> Function2<A, B, C>.decompose(): (A) -> (B) -> (C) = { a ->
    { b ->
        invoke(a, b)
    }
}

fun <A, B, C, D> Function3<A, B, C, D>.decompose(): (A) -> (B) -> (C) -> (D) = { a ->
    { b ->
        { c ->
            invoke(a, b, c)
        }
    }
}

fun <A, B, C, D, E> Function4<A, B, C, D, E>.decompose(): (A) -> (B) -> (C) -> (D) -> E = { a ->
    { b ->
        { c ->
            { d ->
                invoke(a, b, c, d)
            }
        }
    }
}

/* curry */

fun <A, B> Function1<A, B>.curry(a: A): () -> B = {
    invoke(a)
}

fun <A, B, C> Function2<A, B, C>.curry(a: A): (B) -> C = {
    invoke(a, it)
}


fun <A, B, C, D> Function3<A, B, C, D>.curry(a: A): (B, C) -> D = { b, c ->
    invoke(a, b, c)
}

fun <A, B, C, D, E> Function4<A, B, C, D, E>.curry(a: A): (B, C, D) -> E = { b, c, d ->
    invoke(a, b, c, d)
}

/* tail curry */

fun <A, B> Function1<A, B>.tailCurry(a: A): () -> B = {
    invoke(a)
}

fun <A, B, C> Function2<A, B, C>.tailCurry(b: B): (A) -> C = {
    invoke(it, b)
}


fun <A, B, C, D> Function3<A, B, C, D>.tailCurry(c: C): (A, B) -> D = { a, b ->
    invoke(a, b, c)
}

fun <A, B, C, D, E> Function4<A, B, C, D, E>.tailCurry(d: D): (A, B, C) -> E = { a, b, c ->
    invoke(a, b, c, d)
}


/** Map Join **/

fun <K, T1, T2, R> Map<K, T1>.innerJoin(other: Map<K, T2>, joiner: (T1, T2) -> R): Map<K, R> =
        keys.intersect(other.keys).map {
            it to joiner(this[it] ?: error(""), other[it] ?: error(""))
        }.toMap()

fun <K, T1, T2, R> Map<K, T1>.leftJoin(other: Map<K, T2>, joiner: (T1, T2?) -> R): Map<K, R> =
        keys.map {
            it to joiner(this[it] ?: error(""), other[it])
        }.toMap()

fun <K, T1, T2, R> Map<K, T1>.fullJoin(other: Map<K, T2>, joiner: (T1?, T2?) -> R): Map<K, R> =
        (keys + other.keys).map {
            it to joiner(this[it], other[it])
        }.toMap()

fun <K, T1, T2, R> Map<K, T1>.fullJoin(other: Map<K, T2>, def1: T1, def2: T2, joiner: (T1, T2) -> R): Map<K, R> =
        (keys + other.keys).map {
            it to joiner(this[it] ?: def1, other[it] ?: def2)
        }.toMap()

fun <K, T1, R> Map<K, T1>.fullJoin(other: Map<K, T1>, replaceNullWith: T1, joiner: (T1, T1) -> R): Map<K, R> =
        (keys + other.keys).map {
            it to joiner(this[it] ?: replaceNullWith, other[it] ?: replaceNullWith)
        }.toMap()