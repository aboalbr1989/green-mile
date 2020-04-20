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

