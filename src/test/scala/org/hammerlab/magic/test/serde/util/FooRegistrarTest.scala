package org.hammerlab.magic.test.serde.util

import org.hammerlab.magic.test.spark.KryoSerializerSuite

/**
 * Test base-class that registers dummy case-class [[Foo]] for Kryo serde.
 */
class FooRegistrarTest
  extends KryoSerializerSuite(
    registrar = classOf[FooKryoRegistrator],
    registrationRequired = false,
    referenceTracking = true
  )
