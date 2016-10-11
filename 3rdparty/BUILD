
jar_library(
  name='spark-core',
  jars = [
    jar('org.apache.spark', 'spark-core_2.10', '1.6.1')
  ]
)

jar_library(
  name='spire',
  jars = [
    jar('org.spire-math', 'spire_2.10', '0.11.0')
  ]
)

jar_library(
  name='args4j',
  jars = [
    jar('args4j', 'args4j', '2.33')
  ]
)

jar_library(
  name='scalatest',
  jars = [
    jar('org.scalatest', 'scalatest_2.10', '3.0.0')
  ]
)

jar_library(
  name='spark-testing-base',
  jars = [
    jar('com.holdenkarau', 'spark-testing-base_2.10', '1.6.1_0.4.4')
  ]
)

jar_library(
  name='junit',
  jars=[
    jar('junit', 'junit', '4.12')
  ],
  # junit is frequently used only for its annotations.
  #scope='forced',
)

jar_library(
  name='kryo',
  jars=[
    jar('com.esotericsoftware.kryo', 'kryo', '2.21')
  ]
)

jar_library(
  name='hadoop',
  jars=[
    jar(
      org='org.apache.hadoop',
      name='hadoop-common',
      rev='2.6.0',
      excludes=[
        exclude(org='javax.servlet', name='servlet-api'),
      ]
    )
  ]
)

jar_library(
  name='commons-io',
  jars=[
    jar('commons-io', 'commons-io', '2.5')
  ]
)

jar_library(
  name='hadoop-mr',
  jars=[
    jar('org.apache.hadoop', 'hadoop-mapreduce-client-core', '2.2.0')
  ]
)

jar_library(
  name='commons-math',
  jars=[
    jar('org.apache.commons', 'commons-math3', '3.4.1')
  ]
)
