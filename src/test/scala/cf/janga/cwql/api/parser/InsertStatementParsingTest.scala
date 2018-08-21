package cf.janga.cwql.api.parser

import org.scalatest.{Matchers, WordSpec}

class InsertStatementParsingTest extends WordSpec with Matchers {

  "cwql parser" when {
    "given an insert statement" should {
      "parse insertion of a single metric data" in {
        val insertString = "insert into TestNamespace values (latency, Milliseconds, 1234) with InstanceID=123456"
        val Right(insert: Insert) = new Parser().parse(insertString)

        insert.namespace.value should be("TestNamespace")
        insert.namespace.aliasOption should be(None)

        insert.metricData.size should be(1)
        val Some(latencyMetricData) = insert.metricData.headOption
        latencyMetricData.metricName should be("latency")
        latencyMetricData.metricUnit should be("Milliseconds")
        latencyMetricData.value should be("1234")

        insert.dimensions.size should be(1)
        val Some(instanceIdDimension) = insert.dimensions.headOption
        instanceIdDimension.name should be("InstanceID")
        instanceIdDimension.value should be("123456")
      }

      "parse insertion of multiple metric data" in {
        val insertString = "insert into TestNamespace values (latency, Milliseconds, 1234), (buffer, Bytes, 98765) with InstanceID=123456"
        val Right(insert: Insert) = new Parser().parse(insertString)

        insert.namespace.value should be("TestNamespace")
        insert.namespace.aliasOption should be(None)

        insert.metricData.size should be(2)
        val latencyMetricData = insert.metricData(0)
        latencyMetricData.metricName should be("latency")
        latencyMetricData.metricUnit should be("Milliseconds")
        latencyMetricData.value should be("1234")
        val bufferMetricData = insert.metricData(1)
        bufferMetricData.metricName should be("buffer")
        bufferMetricData.metricUnit should be("Bytes")
        bufferMetricData.value should be("98765")

        insert.dimensions.size should be(1)
        val Some(instanceIdDimension) = insert.dimensions.headOption
        instanceIdDimension.name should be("InstanceID")
        instanceIdDimension.value should be("123456")
      }

      "parse insertion of multiple dimensions" in {
        val insertString = "insert into TestNamespace values (latency, Milliseconds, 1234), (buffer, Bytes, 98765) with InstanceID=123456, InstanceType=t2.micro"
        val Right(insert: Insert) = new Parser().parse(insertString)

        insert.namespace.value should be("TestNamespace")
        insert.namespace.aliasOption should be(None)

        insert.metricData.size should be(2)
        val latencyMetricData = insert.metricData(0)
        latencyMetricData.metricName should be("latency")
        latencyMetricData.metricUnit should be("Milliseconds")
        latencyMetricData.value should be("1234")
        val bufferMetricData = insert.metricData(1)
        bufferMetricData.metricName should be("buffer")
        bufferMetricData.metricUnit should be("Bytes")
        bufferMetricData.value should be("98765")

        insert.dimensions.size should be(2)
        val instanceIdDimension = insert.dimensions(0)
        instanceIdDimension.name should be("InstanceID")
        instanceIdDimension.value should be("123456")
        val instanceTypeDimension = insert.dimensions(1)
        instanceTypeDimension.name should be("InstanceType")
        instanceTypeDimension.value should be("t2.micro")
      }
    }
  }
}
