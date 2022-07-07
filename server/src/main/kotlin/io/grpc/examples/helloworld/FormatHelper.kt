package io.grpc.examples.helloworld

import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.util.JsonFormat
import org.json.JSONObject
import org.json.XML

fun GeneratedMessageV3.asXml() : String =
    XML.toString(JSONObject(asJson()))

fun GeneratedMessageV3.Builder<*>.fromJson(json: String): Unit =
    JsonFormat.parser().merge(json, this)

fun GeneratedMessageV3.Builder<*>.fromXML(xml: String): Unit =
    fromJson(XML.toJSONObject(xml).toString(2))

