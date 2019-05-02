package com.esri.sparkstreaming

import com.esri.arcgis.st.{FeatureSchema, Tag, Feature}

object FeatureFunctions {

  implicit class FeatureAttributeReader(val feature: Feature) extends AnyVal {

    def trackId(implicit schema: FeatureSchema): String = {
      val taggedValue = getTaggedValue(schema, Tag.TRACK_ID)
      if (taggedValue != null) taggedValue match {
        case s: String => s
        case _ => taggedValue.toString
      } else null
    }

    def getTaggedValue(schema: FeatureSchema, tagName: String): Any = {
      val attributes = schema.taggedAttributes(tagName)
      attributes.headOption.map(attribute => feature(schema.attributeIndex(attribute.name))).orNull
    }
  }
}
