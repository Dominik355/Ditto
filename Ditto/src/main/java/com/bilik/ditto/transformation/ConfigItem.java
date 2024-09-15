package com.bilik.ditto.transformation;

import com.alibaba.fastjson.JSONObject;
import com.bilik.ditto.transformation.vw.TransformationDefinition;

/**
 * Represents single configuration item in json flat schema.
 * e.g.
 *<p>   ...
 *<p>   "age_id": {
 *<p>     "name": "age_id",
 *<p>     "transformation": null,
 *<p>     "defaultValue": 0
 *<p>   },
 *<p>   ...
 * ConfigItem is stored under object's name ('age_id') in config map
 */
public class ConfigItem {
    public final String name;
    public final TransformationDefinition transformation;
    public final Object defaultValue;

    public ConfigItem(JSONObject map) {
        this(map.getString("name"),
                map.getObject("transformation", TransformationDefinition.class),
                map.get("defaultValue"));
    }

    public ConfigItem(String name, TransformationDefinition transformation) {
        this(name, transformation, null);
    }

    public ConfigItem(String name, TransformationDefinition transformation, Object defaultValue) {
        this.name = name;
        this.transformation = transformation;
        this.defaultValue = defaultValue;
    }

}