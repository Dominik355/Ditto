package com.bilik.ditto.transformation.vw;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.bilik.ditto.transformation.MapMapper;
import com.bilik.ditto.transformation.ProtoMapper;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * The class holds a feature definition and ProtoMapper instance for accessing data in the protobuf.
 * When an instance is created there is some magic around transformation.
 * Transformation has to have a definition in Transformation class, so we could easily validate and transform data
 * also access data and transform them.
 */
public class Feature {

    private String featureInProto;
    private String featureName;
    private char namespace;

    private FeatureType featureType;
    private TransformationDefinition transformation;
    private final Feature[] subFeatures;
    transient private final BiFunction transformationMethod;
    private final boolean multipleFeatures;
    private ProtoMapper protoMapper;
    private final MapMapper mapMapper;

    public Feature(String featureInProto, Descriptors.Descriptor descriptor) {
        this('\0', "", featureInProto, FeatureType.NON_BINARY, null, descriptor);
    }

    public Feature(String featureName, String featureInProto, Descriptors.Descriptor descriptor) {
        this('\0', featureName, featureInProto, FeatureType.NON_BINARY, null, descriptor);
    }

    public Feature(char namespace, String featureName, String featureInProto, Descriptors.Descriptor descriptor) {
        this(namespace, featureName, featureInProto, FeatureType.NON_BINARY, null, descriptor);
    }

    public Feature(char namespace, String featureName, String featureInProto, FeatureType featureType, Descriptors.Descriptor descriptor) {
        this(namespace, featureName, featureInProto, featureType, null, descriptor);
    }

    public enum FeatureType {
        BINARY, // the binary value of vowpal feature means it's in name of the feature and always is 1.0
        NON_BINARY, // normal feature name with a value
        AUTO_FILLED, // auto-filled <- we don't have to setup name and value all is in string
        PREDICT_VALUE // predict value is the value which says in our case if this line was clicked or not
    }

    public Feature(char namespace,
                   String featureName,
                   String featureInProto,
                   FeatureType featureType,
                   TransformationDefinition transformation,
                   Descriptors.Descriptor descriptor) {
        this.namespace = namespace;
        this.featureName = featureName;
        this.featureType = featureType;
        this.featureInProto = featureInProto;
        this.transformation = transformation;
        this.mapMapper = new MapMapper(featureInProto);
        Feature[] subFeatures;
        BiFunction<?,?,?> transformationMethod;
        // get information about transformation
        if (transformation != null) {
            multipleFeatures = transformation.getOthers() != null && transformation.getOthers().length > 0;
            if (multipleFeatures) {
                subFeatures = new Feature[transformation.getOthers().length + 1];
                subFeatures[0] = this;
                for (int i = 0; i < transformation.getOthers().length ; ++i) {
                    subFeatures[i+1] = transformation.getOthers()[i];
                }
            } else {
                subFeatures = null;
            }
            transformationMethod = TransformationProvider.getInstance().getTransformFunction(transformation.getFunction());
        } else {
            multipleFeatures = false;
            subFeatures = null;
            transformationMethod = null;
        }
        this.transformationMethod = transformationMethod;
        this.subFeatures = subFeatures;
        if (descriptor != null) {
            this.protoMapper = new ProtoMapper(descriptor, featureInProto);
        }
    }

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public FeatureType getFeatureType() {
        return featureType;
    }

    public void setFeatureType(FeatureType featureType) {
        this.featureType = featureType;
    }

    public String getFeatureInProto() {
        return featureInProto;
    }

    public void setFeatureInProto(String featureInProto) {
        this.featureInProto = featureInProto;
    }

    public TransformationDefinition getTransformation() {
        return transformation;
    }

    public void setTransformation(TransformationDefinition transformation) {
        this.transformation = transformation;
    }

    public char getNamespace() {
        return namespace;
    }

    public void setNamespace(char namespace) {
        this.namespace = namespace;
    }

    public Object getValue(Object message) {
        if (message instanceof Message) {
            initializeProtoMapper((Message) message);
            return protoMapper.getValue((Message) message);
        } else if (message instanceof Map)  {
            return mapMapper.getValue((Map<String, Object>) message);
        }
        return null;
    }

    public Object getTransformedValue(Object message) {
        if (transformationMethod != null) {
            if (!multipleFeatures) {
                return transformationMethod.apply(message, this);
            } else {
                return transformationMethod.apply(message, subFeatures);
            }
        }

        if (message instanceof Message) {
            initializeProtoMapper((Message) message);
            return protoMapper.getValue((Message) message);
        } else if (message instanceof Map)  {
            return mapMapper.getValue((Map<String, Object>) message);
        }
        return null;
    }

    private String getStringFeatureFromObj(Object message) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getFeatureName());
        switch (this.getFeatureType()) {
            case BINARY: {
                stringBuilder.append(getTransformedValue(message).toString());
                stringBuilder.append(":1.0");
                break;
            }
            case NON_BINARY: {
                stringBuilder.append(":");
                stringBuilder.append(getTransformedValue(message).toString());
                break;
            }
            case AUTO_FILLED:
            case PREDICT_VALUE: {
                stringBuilder.append(getTransformedValue(message).toString());
                break;
            }
            default:
                break;
        }
        return stringBuilder.toString();
    }

    public String getStringFeature(Object message) {
        return getStringFeatureFromObj(message);
    }

    private void initializeProtoMapper(Message message) {
        if (protoMapper == null) {
            protoMapper = new ProtoMapper(message.getDescriptorForType(), featureInProto);
        }
    }

    @Override
    public String toString() {
        return "ns: " + this.namespace + " featName: "+ this.featureName + " pathToProto: "+this.featureInProto;
    }
}