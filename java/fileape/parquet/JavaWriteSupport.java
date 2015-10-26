package fileape.parquet;

import clojure.lang.ISeq;
import clojure.lang.PersistentVector;
import clojure.lang.Seqable;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.*;

/**
 * Implements write support for standard java Map, List, and primitives
 */
public class JavaWriteSupport extends WriteSupport<Map<?, ?>> {

    private RecordConsumer recordConsumer;
    private MessageType schema;
    private Map<String, String> meta;

    public JavaWriteSupport(MessageType schema) {
        this(schema, new HashMap());
    }

    public JavaWriteSupport(MessageType schema, Map<String, String> meta) {
        this.schema = schema;
        this.meta = meta;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, meta);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }


    public void write(Map<?, ?> msg) {
        recordConsumer.startMessage();
        writeData(schema, msg, false);
        recordConsumer.endMessage();
    }

    private void writeData(GroupType type, Map<?, ?> group){
        writeData(type, group, true);
    }

    /**
     * Recursively write data for each type.
     * The type schema is iterated and data drawn from the group:Map.
     * Each message in the schema should be represented as a Map, Lists can be List, Object[], ISeq, Seqable or Collection
     */
    private void writeData(GroupType type, Map<?, ?> group, boolean writeGroup) {

        if(writeGroup)
            recordConsumer.startGroup();

        for (int field = 0; field < type.getFieldCount(); field++) {

            Type fieldType = type.getType(field);

            Object fieldValue = group.get(fieldType.getName());


            if (fieldValue == null) {
                if (fieldType.isRepetition(Type.Repetition.REQUIRED))
                    throw new RuntimeException("Value for field " + fieldType.getName() + " cannot be null");
                else
                    continue;
            }

            if (fieldType.isPrimitive()) {
                if (fieldType.isRepetition(Type.Repetition.REPEATED)) {
                    List list = asList(fieldValue);
                    if (list.size() > 0) {

                        recordConsumer.startField(fieldType.getName(), field);
                        writePrimitiveList(fieldType.asPrimitiveType(), asList(fieldValue));

                        recordConsumer.endField(fieldType.getName(), field);

                    }
                } else {

                    recordConsumer.startField(fieldType.getName(), field);
                    writePrimitiveValue(fieldType.asPrimitiveType(), fieldValue);

                    recordConsumer.endField(fieldType.getName(), field);
                }

            } else {
                //not a primitive, must create a group, but groups can also be repeated
                if (fieldType.isRepetition(Type.Repetition.REPEATED)) {
                    List list = asList(fieldValue);
                    if (list.size() > 0) {

                            recordConsumer.startField(fieldType.getName(), field);
                            writeDataList(fieldType.asGroupType(), list);
                            recordConsumer.endField(fieldType.getName(), field);
                    }
                } else {
                    if (fieldValue instanceof Map) {
                        Map map = (Map) fieldValue;
                        if (map.size() > 0) {

                            recordConsumer.startField(fieldType.getName(), field);
                            writeData(fieldType.asGroupType(), map, true);
                            recordConsumer.endField(fieldType.getName(), field);
                        }
                    } else
                        throw createMessageMustBeMapException(fieldType, fieldValue);
                }
            }


        }

        if(writeGroup)
        recordConsumer.endGroup();
    }


    /**
     * Helper function to create RuntimeException when a Map is expected but another type is found
     */
    private RuntimeException createMessageMustBeMapException(Type type, Object val) {
        return new RuntimeException("A message must be represented as an intance that implements java.util.Map but got " + val + " but got " + type.getName());
    }

    /**
     * Helper function to call writeData for each item in the list, also we check that each item in the list is of type Map
     */
    private void writeDataList(GroupType groupType, List list) {
        for (Object val : list) {
            if (val instanceof Map)
                writeData(groupType, (Map) val, true);
            else
                throw createMessageMustBeMapException(groupType, val);
        }
    }

    /**
     * Helper function to call writePrimitiveValue for each item in the list
     *
     * @param primitiveType
     * @param list
     */
    private void writePrimitiveList(PrimitiveType primitiveType, List list) {
        for (Object val : list)
            writePrimitiveValue(primitiveType, val);
    }


    /**
     * Coerce an entry to a List, supports List, ISeq, Seqable, Object[], Collection
     *
     * @param entry
     * @return
     */
    private List asList(Object entry) {

        if (entry instanceof List)
            return (List) entry;
        else if (entry instanceof Seqable)
            return PersistentVector.create(((Seqable) entry).seq());
        else if (entry instanceof ISeq)
            return PersistentVector.create((ISeq) entry);
        else if (entry instanceof Object[])
            return Arrays.asList((Object[]) entry);
        else if (entry instanceof Collection)
            return new ArrayList((Collection) entry);
        else if (entry == null)
            return new ArrayList(0);
        else
            throw new RuntimeException("Type " + entry + " not supported");
    }

    /**
     * Write primitive values
     *
     * @param primitiveType
     * @param v
     */
    private void writePrimitiveValue(PrimitiveType primitiveType, Object v) {

        switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN:
                recordConsumer.addBoolean((v instanceof Boolean) ? ((Boolean) v).booleanValue() : false);
                break;
            case BINARY:
                recordConsumer.addBinary(asBinary(v));
                break;
            case INT32:
                recordConsumer.addInteger((v instanceof Number) ? ((Number) v).intValue() : -1);
                break;
            case INT64:
                recordConsumer.addLong((v instanceof Number) ? ((Number) v).longValue() : -1);
                break;
            case FLOAT:
                recordConsumer.addFloat((v instanceof Number) ? ((Number) v).floatValue() : -1);
                break;
            case DOUBLE:
                recordConsumer.addDouble((v instanceof Number) ? ((Number) v).doubleValue() : -1);
                break;
            default:
                throw new RuntimeException("Type " + primitiveType.getPrimitiveTypeName() + " is not supported");
        }
    }

    /**
     * Convert an object when byte[] to a Binary or using toString to a Binary
     */
    private Binary asBinary(Object v) {
        if (v instanceof byte[])
            return Binary.fromConstantByteArray((byte[]) v);
        else
            return Binary.fromString(v.toString());
    }

}
