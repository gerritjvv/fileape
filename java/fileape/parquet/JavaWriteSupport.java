package fileape.parquet;

import clojure.lang.ISeq;
import clojure.lang.PersistentVector;
import clojure.lang.Seqable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
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

    private static final Logger LOG = Logger.getLogger(JavaWriteSupport.class);

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
        if(msg == null)
            return;

        recordConsumer.startMessage();
        writeData(msg, schema);
        recordConsumer.endMessage();
    }

    private void writeData(final Map arr, final GroupType type) {
        if (arr == null) {
            return;
        }
        final int fieldCount = type.getFieldCount();

        for (int field = 0; field < fieldCount; ++field) {
            final Type fieldType = type.getType(field);
            final String fieldName = fieldType.getName();
            final Object value = arr.get(fieldName);

            if (value == null) {
                continue;
            }
            recordConsumer.startField(fieldName, field);

            if (fieldType.isPrimitive()) {
                writePrimitive(fieldType.asPrimitiveType(), value);
            } else {
                recordConsumer.startGroup();
                if (value instanceof Map) {
                    writeData((Map)value, fieldType.asGroupType());
                }else{
                    if (fieldType.asGroupType().getRepetition().equals(Type.Repetition.REPEATED)) {
                        writeArray((List) value, fieldType.asGroupType());
                    } else {
                        if(value instanceof Map)
                            writeData((Map)value, fieldType.asGroupType());
                        else {
                            writeArray((List)value, fieldType.asGroupType());
                        }
                    }
                }

                recordConsumer.endGroup();
            }

            recordConsumer.endField(fieldName, field);
        }
    }

    private void writeArray(final List array, final GroupType type) {
        if (array == null) {
            return;
        }

        final int fieldCount = type.getFieldCount();
        for (int field = 0; field < fieldCount; ++field) {
            final Type subType = type.getType(field);
            recordConsumer.startField(subType.getName(), field);
            for (int i = 0; i < array.size(); ++i) {
                final Object subValue = array.get(i);
                if (subValue != null) {
                    if (subType.isPrimitive()) {
                        if (subValue instanceof List) {
                            writePrimitive(subType.asPrimitiveType(), ((List) subValue).get(field));// 0 ?
                        } else {
                            writePrimitive(subType.asPrimitiveType(), subValue);
                        }
                    } else {

                            recordConsumer.startGroup();


                            if(subValue instanceof Map)
                                writeData((Map) subValue, subType.asGroupType());
                            else if(subType.isPrimitive())
                                writePrimitive(subType.asPrimitiveType(), subValue);
                            else if(!subType.isPrimitive()){
                                //if not a primitive we have a mismatch between how arrays in the schema are
                                //represented group <name> { repeated bag { group/primitive-type <name> } }
                                if(subValue instanceof List)
                                    writeArray(asList(subValue), subType.asGroupType());
                                else{
                                    //subValue is primitive but we have a group value
                                    writeArray(Arrays.asList(subValue), subType.asGroupType());
                                }

                            }
                            recordConsumer.endGroup();

                    }
                }
            }
            recordConsumer.endField(subType.getName(), field);
        }
    }

    private void writePrimitive(final PrimitiveType type, final Object v) {
        if (v == null) {
            return;
        }
        switch (type.getPrimitiveTypeName()) {
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
                throw new RuntimeException("Type " + type.getPrimitiveTypeName() + " is not supported");
        }
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
     * Convert an object when byte[] to a Binary or using toString to a Binary
     */
    private Binary asBinary(Object v) {
        if (v instanceof byte[])
            return Binary.fromConstantByteArray((byte[]) v);
        else
            return Binary.fromString(v.toString());
    }

}
