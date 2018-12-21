/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class GetPartitionsFilterSpec implements org.apache.thrift.TBase<GetPartitionsFilterSpec, GetPartitionsFilterSpec._Fields>, java.io.Serializable, Cloneable, Comparable<GetPartitionsFilterSpec> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("GetPartitionsFilterSpec");

  private static final org.apache.thrift.protocol.TField FILTER_MODE_FIELD_DESC = new org.apache.thrift.protocol.TField("filterMode", org.apache.thrift.protocol.TType.I32, (short)7);
  private static final org.apache.thrift.protocol.TField FILTERS_FIELD_DESC = new org.apache.thrift.protocol.TField("filters", org.apache.thrift.protocol.TType.LIST, (short)8);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new GetPartitionsFilterSpecStandardSchemeFactory());
    schemes.put(TupleScheme.class, new GetPartitionsFilterSpecTupleSchemeFactory());
  }

  private PartitionFilterMode filterMode; // optional
  private List<String> filters; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see PartitionFilterMode
     */
    FILTER_MODE((short)7, "filterMode"),
    FILTERS((short)8, "filters");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 7: // FILTER_MODE
          return FILTER_MODE;
        case 8: // FILTERS
          return FILTERS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.FILTER_MODE,_Fields.FILTERS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FILTER_MODE, new org.apache.thrift.meta_data.FieldMetaData("filterMode", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, PartitionFilterMode.class)));
    tmpMap.put(_Fields.FILTERS, new org.apache.thrift.meta_data.FieldMetaData("filters", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(GetPartitionsFilterSpec.class, metaDataMap);
  }

  public GetPartitionsFilterSpec() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetPartitionsFilterSpec(GetPartitionsFilterSpec other) {
    if (other.isSetFilterMode()) {
      this.filterMode = other.filterMode;
    }
    if (other.isSetFilters()) {
      List<String> __this__filters = new ArrayList<String>(other.filters);
      this.filters = __this__filters;
    }
  }

  public GetPartitionsFilterSpec deepCopy() {
    return new GetPartitionsFilterSpec(this);
  }

  @Override
  public void clear() {
    this.filterMode = null;
    this.filters = null;
  }

  /**
   * 
   * @see PartitionFilterMode
   */
  public PartitionFilterMode getFilterMode() {
    return this.filterMode;
  }

  /**
   * 
   * @see PartitionFilterMode
   */
  public void setFilterMode(PartitionFilterMode filterMode) {
    this.filterMode = filterMode;
  }

  public void unsetFilterMode() {
    this.filterMode = null;
  }

  /** Returns true if field filterMode is set (has been assigned a value) and false otherwise */
  public boolean isSetFilterMode() {
    return this.filterMode != null;
  }

  public void setFilterModeIsSet(boolean value) {
    if (!value) {
      this.filterMode = null;
    }
  }

  public int getFiltersSize() {
    return (this.filters == null) ? 0 : this.filters.size();
  }

  public java.util.Iterator<String> getFiltersIterator() {
    return (this.filters == null) ? null : this.filters.iterator();
  }

  public void addToFilters(String elem) {
    if (this.filters == null) {
      this.filters = new ArrayList<String>();
    }
    this.filters.add(elem);
  }

  public List<String> getFilters() {
    return this.filters;
  }

  public void setFilters(List<String> filters) {
    this.filters = filters;
  }

  public void unsetFilters() {
    this.filters = null;
  }

  /** Returns true if field filters is set (has been assigned a value) and false otherwise */
  public boolean isSetFilters() {
    return this.filters != null;
  }

  public void setFiltersIsSet(boolean value) {
    if (!value) {
      this.filters = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FILTER_MODE:
      if (value == null) {
        unsetFilterMode();
      } else {
        setFilterMode((PartitionFilterMode)value);
      }
      break;

    case FILTERS:
      if (value == null) {
        unsetFilters();
      } else {
        setFilters((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FILTER_MODE:
      return getFilterMode();

    case FILTERS:
      return getFilters();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FILTER_MODE:
      return isSetFilterMode();
    case FILTERS:
      return isSetFilters();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GetPartitionsFilterSpec)
      return this.equals((GetPartitionsFilterSpec)that);
    return false;
  }

  public boolean equals(GetPartitionsFilterSpec that) {
    if (that == null)
      return false;

    boolean this_present_filterMode = true && this.isSetFilterMode();
    boolean that_present_filterMode = true && that.isSetFilterMode();
    if (this_present_filterMode || that_present_filterMode) {
      if (!(this_present_filterMode && that_present_filterMode))
        return false;
      if (!this.filterMode.equals(that.filterMode))
        return false;
    }

    boolean this_present_filters = true && this.isSetFilters();
    boolean that_present_filters = true && that.isSetFilters();
    if (this_present_filters || that_present_filters) {
      if (!(this_present_filters && that_present_filters))
        return false;
      if (!this.filters.equals(that.filters))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_filterMode = true && (isSetFilterMode());
    list.add(present_filterMode);
    if (present_filterMode)
      list.add(filterMode.getValue());

    boolean present_filters = true && (isSetFilters());
    list.add(present_filters);
    if (present_filters)
      list.add(filters);

    return list.hashCode();
  }

  @Override
  public int compareTo(GetPartitionsFilterSpec other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetFilterMode()).compareTo(other.isSetFilterMode());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFilterMode()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.filterMode, other.filterMode);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetFilters()).compareTo(other.isSetFilters());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFilters()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.filters, other.filters);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("GetPartitionsFilterSpec(");
    boolean first = true;

    if (isSetFilterMode()) {
      sb.append("filterMode:");
      if (this.filterMode == null) {
        sb.append("null");
      } else {
        sb.append(this.filterMode);
      }
      first = false;
    }
    if (isSetFilters()) {
      if (!first) sb.append(", ");
      sb.append("filters:");
      if (this.filters == null) {
        sb.append("null");
      } else {
        sb.append(this.filters);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class GetPartitionsFilterSpecStandardSchemeFactory implements SchemeFactory {
    public GetPartitionsFilterSpecStandardScheme getScheme() {
      return new GetPartitionsFilterSpecStandardScheme();
    }
  }

  private static class GetPartitionsFilterSpecStandardScheme extends StandardScheme<GetPartitionsFilterSpec> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, GetPartitionsFilterSpec struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 7: // FILTER_MODE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.filterMode = org.apache.hadoop.hive.metastore.api.PartitionFilterMode.findByValue(iprot.readI32());
              struct.setFilterModeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 8: // FILTERS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list1000 = iprot.readListBegin();
                struct.filters = new ArrayList<String>(_list1000.size);
                String _elem1001;
                for (int _i1002 = 0; _i1002 < _list1000.size; ++_i1002)
                {
                  _elem1001 = iprot.readString();
                  struct.filters.add(_elem1001);
                }
                iprot.readListEnd();
              }
              struct.setFiltersIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, GetPartitionsFilterSpec struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.filterMode != null) {
        if (struct.isSetFilterMode()) {
          oprot.writeFieldBegin(FILTER_MODE_FIELD_DESC);
          oprot.writeI32(struct.filterMode.getValue());
          oprot.writeFieldEnd();
        }
      }
      if (struct.filters != null) {
        if (struct.isSetFilters()) {
          oprot.writeFieldBegin(FILTERS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.filters.size()));
            for (String _iter1003 : struct.filters)
            {
              oprot.writeString(_iter1003);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GetPartitionsFilterSpecTupleSchemeFactory implements SchemeFactory {
    public GetPartitionsFilterSpecTupleScheme getScheme() {
      return new GetPartitionsFilterSpecTupleScheme();
    }
  }

  private static class GetPartitionsFilterSpecTupleScheme extends TupleScheme<GetPartitionsFilterSpec> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, GetPartitionsFilterSpec struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetFilterMode()) {
        optionals.set(0);
      }
      if (struct.isSetFilters()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetFilterMode()) {
        oprot.writeI32(struct.filterMode.getValue());
      }
      if (struct.isSetFilters()) {
        {
          oprot.writeI32(struct.filters.size());
          for (String _iter1004 : struct.filters)
          {
            oprot.writeString(_iter1004);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, GetPartitionsFilterSpec struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.filterMode = org.apache.hadoop.hive.metastore.api.PartitionFilterMode.findByValue(iprot.readI32());
        struct.setFilterModeIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list1005 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.filters = new ArrayList<String>(_list1005.size);
          String _elem1006;
          for (int _i1007 = 0; _i1007 < _list1005.size; ++_i1007)
          {
            _elem1006 = iprot.readString();
            struct.filters.add(_elem1006);
          }
        }
        struct.setFiltersIsSet(true);
      }
    }
  }

}

