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
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class GetTableRequest implements org.apache.thrift.TBase<GetTableRequest, GetTableRequest._Fields>, java.io.Serializable, Cloneable, Comparable<GetTableRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("GetTableRequest");

  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("dbName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TBL_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("tblName", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField CAPABILITIES_FIELD_DESC = new org.apache.thrift.protocol.TField("capabilities", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField CAT_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("catName", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField TXN_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("txnId", org.apache.thrift.protocol.TType.I64, (short)5);
  private static final org.apache.thrift.protocol.TField VALID_WRITE_ID_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("validWriteIdList", org.apache.thrift.protocol.TType.STRING, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new GetTableRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new GetTableRequestTupleSchemeFactory());
  }

  private String dbName; // required
  private String tblName; // required
  private ClientCapabilities capabilities; // optional
  private String catName; // optional
  private long txnId; // optional
  private String validWriteIdList; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DB_NAME((short)1, "dbName"),
    TBL_NAME((short)2, "tblName"),
    CAPABILITIES((short)3, "capabilities"),
    CAT_NAME((short)4, "catName"),
    TXN_ID((short)5, "txnId"),
    VALID_WRITE_ID_LIST((short)6, "validWriteIdList");

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
        case 1: // DB_NAME
          return DB_NAME;
        case 2: // TBL_NAME
          return TBL_NAME;
        case 3: // CAPABILITIES
          return CAPABILITIES;
        case 4: // CAT_NAME
          return CAT_NAME;
        case 5: // TXN_ID
          return TXN_ID;
        case 6: // VALID_WRITE_ID_LIST
          return VALID_WRITE_ID_LIST;
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
  private static final int __TXNID_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.CAPABILITIES,_Fields.CAT_NAME,_Fields.TXN_ID,_Fields.VALID_WRITE_ID_LIST};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.DB_NAME, new org.apache.thrift.meta_data.FieldMetaData("dbName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TBL_NAME, new org.apache.thrift.meta_data.FieldMetaData("tblName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CAPABILITIES, new org.apache.thrift.meta_data.FieldMetaData("capabilities", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ClientCapabilities.class)));
    tmpMap.put(_Fields.CAT_NAME, new org.apache.thrift.meta_data.FieldMetaData("catName", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TXN_ID, new org.apache.thrift.meta_data.FieldMetaData("txnId", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.VALID_WRITE_ID_LIST, new org.apache.thrift.meta_data.FieldMetaData("validWriteIdList", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(GetTableRequest.class, metaDataMap);
  }

  public GetTableRequest() {
    this.txnId = -1L;

  }

  public GetTableRequest(
    String dbName,
    String tblName)
  {
    this();
    this.dbName = dbName;
    this.tblName = tblName;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetTableRequest(GetTableRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetTblName()) {
      this.tblName = other.tblName;
    }
    if (other.isSetCapabilities()) {
      this.capabilities = new ClientCapabilities(other.capabilities);
    }
    if (other.isSetCatName()) {
      this.catName = other.catName;
    }
    this.txnId = other.txnId;
    if (other.isSetValidWriteIdList()) {
      this.validWriteIdList = other.validWriteIdList;
    }
  }

  public GetTableRequest deepCopy() {
    return new GetTableRequest(this);
  }

  @Override
  public void clear() {
    this.dbName = null;
    this.tblName = null;
    this.capabilities = null;
    this.catName = null;
    this.txnId = -1L;

    this.validWriteIdList = null;
  }

  public String getDbName() {
    return this.dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void unsetDbName() {
    this.dbName = null;
  }

  /** Returns true if field dbName is set (has been assigned a value) and false otherwise */
  public boolean isSetDbName() {
    return this.dbName != null;
  }

  public void setDbNameIsSet(boolean value) {
    if (!value) {
      this.dbName = null;
    }
  }

  public String getTblName() {
    return this.tblName;
  }

  public void setTblName(String tblName) {
    this.tblName = tblName;
  }

  public void unsetTblName() {
    this.tblName = null;
  }

  /** Returns true if field tblName is set (has been assigned a value) and false otherwise */
  public boolean isSetTblName() {
    return this.tblName != null;
  }

  public void setTblNameIsSet(boolean value) {
    if (!value) {
      this.tblName = null;
    }
  }

  public ClientCapabilities getCapabilities() {
    return this.capabilities;
  }

  public void setCapabilities(ClientCapabilities capabilities) {
    this.capabilities = capabilities;
  }

  public void unsetCapabilities() {
    this.capabilities = null;
  }

  /** Returns true if field capabilities is set (has been assigned a value) and false otherwise */
  public boolean isSetCapabilities() {
    return this.capabilities != null;
  }

  public void setCapabilitiesIsSet(boolean value) {
    if (!value) {
      this.capabilities = null;
    }
  }

  public String getCatName() {
    return this.catName;
  }

  public void setCatName(String catName) {
    this.catName = catName;
  }

  public void unsetCatName() {
    this.catName = null;
  }

  /** Returns true if field catName is set (has been assigned a value) and false otherwise */
  public boolean isSetCatName() {
    return this.catName != null;
  }

  public void setCatNameIsSet(boolean value) {
    if (!value) {
      this.catName = null;
    }
  }

  public long getTxnId() {
    return this.txnId;
  }

  public void setTxnId(long txnId) {
    this.txnId = txnId;
    setTxnIdIsSet(true);
  }

  public void unsetTxnId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TXNID_ISSET_ID);
  }

  /** Returns true if field txnId is set (has been assigned a value) and false otherwise */
  public boolean isSetTxnId() {
    return EncodingUtils.testBit(__isset_bitfield, __TXNID_ISSET_ID);
  }

  public void setTxnIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TXNID_ISSET_ID, value);
  }

  public String getValidWriteIdList() {
    return this.validWriteIdList;
  }

  public void setValidWriteIdList(String validWriteIdList) {
    this.validWriteIdList = validWriteIdList;
  }

  public void unsetValidWriteIdList() {
    this.validWriteIdList = null;
  }

  /** Returns true if field validWriteIdList is set (has been assigned a value) and false otherwise */
  public boolean isSetValidWriteIdList() {
    return this.validWriteIdList != null;
  }

  public void setValidWriteIdListIsSet(boolean value) {
    if (!value) {
      this.validWriteIdList = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case DB_NAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String)value);
      }
      break;

    case TBL_NAME:
      if (value == null) {
        unsetTblName();
      } else {
        setTblName((String)value);
      }
      break;

    case CAPABILITIES:
      if (value == null) {
        unsetCapabilities();
      } else {
        setCapabilities((ClientCapabilities)value);
      }
      break;

    case CAT_NAME:
      if (value == null) {
        unsetCatName();
      } else {
        setCatName((String)value);
      }
      break;

    case TXN_ID:
      if (value == null) {
        unsetTxnId();
      } else {
        setTxnId((Long)value);
      }
      break;

    case VALID_WRITE_ID_LIST:
      if (value == null) {
        unsetValidWriteIdList();
      } else {
        setValidWriteIdList((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case DB_NAME:
      return getDbName();

    case TBL_NAME:
      return getTblName();

    case CAPABILITIES:
      return getCapabilities();

    case CAT_NAME:
      return getCatName();

    case TXN_ID:
      return getTxnId();

    case VALID_WRITE_ID_LIST:
      return getValidWriteIdList();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case DB_NAME:
      return isSetDbName();
    case TBL_NAME:
      return isSetTblName();
    case CAPABILITIES:
      return isSetCapabilities();
    case CAT_NAME:
      return isSetCatName();
    case TXN_ID:
      return isSetTxnId();
    case VALID_WRITE_ID_LIST:
      return isSetValidWriteIdList();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GetTableRequest)
      return this.equals((GetTableRequest)that);
    return false;
  }

  public boolean equals(GetTableRequest that) {
    if (that == null)
      return false;

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
        return false;
    }

    boolean this_present_tblName = true && this.isSetTblName();
    boolean that_present_tblName = true && that.isSetTblName();
    if (this_present_tblName || that_present_tblName) {
      if (!(this_present_tblName && that_present_tblName))
        return false;
      if (!this.tblName.equals(that.tblName))
        return false;
    }

    boolean this_present_capabilities = true && this.isSetCapabilities();
    boolean that_present_capabilities = true && that.isSetCapabilities();
    if (this_present_capabilities || that_present_capabilities) {
      if (!(this_present_capabilities && that_present_capabilities))
        return false;
      if (!this.capabilities.equals(that.capabilities))
        return false;
    }

    boolean this_present_catName = true && this.isSetCatName();
    boolean that_present_catName = true && that.isSetCatName();
    if (this_present_catName || that_present_catName) {
      if (!(this_present_catName && that_present_catName))
        return false;
      if (!this.catName.equals(that.catName))
        return false;
    }

    boolean this_present_txnId = true && this.isSetTxnId();
    boolean that_present_txnId = true && that.isSetTxnId();
    if (this_present_txnId || that_present_txnId) {
      if (!(this_present_txnId && that_present_txnId))
        return false;
      if (this.txnId != that.txnId)
        return false;
    }

    boolean this_present_validWriteIdList = true && this.isSetValidWriteIdList();
    boolean that_present_validWriteIdList = true && that.isSetValidWriteIdList();
    if (this_present_validWriteIdList || that_present_validWriteIdList) {
      if (!(this_present_validWriteIdList && that_present_validWriteIdList))
        return false;
      if (!this.validWriteIdList.equals(that.validWriteIdList))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_dbName = true && (isSetDbName());
    list.add(present_dbName);
    if (present_dbName)
      list.add(dbName);

    boolean present_tblName = true && (isSetTblName());
    list.add(present_tblName);
    if (present_tblName)
      list.add(tblName);

    boolean present_capabilities = true && (isSetCapabilities());
    list.add(present_capabilities);
    if (present_capabilities)
      list.add(capabilities);

    boolean present_catName = true && (isSetCatName());
    list.add(present_catName);
    if (present_catName)
      list.add(catName);

    boolean present_txnId = true && (isSetTxnId());
    list.add(present_txnId);
    if (present_txnId)
      list.add(txnId);

    boolean present_validWriteIdList = true && (isSetValidWriteIdList());
    list.add(present_validWriteIdList);
    if (present_validWriteIdList)
      list.add(validWriteIdList);

    return list.hashCode();
  }

  @Override
  public int compareTo(GetTableRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetDbName()).compareTo(other.isSetDbName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDbName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dbName, other.dbName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTblName()).compareTo(other.isSetTblName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTblName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tblName, other.tblName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCapabilities()).compareTo(other.isSetCapabilities());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCapabilities()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.capabilities, other.capabilities);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCatName()).compareTo(other.isSetCatName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCatName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.catName, other.catName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTxnId()).compareTo(other.isSetTxnId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTxnId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.txnId, other.txnId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetValidWriteIdList()).compareTo(other.isSetValidWriteIdList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValidWriteIdList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.validWriteIdList, other.validWriteIdList);
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
    StringBuilder sb = new StringBuilder("GetTableRequest(");
    boolean first = true;

    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("tblName:");
    if (this.tblName == null) {
      sb.append("null");
    } else {
      sb.append(this.tblName);
    }
    first = false;
    if (isSetCapabilities()) {
      if (!first) sb.append(", ");
      sb.append("capabilities:");
      if (this.capabilities == null) {
        sb.append("null");
      } else {
        sb.append(this.capabilities);
      }
      first = false;
    }
    if (isSetCatName()) {
      if (!first) sb.append(", ");
      sb.append("catName:");
      if (this.catName == null) {
        sb.append("null");
      } else {
        sb.append(this.catName);
      }
      first = false;
    }
    if (isSetTxnId()) {
      if (!first) sb.append(", ");
      sb.append("txnId:");
      sb.append(this.txnId);
      first = false;
    }
    if (isSetValidWriteIdList()) {
      if (!first) sb.append(", ");
      sb.append("validWriteIdList:");
      if (this.validWriteIdList == null) {
        sb.append("null");
      } else {
        sb.append(this.validWriteIdList);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetDbName()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'dbName' is unset! Struct:" + toString());
    }

    if (!isSetTblName()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'tblName' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (capabilities != null) {
      capabilities.validate();
    }
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class GetTableRequestStandardSchemeFactory implements SchemeFactory {
    public GetTableRequestStandardScheme getScheme() {
      return new GetTableRequestStandardScheme();
    }
  }

  private static class GetTableRequestStandardScheme extends StandardScheme<GetTableRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, GetTableRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // DB_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dbName = iprot.readString();
              struct.setDbNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TBL_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.tblName = iprot.readString();
              struct.setTblNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CAPABILITIES
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.capabilities = new ClientCapabilities();
              struct.capabilities.read(iprot);
              struct.setCapabilitiesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // CAT_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.catName = iprot.readString();
              struct.setCatNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // TXN_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.txnId = iprot.readI64();
              struct.setTxnIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // VALID_WRITE_ID_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.validWriteIdList = iprot.readString();
              struct.setValidWriteIdListIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, GetTableRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.dbName != null) {
        oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
        oprot.writeString(struct.dbName);
        oprot.writeFieldEnd();
      }
      if (struct.tblName != null) {
        oprot.writeFieldBegin(TBL_NAME_FIELD_DESC);
        oprot.writeString(struct.tblName);
        oprot.writeFieldEnd();
      }
      if (struct.capabilities != null) {
        if (struct.isSetCapabilities()) {
          oprot.writeFieldBegin(CAPABILITIES_FIELD_DESC);
          struct.capabilities.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.catName != null) {
        if (struct.isSetCatName()) {
          oprot.writeFieldBegin(CAT_NAME_FIELD_DESC);
          oprot.writeString(struct.catName);
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetTxnId()) {
        oprot.writeFieldBegin(TXN_ID_FIELD_DESC);
        oprot.writeI64(struct.txnId);
        oprot.writeFieldEnd();
      }
      if (struct.validWriteIdList != null) {
        if (struct.isSetValidWriteIdList()) {
          oprot.writeFieldBegin(VALID_WRITE_ID_LIST_FIELD_DESC);
          oprot.writeString(struct.validWriteIdList);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GetTableRequestTupleSchemeFactory implements SchemeFactory {
    public GetTableRequestTupleScheme getScheme() {
      return new GetTableRequestTupleScheme();
    }
  }

  private static class GetTableRequestTupleScheme extends TupleScheme<GetTableRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, GetTableRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.dbName);
      oprot.writeString(struct.tblName);
      BitSet optionals = new BitSet();
      if (struct.isSetCapabilities()) {
        optionals.set(0);
      }
      if (struct.isSetCatName()) {
        optionals.set(1);
      }
      if (struct.isSetTxnId()) {
        optionals.set(2);
      }
      if (struct.isSetValidWriteIdList()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetCapabilities()) {
        struct.capabilities.write(oprot);
      }
      if (struct.isSetCatName()) {
        oprot.writeString(struct.catName);
      }
      if (struct.isSetTxnId()) {
        oprot.writeI64(struct.txnId);
      }
      if (struct.isSetValidWriteIdList()) {
        oprot.writeString(struct.validWriteIdList);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, GetTableRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.dbName = iprot.readString();
      struct.setDbNameIsSet(true);
      struct.tblName = iprot.readString();
      struct.setTblNameIsSet(true);
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.capabilities = new ClientCapabilities();
        struct.capabilities.read(iprot);
        struct.setCapabilitiesIsSet(true);
      }
      if (incoming.get(1)) {
        struct.catName = iprot.readString();
        struct.setCatNameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.txnId = iprot.readI64();
        struct.setTxnIdIsSet(true);
      }
      if (incoming.get(3)) {
        struct.validWriteIdList = iprot.readString();
        struct.setValidWriteIdListIsSet(true);
      }
    }
  }

}

