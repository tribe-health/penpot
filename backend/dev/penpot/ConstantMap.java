/*
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.

  Copyright (c) UXBOX Labs SL

  This file contains a UUIDv8 with conformance with
  https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format
*/

package penpot;

import clojure.lang.AMapEntry;
import clojure.lang.IFn;
import clojure.lang.IHashEq;
import clojure.lang.IMapEntry;
import clojure.lang.IObj;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.MapEntry;
import clojure.lang.Murmur3;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashMap;
import clojure.lang.RT;
import clojure.lang.Util;
// import java.lang.UnsupportedOperationException

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.UUID;

public class ConstantMap implements Iterable, Cloneable, IObj, IPersistentMap, IHashEq {
  public static IFn encodeFn;
  public static IFn decodeFn;
  public static int RECORD_SIZE = 16 + 8;

  public static long POSITION_MASK = 0x0000_0000_ffff_ffffL;
  public static long SIZE_MASK = 0xffff_ffff_0000_0000L;

  public IPersistentMap cache;
  public IPersistentMap positions;

  public ByteBuffer blob;
  public ByteBuffer header;
  public ByteBuffer content;

  public int pending = 0;
  public boolean initialized = false;

  IPersistentMap meta = PersistentArrayMap.EMPTY;

  int _hasheq;

  // -----------------------------------------------------------------
  // ---- Static setters
  // -----------------------------------------------------------------

  public static void setup(IFn encode, IFn decode) {
    encodeFn = encode;
    decodeFn = decode;
  }

  public static ConstantMap EMPTY = createEmpty();

  // -----------------------------------------------------------------
  // ---- Static constructors
  // -----------------------------------------------------------------

  public static ConstantMap createFromByteArray(final byte[] buff) {
    return new ConstantMap(ByteBuffer.wrap(buff));
  }

  public static ConstantMap createEmpty() {
    var blob = ByteBuffer.allocate(4);
    blob.putInt(0, 0);
    return new ConstantMap(blob);
  }

  // -----------------------------------------------------------------
  // ---- Constructors
  // -----------------------------------------------------------------

  public ConstantMap(final IPersistentMap positions,
                     final IPersistentMap cache,
                     final ByteBuffer blob,
                     final ByteBuffer header,
                     final ByteBuffer content,
                     final int pending) {
    this.positions = positions;
    this.cache = cache;
    this.blob = blob;
    this.header = header;
    this.content = content;
    this.pending = pending;
    this.initialized = true;
  }

  public ConstantMap(final ByteBuffer blob) {
    this.blob = blob;
    this.initialized = false;
  }

  private void initialize() {
    if (this.initialized) return;

    var headerSize = this.blob.getInt(0);
    var header = this.blob.slice(4, headerSize);
    var content = this.blob.slice(headerSize+4, blob.remaining() - (headerSize+4));

    var nitems = header.remaining() / RECORD_SIZE;
    IPersistentMap positions = PersistentHashMap.EMPTY;

    for (int i=0; i<nitems; i++) {
      var hbuff = header.slice(i*RECORD_SIZE, RECORD_SIZE);
      var ra = hbuff.getLong();
      var rb = hbuff.getLong();
      var rc = hbuff.getLong();
      positions = positions.assoc(new UUID(ra, rb), rc);
    }

    this.positions = positions;
    this.cache = PersistentHashMap.EMPTY;
    this.header = header;
    this.content = content;
    this.pending = 0;
    this.initialized = true;
  }

  @Override
  public ConstantMap clone() {
    if (this.initialized) {
      return new ConstantMap(this.positions,
                             this.cache,
                             this.blob,
                             this.header,
                             this.content,
                             this.pending);
    } else {
      return new ConstantMap(this.blob);
    }
  }

  // -----------------------------------------------------------------
  // ---- IObj
  // -----------------------------------------------------------------

  public IPersistentMap meta() {
    return this.meta;
  }

  public IObj withMeta(IPersistentMap meta) {
    var instance = this.clone();
    instance.meta = meta;
    return instance;
  }

  // -----------------------------------------------------------------
  // ---- IPersistentMap
  // -----------------------------------------------------------------

  @Override
  public IPersistentMap assoc(Object key, Object val) {
    return this.set((UUID) key, val);
  }

  public IPersistentMap assocEx(Object key, Object val) {
    throw new UnsupportedOperationException("method not implemented");
  }

  @Override
  public IPersistentMap without(Object key) {
    this.initialize();
    return new ConstantMap(this.positions.without(key),
                           this.cache.without(key),
                           this.blob,
                           this.header,
                           this.content,
                           this.pending + 1);
  }

  // --- Associative

  public boolean containsKey(Object key) {
    this.initialize();
    return this.positions.containsKey(key);
  }

  public IMapEntry entryAt(Object key) {
    this.initialize();
    return new LazyMapEntry(this, key);
  }

  // --- ILookup

  public Object valAt(Object key) {
    return this.get((UUID) key);
  }

  public Object valAt(Object key, Object notFound) {
    this.initialize();
    if (this.positions.containsKey(key)) {
      return this.get((UUID) key);
    } else {
      return notFound;
    }
  }

  // --- Counted

  public int count() {
    this.initialize();
    return this.positions.count();
  }

  // --- Seqable

  public ISeq seq() {
    this.initialize();
    return RT.chunkIteratorSeq(this.iterator());
  }

  // --- IPersistentCollection

  public IPersistentCollection cons(Object o) {
    this.initialize();
    var entry = (MapEntry) o;
    return this.assoc(entry.key(), entry.val());
  }

  public IPersistentCollection empty() {
    return createEmpty();
  }

  public boolean equiv(Object o) {
    // System.out.println("call: equiv");
    return this == o;
  }

  // --- IHashEq

  public int hasheq() {
    // System.out.println("call: hasheq");
    if (this._hasheq == 0) {
      this._hasheq = Murmur3.hashUnordered(this);
    }

    return this._hasheq;
  }

  public int hashCode() {
    // System.out.println("call: hashCode");
    return this.hasheq();
  }

  // -----------------------------------------------------------------
  // ---- OTHER
  // -----------------------------------------------------------------

  public void forceModified() {
    for (Object entry: positions) {
      var mentry = (MapEntry) entry;
      var key    = (UUID) (mentry.key());
      var val    = this.get(key);

      this.positions = this.positions.assoc(key, -1L);
      this.cache = this.cache.assoc(key, val);
    }

    this.pending = 1;
  }

  public ConstantMap set(final UUID key, final Object val) {
    this.initialize();
    return new ConstantMap(this.positions.assoc(key, -1L),
                           this.cache.assoc(key, val),
                           this.blob,
                           this.header,
                           this.content,
                           this.pending + 1);
  }

  public Object get(final UUID key) {
    if (cache.containsKey(key)) {
      return cache.valAt(key);
    }

    if (positions.containsKey(key)) {
      var rc   = (long) positions.valAt(key);
      var size = this.getSizeFromRc(rc);
      var pos  = this.getPositionFromRc(rc);

      var tmpBuff = new byte[size-4];
      content.get(pos+4, tmpBuff, 0, size-4);

      var val = this.decodeFn.invoke(tmpBuff);
      this.cache = this.cache.assoc(key, val);
      return val;
    }

    this.cache = this.cache.assoc(key, null);
    return null;
  }

  int getHashEq(Object key) {
    if (cache.containsKey(key)) {
      return ((IHashEq)cache.valAt(key)).hasheq();
    } else {
      long rc = (long) this.positions.valAt(key);
      int pos = this.getPositionFromRc(rc);

      return this.content.getInt(pos);
    }
  }

  public Iterator iterator() {
    return new LazyMapIterator(this);
  }

  public byte[] toByteArray() {
    this.compact();
    return blob.array();
  }

  private long encodeRc(int size, int position) {
    return (((long)size << 32) & SIZE_MASK) | (position & POSITION_MASK);
  }

  private long updateRcWithPosition(long rc, int position) {
    return (rc & SIZE_MASK) | ((long)position & POSITION_MASK);
  }

  private int getSizeFromRc(long rc) {
    return (int) (rc >>> 32L);
  }

  private int getPositionFromRc(long rc) {
    return (int) (rc & POSITION_MASK);
  }

  public void compact() {
    if (this.pending == 0) {
      return;
    }

    // Calculate the total of elements and serialize the new ones
    int contentSize = 0;
    int contentElements = 0;

    var newItems = new HashMap<UUID, byte[]>(this.cache.count());
    var newHashes = new HashMap<UUID, Integer>(this.cache.count());

    for (Object entry: positions) {
      var mentry = (MapEntry) entry;
      var key = mentry.key();
      var rc = (long) mentry.val();

      contentElements++;

      if (rc != -1L) {
        var size = this.getSizeFromRc(rc);
        contentSize += size;
      } else {
        var oval = cache.valAt(key);
        var bval = (byte[]) encodeFn.invoke(oval);
        contentSize += (bval.length + 4);

        newItems.put((UUID) key, bval);
        newHashes.put((UUID) key, Util.hasheq(oval));
      }
    }

    var headerSize = contentElements * RECORD_SIZE;
    var buff = ByteBuffer.allocate(headerSize + contentSize + 4);
    var header = buff.slice(4, headerSize);
    var content = buff.slice(headerSize+4, contentSize);

    buff.putInt(0, headerSize);

    int position = 0;

    var tmpRecordBuffer = ByteBuffer.allocate(RECORD_SIZE);

    for (Object entry: positions) {
      var mentry = (MapEntry) entry;
      var rc     = (long) (mentry.val());
      var key    = (UUID) (mentry.key());

      tmpRecordBuffer.putLong(key.getMostSignificantBits());
      tmpRecordBuffer.putLong(key.getLeastSignificantBits());

      // System.out.println("=====");
      // System.out.println(key.toString());
      // System.out.println(Long.toString(rc));
      // System.out.println(rc != -1L);
      // System.out.println("=====");

      // this means we just copy the object to new location
      if (rc != -1L) {
        int size = this.getSizeFromRc(rc);
        int prevPos = this.getPositionFromRc(rc);

        rc = this.updateRcWithPosition(rc, position);

        tmpRecordBuffer.putLong(rc);

        positions = positions.assoc(key, rc);
        position += size;

        header.put(tmpRecordBuffer.rewind());
        content.put(this.content.slice(prevPos, size));

      } else {
        byte[] bval = newItems.get(key);
        int hval = newHashes.get(key);
        int size = bval.length + 4;

        rc = this.encodeRc(size, position);
        tmpRecordBuffer.putLong(rc);

        positions = positions.assoc(key, rc);
        position += size;

        tmpRecordBuffer.rewind();
        header.put(tmpRecordBuffer);

        content.putInt(hval);
        content.put(bval, 0, size - 4);
      }

      tmpRecordBuffer.clear();
    }

    header.rewind();
    content.rewind();
    buff.rewind();

    this.pending = 0;
    this.blob = buff;
    this.header = header;
    this.content = content;
  }

  public class LazyMapEntry extends AMapEntry implements IHashEq {
    final Object _key;
    final ConstantMap _cmap;

    public LazyMapEntry(ConstantMap cmap, Object key) {
      this._cmap = cmap;
      this._key = key;
    }

    public Object key() {
      return this._key;
    }

    public Object val() {
      return this._cmap.valAt(this._key);
    }

    public Object getKey() {
      return this.key();
    }

    public Object getValue() {
      return this.val();
    }

    public int hasheq() {
      return this._cmap.getHashEq(this._key);
    }
  }

  public class LazyMapIterator implements Iterator {
    final Iterator _iterator;
    final ConstantMap _cmap;

    public LazyMapIterator(final ConstantMap cmap) {
      this._iterator = cmap.positions.iterator();
      this._cmap = cmap;
    }

    public boolean hasNext() {
      return this._iterator.hasNext();
    }

    public Object next() {
      var entry = (MapEntry) this._iterator.next();
      return new LazyMapEntry(this._cmap, entry.key());
    }
  }
}
