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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.UUID;

public class ConstantMap implements Iterable, IObj, IPersistentMap, IHashEq {
  public static IFn encodeFn;
  public static IFn decodeFn;
  public static int RECORD_SIZE = 16 + 8;

  public static long POSITION_MASK = 0x0000_0000_ffff_ffffL;

  public IPersistentMap cache;
  public IPersistentMap positions;

  public ByteBuffer blob;
  public ByteBuffer header;
  public ByteBuffer content;
  public int pending = 0;

  IPersistentMap meta = PersistentArrayMap.EMPTY;

  int _hasheq;

  // -----------------------------------------------------------------
  // ---- Static setters
  // -----------------------------------------------------------------

  public static void setEncodeFn(IFn f) {
    encodeFn = f;
  }

  public static void setDecodeFn(IFn f) {
    decodeFn = f;
  }

  // -----------------------------------------------------------------
  // ---- Static constructors
  // -----------------------------------------------------------------

  public static ConstantMap createFromBlob(final ByteBuffer blob) {
    var headerSize = blob.getInt(0);
    var header = blob.slice(4, headerSize);
    var content = blob.slice(headerSize+4, blob.remaining() - (headerSize+4));

    var nitems = header.remaining() / RECORD_SIZE;
    IPersistentMap positions = PersistentHashMap.EMPTY;

    for (int i=0; i<nitems; i++) {
      var hbuff = header.slice(i*RECORD_SIZE, RECORD_SIZE);
      var msb = hbuff.getLong();
      var lsb = hbuff.getLong();
      var met = hbuff.getLong();
      positions = positions.assoc(new UUID(msb, lsb), met);
    }

    return new ConstantMap((IPersistentMap)positions,
                           (IPersistentMap)PersistentHashMap.EMPTY,
                           blob,
                           header,
                           content,
                           0);
  }

  public static ConstantMap createFromBlob(final byte[] buff) {
    return createFromBlob(ByteBuffer.wrap(buff));
  }

  public static ConstantMap createEmpty() {
    var blob = ByteBuffer.allocate(4);
    blob.putInt(0, 0);
    return createFromBlob(blob);
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
  }

  private ConstantMap newInstance() {
    return new ConstantMap(this.positions,
                           this.cache,
                           this.blob,
                           this.header,
                           this.content,
                           this.pending);
  }

  // -----------------------------------------------------------------
  // ---- IObj
  // -----------------------------------------------------------------

  public IPersistentMap meta() {
    return this.meta;
  }

  public IObj withMeta(IPersistentMap meta) {
    var instance = this.newInstance();
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
    throw UnsupportedOperationException("method not implemented");
  }
  
  @Override
  public IPersistentMap without(Object key) {
    return new ConstantMap(this.positions.withiut(key),
                           this.cache.without(key),
                           this.blob,
                           this.header,
                           this.content,
                           this.pending + 1);
  }

  // --- Associative

  public boolean containsKey(Object key) {
    return this.positions.containsKey(key);
  }

  public IMapEntry entryAt(Object key) {
    return new LazyMapEntry(this, key);
  }

  // --- ILookup

  public Object valAt(Object key) {
    return this.get((UUID) key);
  }

  public Object valAt(Object key, Object notFound) {
    if (this.positions.containsKey(key)) {
      return this.get((UUID) key);
    } else {
      return notFound;
    }
  }

  // --- Counted

  public int count() {
    return this.positions.count();
  }

  // --- Seqable

  public ISeq seq() {
    return RT.chunkIteratorSeq(this);
  }

  // --- IPersistentCollection

  public IPersistentCollection cons(Object o) {
    var entry = (MapEntry) o;
    return this.assoc(entry.key(), entry.val());
  }

  public IPersistentCollection empty() {
    return createEmpty();
  }

  public boolean equiv(Object o) {
  }

  // --- IHashEq

  public int hasheq() {
    if (this._hasheq == 0) {
      this._hasheq = Murmur3.hashUnordered(this);
    }

    return this._hasheq;
  }

  public int hashCode() {
    return this.hasheq();
  }

  // -----------------------------------------------------------------
  // ---- OTHER
  // -----------------------------------------------------------------

  public ConstantMap set(final UUID key, final Object val) {
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
      var met = (long) positions.valAt(key);
      var size = (int) (met >>> 32);
      var pos  = (int) (met & POSITION_MASK);

      int cnt = 0;
      var bbuff = new byte[size-4];
      content.get(pos+4, bbuff, 0, size-4);

      var val = this.decodeFn.invoke(bbuff);
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
      var met = (long) positions.valAt(key);
      var pos = (int) (met & POSITION_MASK);
      return this.content.getInt(pos);
    }
  }

  public Iterator iterator() {
    return new LazyMapIterator(this);
  }

  public byte[] toBlob() {
    this.compact();
    return blob.array();
  }

  public void compact() {
    if (this.pending == 0) {
      return;
    }

    // Calculate the total of elements and serialize the new ones
    int contentSize = 0;
    int contentElements = 0;

    var newItems = new HashMap<UUID, byte[]>(this.cache.count());
    var newHashes = new HashMap<UUID, int>(this.cache.count());

    for (Object entry: positions) {
      var mentry = (MapEntry) entry;
      var key = mentry.key();
      var met = (long) mentry.val();

      contentElements++;

      if (met != -1L) {
        var size = (int) (met >>> 32);
        contentSize += size;
      } else {
        var oval = cache.valAt(key);
        var bval = (byte[]) encodeFn.invoke(oval);
        contentSize += bval.length;

        newItems.put((UUID) key, bval);
        newHashes.put((UUID) key, Util.hasheq(oval));
      }
    }

    var headerSize = contentElements * RECORD_SIZE;

    var buff = ByteBuffer.allocate(headerSize + contentSize + 4);
    var header = buff.slice(4, headerSize);
    var content = buff.slice(headerSize+4, contentSize);

    buff.putInt(0, headerSize);

    long position = 0;

    var tmpRecordBuffer = ByteBuffer.allocate(RECORD_SIZE);

    for (Object entry: positions) {
      var mentry = (MapEntry) entry;
      var met    = (long) (mentry.val());
      var key    = (UUID) (mentry.key());

      tmpRecordBuffer.putLong(key.getMostSignificantBits());
      tmpRecordBuffer.putLong(key.getLeastSignificantBits());

      // this means we just copy the object to new location
      if (met != -1L) {
        int size = (int)(met >>> 32);
        int prevPos = (int)(met & 0xffff_ffff);

        met = (met & 0xffff_ffff_0000_0000L) | (position & 0x0000_0000_ffff_ffffL);

        tmpRecordBuffer.putLong(met);

        positions = positions.assoc(key, met);
        position += (long)size;

        header.put(tmpRecordBuffer.rewind());
        content.put(this.content.slice(prevPos, size));

      } else {
        byte[] bval = newItems.get(key);
        int hval = newHashes.get(key);
        int size = bval.length + 4;

        met = (((long)size << 32) & 0xffff_ffff_0000_0000L) | (position & 0x0000_0000_ffff_ffffL);

        tmpRecordBuffer.putLong(met);

        positions = positions.assoc(key, met);
        position += (long) size;

        header.put(tmpRecordBuffer.rewind());

        content.putInt(hval);
        content.put(bval, 0, size);
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

    public Object getVal() {
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
