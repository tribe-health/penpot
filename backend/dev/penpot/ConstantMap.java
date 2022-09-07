/*
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.

  Copyright (c) UXBOX Labs SL

  This file contains a UUIDv8 with conformance with
  https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format
*/

package penpot;

import java.util.LinkedList;
import java.util.Iterator;
import java.util.UUID;
import java.util.HashMap;
import java.nio.ByteBuffer;
import clojure.lang.IFn;
import clojure.lang.PersistentHashMap;
import clojure.lang.IPersistentMap;
import clojure.lang.MapEntry;

public class ConstantMap implements Iterable {
  public final int recordSize = 16 + 4;
  public final IFn encodeFn;
  public final IFn decodeFn;

  public int pending = 0;

  public IPersistentMap cache = PersistentHashMap.EMPTY;
  public IPersistentMap positions = PersistentHashMap.EMPTY;

  public ByteBuffer blob;
  public ByteBuffer header;
  public ByteBuffer content;


  public static ConstantMap createFromBlob(final Object encodeFn,
                                    final Object decodeFn,
                                    final byte[] blob) {
    return new ConstantMap((IFn)encodeFn, (IFn)decodeFn, ByteBuffer.wrap(blob));
  }

  public static ConstantMap createEmpty(Object encodeFn, Object decodeFn) {
    var buff = ByteBuffer.allocate(4);
    buff.putInt(0, 0);

    return new ConstantMap((IFn)encodeFn, (IFn)decodeFn, buff);
  }

  public ConstantMap(final IFn encodeFn,
                      final IFn decodeFn,
                      final IPersistentMap positions,
                      final IPersistentMap cache,
                      final ByteBuffer blob,
                      final ByteBuffer header,
                      final ByteBuffer content,
                      final int pending) {
    this.encodeFn = encodeFn;
    this.decodeFn = decodeFn;
    this.cache = cache;
    this.positions = positions;
    this.blob = blob;
    this.header = header;
    this.content = content;
    this.pending = pending;
  }

  public ConstantMap(final IFn encodeFn,
                      final IFn decodeFn,
                      final ByteBuffer blob) {

    this.encodeFn = encodeFn;
    this.decodeFn = decodeFn;
    this.blob = blob;

    var headerSize = blob.getInt(0);
    header = blob.slice(4, headerSize);
    content = blob.slice(headerSize+4, blob.remaining() - (headerSize+4));

    var nitems = header.remaining() / recordSize;

    for (int i=0; i<nitems; i++) {
      var hbuff = header.slice(i*recordSize, recordSize);
      var msb = hbuff.getLong();
      var lsb = hbuff.getLong();
      var met = hbuff.getLong();
      positions = positions.assoc(new UUID(msb, lsb), met);
    }
  }

  public ConstantMap set(final UUID key, final Object val) {
    return new ConstantMap(encodeFn,
                           decodeFn,
                           positions.assoc(key, -1L),
                           cache.assoc(key, val),
                           blob,
                           header,
                           content,
                           pending + 1);
  }

  public Object get(final UUID key) {
    if (cache.containsKey(key)) {
      return cache.valAt(key);
    }

    if (positions.containsKey(key)) {
      long met = (long) positions.valAt(key);
      var size = (int) (met >>> 32);
      var pos  = (int) (met & 0x0000_ffffL);

      int cnt = 0;
      var bbuff = new byte[size];
      content.get(pos, bbuff, 0, size);

      var val = this.decodeFn.invoke(bbuff);
      this.cache = this.cache.assoc(key, val);
      return val;
    }

    this.cache = this.cache.assoc(key, null);
    return null;
  }

  public Iterator iterator() {
    return new ConstantMapIterator(this);
  }

  public byte[] toBlob() {
    this.compact();
    return blob.array();
  }

  public void compact() {
    if (this.pending == 0) {
      return;
    }

    int contentSize = 0;
    int contentElements = 0;

    var newItems = new HashMap<UUID, byte[]>(this.cache.count());

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
      }
    }

    var headerSize = contentElements * recordSize;
    var buff = ByteBuffer.allocate(headerSize + contentSize + 4);
    var header = buff.slice(4, headerSize);
    var content = buff.slice(headerSize+4, contentSize);

    long position = 0;

    for (Object entry: positions) {
      var mentry = (MapEntry) entry;

      var met = (long) (mentry.val());
      var key = (UUID) (mentry.key());

      // this means we just copy the object to new location
      if (met != -1L) {
        var hbuff = ByteBuffer.allocate(recordSize);
        hbuff.putLong(key.getMostSignificantBits());
        hbuff.putLong(key.getLeastSignificantBits());

        int size = (int)(met >>> 32);
        int pos  = (int)(met & 0xffff);

        met = (met & 0xffff_0000L) | (position & 0x0000_ffffL);

        hbuff.putLong(met);
        positions = positions.assoc(key, met);
        position += (long)size;

        header.put(hbuff);
        content.put(this.content.slice(pos, size));

      } else {
        byte[] bval = newItems.get(key);
        int size = bval.length;

        var hbuff = ByteBuffer.allocate(recordSize);
        hbuff.putLong(key.getMostSignificantBits());
        hbuff.putLong(key.getLeastSignificantBits());

        met = (((long)size << 32) & 0xffff_0000L) | (position & 0x0000_ffffL);
        hbuff.putLong(met);

        positions = positions.assoc(key, met);
        position += (long) size;

        header.put(hbuff);
        content.put(bval, 0, size);
      }
    }

    this.pending = 0;
    this.blob = buff;
    this.header = header;
    this.content = content;
  }

  public class ConstantMapIterator implements Iterator {
    final Iterator iterator;
    final ConstantMap data;

    public ConstantMapIterator(final ConstantMap data) {
      this.iterator = data.positions.iterator();
      this.data = data;
    }

    public boolean hasNext() {
      return this.iterator.hasNext();
    }

    public Object next() {
      var entry = (MapEntry) this.iterator.next();
      var key = (UUID) entry.key();
      return new MapEntry(key, data.get(key));
    }
  }
}
