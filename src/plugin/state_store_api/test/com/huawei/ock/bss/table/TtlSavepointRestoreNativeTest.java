/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistatestore is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

package com.huawei.ock.bss.table;

import com.huawei.ock.bss.common.BoostStateDB;
import com.huawei.ock.bss.common.conf.BoostConfig;
import com.huawei.ock.bss.common.conf.TableConfig;
import com.huawei.ock.bss.common.serialize.TableSerializer;
import com.huawei.ock.bss.common.serialize.SubTableSerializer;
import com.huawei.ock.bss.common.serialize.KeyPairSerializer;
import com.huawei.ock.bss.table.description.KListTableDescription;
import com.huawei.ock.bss.table.description.KVTableDescription;
import com.huawei.ock.bss.table.description.KMapTableDescription;
import com.huawei.ock.bss.table.description.NsKListTableDescription;
import com.huawei.ock.bss.table.namespace.NsKListTableImpl;
import com.huawei.ock.bss.table.namespace.NsKVTableImpl;
import com.huawei.ock.bss.table.namespace.NsKMapTableImpl;
import com.huawei.ock.bss.table.description.NsKVTableDescription;
import com.huawei.ock.bss.table.description.NsKMapTableDescription;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

/** Real JNI restore writes, followed by native physical cleanup. */
public class TtlSavepointRestoreNativeTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    private BoostStateDB open(boolean ttl) throws Exception {
        NativeUtil.getInstance().loadLibrary();
        BoostConfig config = new BoostConfig(new KeyGroupRange(0, 1));
        config.setMaxParallelism(2);
        config.setInstanceBasePath(temporary.newFolder().getAbsolutePath());
        config.setBackendUID(UUID.randomUUID().toString());
        config.setTaskSlotFlag(923452);
        config.setTaskSlotMemoryLimit(128L * 1024 * 1024);
        config.setTotalDBSize(128L * 1024 * 1024);
        config.setFileMemoryFraction(0.2F);
        config.setBorrowHeapSize(128L * 1024 * 1024);
        config.setSliceMemWaterMark(0.01F);
        config.setLsmStoreCompressionPolicy("lz4");
        config.setLsmStoreCompressionLevelPolicy("lz4,lz4,lz4");
        config.setTtlFilterSwitch(ttl);
        config.setEnableBloomFilter(true);
        config.setExpectedKeyCount(100000);
        return new BoostStateDB(config);
    }

    private TtlStateFactory.TtlSerializer<String> serializer() {
        return new TtlStateFactory.TtlSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    private void flushWithFiller(BoostStateDB db) {
        KVTableImpl<Integer, String> filler = (KVTableImpl<Integer, String>) db.getTableOrCreate(
            new KVTableDescription<>("filler", 2,
                new TableSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE)));
        if (db.getConfig().getTtlFilterSwitch()) {
            filler.updateTableConfig(new TableConfig.Builder().setTableTtl(60000).build());
        }
        char[] chars = new char[4096];
        Arrays.fill(chars, 'x');
        String payload = new String(chars);
        for (int i = 0; i < 16384; i++) {
            filler.put(i, payload);
        }
    }

    private static void assertTtlEquals(TtlValue<String> expected, TtlValue<String> actual) {
        assertNotNull(actual);
        assertEquals(expected.getUserValue(), actual.getUserValue());
        assertEquals(expected.getLastAccessTimestamp(), actual.getLastAccessTimestamp());
    }

    private static void assertListEquals(List<TtlValue<String>> expected, List<TtlValue<String>> actual) {
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertTtlEquals(expected.get(i), actual.get(i));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void restoresAllSixTableTypesWithoutRenewingTtl() throws Exception {
        BoostStateDB db = open(true);
        try {
            TtlStateFactory.TtlSerializer<String> serializer = serializer();
            TableSerializer<Integer, TtlValue<String>> tableSerializer =
                new TableSerializer<>(IntSerializer.INSTANCE, serializer);
            SubTableSerializer<Integer, Integer, TtlValue<String>> subSerializer =
                new SubTableSerializer<>(IntSerializer.INSTANCE, IntSerializer.INSTANCE, serializer);
            KeyPairSerializer<Integer, Integer> pairSerializer =
                new KeyPairSerializer<>(IntSerializer.INSTANCE, IntSerializer.INSTANCE);
            KVTableImpl<Integer, TtlValue<String>> values = (KVTableImpl<Integer, TtlValue<String>>)
                db.getTableOrCreate(new KVTableDescription<>("values", 2, tableSerializer));
            KListTableImpl<Integer, TtlValue<String>> lists = (KListTableImpl<Integer, TtlValue<String>>)
                db.getTableOrCreate(new KListTableDescription<>("lists", 2, tableSerializer));
            KMapTableImpl<Integer, Integer, TtlValue<String>> maps =
                (KMapTableImpl<Integer, Integer, TtlValue<String>>) db.getTableOrCreate(
                    new KMapTableDescription<>("maps", 2, subSerializer));
            NsKVTableImpl<Integer, Integer, TtlValue<String>> nsValues =
                (NsKVTableImpl<Integer, Integer, TtlValue<String>>) db.getTableOrCreate(
                    new NsKVTableDescription<>("nsValues", 2, subSerializer));
            NsKListTableImpl<Integer, Integer, TtlValue<String>> nsLists =
                (NsKListTableImpl<Integer, Integer, TtlValue<String>>) db.getTableOrCreate(
                    new NsKListTableDescription<>("nsLists", 2, new TableSerializer<>(pairSerializer, serializer)));
            NsKMapTableImpl<Integer, Integer, Integer, TtlValue<String>, java.util.Map<Integer, TtlValue<String>>> nsMaps =
                (NsKMapTableImpl<Integer, Integer, Integer, TtlValue<String>, java.util.Map<Integer, TtlValue<String>>>)
                    db.getTableOrCreate(new NsKMapTableDescription<>("nsMaps", 2,
                        new SubTableSerializer<>(pairSerializer, IntSerializer.INSTANCE, serializer)));
            long now = System.currentTimeMillis();
            TtlValue<String> old = new TtlValue<>("expired", 1);
            TtlValue<String> live = new TtlValue<>("live", now);
            // Deliberately restore newer timestamps before old ones.
            TtlSavepointRestore.restoreValue(values, 2, null, live);
            TtlSavepointRestore.restoreValue(values, 1, null, old);
            TtlSavepointRestore.restoreValue(nsValues, 1, 8, live);
            TtlSavepointRestore.restoreValue(nsValues, 1, 7, old);
            TtlSavepointRestore.restoreMapEntry(maps, 1, null, 2, live);
            TtlSavepointRestore.restoreMapEntry(maps, 1, null, 1, old);
            TtlSavepointRestore.restoreMapEntry(nsMaps, 1, 7, 2, live);
            TtlSavepointRestore.restoreMapEntry(nsMaps, 1, 7, 1, old);
            List<TtlValue<String>> mixed = Arrays.asList(old, live, new TtlValue<>("older", 0));
            TtlSavepointRestore.restoreList(lists, 1, null, Arrays.asList(old));
            TtlSavepointRestore.restoreList(lists, 2, null, mixed);
            TtlSavepointRestore.restoreList(nsLists, 1, 7, Arrays.asList(old));
            TtlSavepointRestore.restoreList(nsLists, 2, 7, mixed);
            TableConfig ttl = new TableConfig.Builder().setTableTtl(60000).build();
            for (AbstractTable<?, ?> table : Arrays.asList(values, nsValues, maps, nsMaps, lists, nsLists)) {
                table.updateTableConfig(ttl);
            }
            assertListEquals(mixed, lists.get(2));
            assertListEquals(mixed, nsLists.get(new KeyPair<>(2, 7)));
            flushWithFiller(db);
            KeyPair<Integer, Integer> oldPair = new KeyPair<>(1, 7);
            KeyPair<Integer, Integer> mixedPair = new KeyPair<>(2, 7);
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (System.nanoTime() < deadline) {
                boolean oldValuesRemain = values.get(1) != null || lists.get(1) != null
                    || nsValues.get(1, 7) != null;
                boolean oldMapsRemain = maps.get(1, 1) != null || nsMaps.get(oldPair, 1) != null
                    || nsLists.get(oldPair) != null;
                if (!oldValuesRemain && !oldMapsRemain) {
                    break;
                }
                Thread.sleep(10);
            }
            assertNull(values.get(1));
            assertNull(nsValues.get(1, 7));
            assertNull(maps.get(1, 1));
            assertNull(nsMaps.get(oldPair, 1));
            assertNull(lists.get(1));
            assertNull(nsLists.get(oldPair));
            assertTtlEquals(live, values.get(2));
            assertTtlEquals(live, nsValues.get(1, 8));
            assertTtlEquals(live, maps.get(1, 2));
            assertTtlEquals(live, nsMaps.get(oldPair, 2));
            // The list's sequence timestamp is live, so native sequence filtering alone
            // cannot remove its old elements. Wait for physical list cleanup after flush.
            long cleanupDeadline = System.nanoTime() + 60_000_000_000L;
            for (int wave = 0; wave < 12 && System.nanoTime() < cleanupDeadline; wave++) {
                List<TtlValue<String>> remaining = lists.get(2);
                List<TtlValue<String>> nsRemaining = nsLists.get(mixedPair);
                if (remaining != null && remaining.size() == 1
                    && nsRemaining != null && nsRemaining.size() == 1) {
                    break;
                }
                // Keep the restored keys in successive slices so their list callback runs.
                if (wave < 3) {
                    lists.add(2, old);
                    nsLists.add(mixedPair, old);
                }
                flushWithFiller(db);
                Thread.sleep(1000);
            }
            assertListEquals(Arrays.asList(live), lists.get(2));
            assertListEquals(Arrays.asList(live), nsLists.get(mixedPair));
        } finally {
            db.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void disabledTtlAndSubsequentListAppendPreserveValues() throws Exception {
        BoostStateDB db = open(false);
        try {
            TableSerializer<Integer, TtlValue<String>> tableSerializer =
                new TableSerializer<>(IntSerializer.INSTANCE, serializer());
            KListTableImpl<Integer, TtlValue<String>> lists = (KListTableImpl<Integer, TtlValue<String>>)
                db.getTableOrCreate(new KListTableDescription<>("lists", 2, tableSerializer));
            TtlValue<String> old = new TtlValue<>("old", 1);
            TtlValue<String> future = new TtlValue<>("future", System.currentTimeMillis() + 600000);
            TtlSavepointRestore.restoreList(lists, 1, null, Arrays.asList(future, old));
            TtlSavepointRestore.restoreList(lists, 2, null, Arrays.asList(old));
            flushWithFiller(db);
            TtlValue<String> appended = new TtlValue<>("appended", System.currentTimeMillis());
            lists.add(1, appended);
            assertListEquals(Arrays.asList(future, old, appended), lists.get(1));
            assertListEquals(Arrays.asList(old), lists.get(2));
            lists.put(1, Arrays.asList(appended));
            assertListEquals(Arrays.asList(appended), lists.get(1));
            lists.remove(1);
            assertNull(lists.get(1));
        } finally {
            db.close();
        }
    }
}
