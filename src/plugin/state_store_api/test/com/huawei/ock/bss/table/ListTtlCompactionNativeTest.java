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

/** Verifies physical cleanup through actual background compaction and JNI. */
public class ListTtlCompactionNativeTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();
    private BoostStateDB open(boolean ttl) throws Exception {
        NativeUtil.getInstance().loadLibrary();
        BoostConfig config = new BoostConfig(new KeyGroupRange(0, 1));
        config.setMaxParallelism(2);
        config.setInstanceBasePath(temporary.newFolder().getAbsolutePath());
        config.setBackendUID(UUID.randomUUID().toString());
        config.setTaskSlotFlag(923453);
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

    @Test
    @SuppressWarnings("unchecked")
    public void customClockIsRejectedBeforeChangingNativeTtlForEveryTableType() throws Exception {
        BoostStateDB db = open(true);
        try {
            TableSerializer<Integer, String> serializer =
                new TableSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
            AbstractTable<?, ?>[] tables = {
                (KVTableImpl<Integer, String>) db.getTableOrCreate(new KVTableDescription<>("value", 2, serializer)),
                (KListTableImpl<Integer, String>) db.getTableOrCreate(new KListTableDescription<>("list", 2, serializer)),
                (KMapTableImpl<Integer, Integer, String>) db.getTableOrCreate(new KMapTableDescription<>("map", 2,
                    new SubTableSerializer<>(IntSerializer.INSTANCE, IntSerializer.INSTANCE, StringSerializer.INSTANCE)))
            };
            TableConfig custom = new TableConfig.Builder().setTableTtl(60000)
                .setTtlTimeProvider(() -> 1L).build();
            for (AbstractTable<?, ?> table : tables) {
                try {
                    table.updateTableConfig(custom);
                    fail("Custom Flink clock cannot be used with native TTL filtering");
                } catch (IllegalArgumentException expected) {
                    assertTrue(expected.getMessage().contains("TtlTimeProvider.DEFAULT"));
                }
                table.updateTableConfig(new TableConfig.Builder().setTableTtl(60000).build());
            }
        } finally {
            db.close();
        }
    }


    @Test
    @SuppressWarnings("unchecked")
    public void compactionRemovesOutOfOrderExpiredElementsWithoutRenewingSurvivors() throws Exception {
        BoostStateDB db = open(true);
        try {
            TtlStateFactory.TtlSerializer<String> serializer =
                new TtlStateFactory.TtlSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
            KListTableImpl<Integer, TtlValue<String>> lists = (KListTableImpl<Integer, TtlValue<String>>)
                db.getTableOrCreate(new KListTableDescription<>("lists", 2,
                    new TableSerializer<>(IntSerializer.INSTANCE, serializer)));
            NsKListTableImpl<Integer, Integer, TtlValue<String>> nsLists =
                (NsKListTableImpl<Integer, Integer, TtlValue<String>>) db.getTableOrCreate(
                    new NsKListTableDescription<>("nsLists", 2, new TableSerializer<>(
                        new KeyPairSerializer<>(IntSerializer.INSTANCE, IntSerializer.INSTANCE), serializer)));
            long liveTimestamp = System.currentTimeMillis();
            TableConfig ttl = new TableConfig.Builder().setTableTtl(600000).build();
            lists.updateTableConfig(ttl);
            nsLists.updateTableConfig(ttl);
            TtlValue<String> old = new TtlValue<>("expired", 1);
            TtlValue<String> live = new TtlValue<>("live", liveTimestamp);
            // Storage seqId remains current; element TTL is deliberately non-monotonic.
            lists.put(1, Arrays.asList(old, live, old));
            lists.put(2, Arrays.asList(old));
            lists.put(3, Arrays.asList(live));
            lists.add(3, old); // An expired append must not mask this live PUT.
            lists.put(4, Arrays.asList(live));
            lists.put(4, Arrays.asList(old)); // Expiring the replacement must not resurrect live.
            KeyPair<Integer, Integer> key = new KeyPair<>(1, 7);
            nsLists.put(key, Arrays.asList(old, live, old));
            assertEquals(3, lists.get(1).size());
            assertEquals(3, nsLists.get(key).size());
            KVTableImpl<Integer, String> filler = (KVTableImpl<Integer, String>) db.getTableOrCreate(
                new KVTableDescription<>("filler", 2,
                    new TableSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE)));
            filler.updateTableConfig(new TableConfig.Builder().setTableTtl(600000).build());
            char[] chars = new char[4096]; Arrays.fill(chars, 'x');
            String payload = new String(chars);
            boolean cleaned = false;
            for (int wave = 0; wave < 12 && !cleaned; wave++) {
                if (wave < 3) { lists.add(1, old); nsLists.add(key, old); }
                for (int i = 0; i < 16384; i++) { filler.put(i, payload); }
                Thread.sleep(1000);
                List<TtlValue<String>> actual = lists.get(1);
                List<TtlValue<String>> nsActual = nsLists.get(key);
                // A live sequence timestamp keeps the whole chunk visible until the list callback runs.
                cleaned = actual != null && actual.size() == 1
                    && nsActual != null && nsActual.size() == 1;
            }
            assertTrue("Background compaction must physically remove expired list elements", cleaned);
            assertNull("Fully expired PUT must be physically absent", lists.get(2));
            assertEquals(1, lists.get(3).size());
            assertEquals("live", lists.get(3).get(0).getUserValue());
            assertNull("Expired replacement must not resurrect an older PUT", lists.get(4));
            assertEquals("live", lists.get(1).get(0).getUserValue());
            assertEquals(liveTimestamp, lists.get(1).get(0).getLastAccessTimestamp());
            assertEquals("live", nsLists.get(key).get(0).getUserValue());
            assertEquals(liveTimestamp, nsLists.get(key).get(0).getLastAccessTimestamp());
            lists.add(1, new TtlValue<>("appended", System.currentTimeMillis()));
            assertEquals(2, lists.get(1).size());
            assertEquals("appended", lists.get(1).get(1).getUserValue());
        } finally { db.close(); }
    }
}
