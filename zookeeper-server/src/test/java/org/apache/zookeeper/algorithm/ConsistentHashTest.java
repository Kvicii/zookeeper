package org.apache.zookeeper.algorithm;

import org.junit.Test;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author kyushu
 * @date 2020/4/1 22:06
 * @description
 */
public class ConsistentHashTest {

    /**
     * 没有虚拟节点的一致性哈希
     */
    @Test
    public void consistentHashNoViatualTest() {

        // 初始化服务器节点 建立与hash环的对应关系
        TreeMap<Integer, String> map = new TreeMap<>();
        String[] serverList = new String[]{"138.133.13", "162.13.90.24", "184.24.43.111", "199.34.22.11"};
        String[] clientList = new String[]{"98.48.13.44", "77.13.34.58", "99.4.31.11"};
        for (String serverIp : serverList) {
            int index = Math.abs(serverIp.hashCode());
            map.put(index, serverIp);
        }
        // 客户端ip hash
        for (String clientIp : clientList) {
            int index = Math.abs(clientIp.hashCode());
            // 顺时针找节点
            SortedMap<Integer, String> res = map.tailMap(index);
            if (res == null && index > map.firstKey()) {
                System.out.println(map.get(map.firstKey()));
            } else {
                System.out.println(res.get(res.firstKey()));
            }
        }

    }

    /**
     * 有虚拟节点的一致性哈希
     */
    @Test
    public void consistentHashWithViatualTest() {

        // 初始化服务器节点 建立与hash环的对应关系
        TreeMap<Integer, String> map = new TreeMap<>();
        String[] serverList = new String[]{"138.133.13", "162.13.90.24", "184.24.43.111", "199.34.22.11"};
        String[] clientList = new String[]{"98.48.13.44", "77.13.34.58", "99.4.31.11"};

        int viatualCount = 3;
        for (String serverIp : serverList) {
//            int index = Math.abs(serverIp.hashCode());
            for (int i = 0; i < viatualCount; i++) {
                Integer viatualIndex = Math.abs((serverIp + "#" + (16 << 2 + i)).hashCode());
                map.put(viatualIndex, serverIp);
            }
        }
        // 客户端ip hash
        for (String clientIp : clientList) {
            int index = Math.abs(clientIp.hashCode());
            // 顺时针找节点
            SortedMap<Integer, String> res = map.tailMap(index);
            if (res == null && index > map.firstKey()) {
                System.out.println(map.get(map.firstKey()));
            } else {
                System.out.println(res.get(res.firstKey()));
            }
        }

    }
}
