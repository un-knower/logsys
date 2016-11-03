package cn.whaley.bi.logsys.common;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by fj on 15/10/27.
 */
public class ZKUtil implements Watcher {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ZooKeeper zooKeeper;
    private CountDownLatch connCD = new CountDownLatch(1);

    /**
     * @param conns zookeeper连接字符串,如172.16.3.42:2181,172.16.3.65:2181,172.16.3.24:2181
     * @throws IOException
     */
    public ZKUtil(String conns) throws IOException, InterruptedException {
        this(conns, 10000);
    }

    /**
     * @return
     */
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    /**
     * @param conns          zookeeper连接字符串,如172.16.3.42:2181,172.16.3.65:2181,172.16.3.24:2181
     * @param sessionTimeout
     * @throws IOException
     */
    public ZKUtil(String conns, int sessionTimeout) throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(conns, sessionTimeout, this);
        connCD.await();
        logger.info("zookeeper util initialized!");
    }

    /**
     * 创建一个锁节点
     *
     * @param lockParentPath 锁节点所在父目录路径
     * @param lockNode       锁节点名称
     * @param data           锁节点数据
     * @return <IsSuccess,Info> IsSuccess=true,Info=[锁节点路径,数据]列表
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Tuple2<Boolean, List<Tuple2<String, String>>> createLockedPath(String lockParentPath, String lockNode, String data) throws KeeperException, InterruptedException {
        String nodePath = Utils.joinPath(lockParentPath, lockNode);
        byte[] nodeData = data == null ? null : data.getBytes();
        String refPath = zooKeeper.create(nodePath, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info("lock path created:" + refPath);
        List<Tuple2<String, String>> infos = new ArrayList<Tuple2<String, String>>();
        boolean isMin = checkIsMinPath(lockParentPath, refPath, infos);
        Tuple2<Boolean, List<Tuple2<String, String>>> ret = new Tuple2<Boolean, List<Tuple2<String, String>>>(isMin, infos);
        return ret;
    }

    /**
     * 检查lockNodePath是不是lockParentPath下的最小子节点
     *
     * @return
     */
    public boolean checkIsMinPath(String lockParentPath, String lockNodePath, List<Tuple2<String, String>> outInfos) throws KeeperException, InterruptedException {
        List<String> subNodes = zooKeeper.getChildren(lockParentPath, false);
        Collections.sort(subNodes);
        int index = subNodes.indexOf(lockNodePath.substring(lockParentPath.length() + 1));
        switch (index) {
            case 0: {
                String waitPath = Utils.joinPath(lockParentPath, subNodes.get(0));
                byte[] bs = zooKeeper.getData(waitPath, true, new Stat());
                outInfos.add(new Tuple2<String, String>(waitPath, new String(bs)));
                return true;
            }
            default: {
                String waitPath = Utils.joinPath(lockParentPath, subNodes.get(index - 1));
                try {
                    byte[] bs = zooKeeper.getData(waitPath, true, new Stat());
                    if (bs != null) {
                        outInfos.add(new Tuple2<String, String>(waitPath, new String(bs)));
                    }
                    return false;
                } catch (KeeperException e) {
                    if (zooKeeper.exists(waitPath, false) == null) {
                        return checkIsMinPath(lockParentPath, lockNodePath, outInfos);
                    } else {
                        throw e;
                    }
                }

            }
        }
    }

    /**
     * 获取zookeeper中节点数据
     *
     * @param path
     * @return 不存在则返回null
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] getNodeData(String path) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) == null) {
            return null;
        }
        return zooKeeper.getData(path, false, new Stat());
    }

    /**
     * 向zookeeper节点写入数据
     *
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void setNodeData(String path, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data, -1);
    }

    /**
     * 如果path指定的节点不存在则创建
     *
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void createNodeIfNotExists(String path) throws KeeperException, InterruptedException {
        int pos = 0;
        if (!path.endsWith("/")) path = path + "/";
        while ((pos = path.indexOf('/', pos + 1)) > 1) {
            String prefixPath = path.substring(0, pos);
            Stat stat = zooKeeper.exists(prefixPath, false);
            if (stat == null) {
                try {
                    zooKeeper.create(prefixPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    //忽略NodeExistsException
                    logger.warn("create path failure, may be other has executed creation.", prefixPath);
                } catch (KeeperException e) {
                    throw e;
                }
                logger.info("create path:{}", prefixPath);
            }
        }
        ;
    }

    public void createNodeIfNotExists(String path, byte[] data) throws KeeperException, InterruptedException {
        int pos = 0;
        if (!path.endsWith("/")) path = path + "/";
        while ((pos = path.indexOf('/', pos + 1)) > 1) {
            String prefixPath = path.substring(0, pos);
            Stat stat = zooKeeper.exists(prefixPath, false);
            if (stat == null) {
                try {
                    if(prefixPath.equals(path)) {
                        zooKeeper.create(prefixPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }else{
                        zooKeeper.create(prefixPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException.NodeExistsException e) {
                    //忽略NodeExistsException
                    logger.warn("create path failure, may be other has executed creation.", prefixPath);
                } catch (KeeperException e) {
                    throw e;
                }
                logger.info("create path:{}", prefixPath);
            }
        }
        ;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event == null) return;
        Event.EventType eventType = event.getType();
        Event.KeeperState state = event.getState();
        if (Event.KeeperState.SyncConnected == state) {
            if (Event.EventType.None == eventType) {
                connCD.countDown();
                logger.info("zookeeper connected!");
                return;
            }
            String path = event.getPath();
            logger.info("node event: path={},event={}", path, event.getType().name());

        } else {
            logger.info("connect event:" + eventType.name());
        }
    }


}
