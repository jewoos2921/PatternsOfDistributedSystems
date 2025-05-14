public class Server {
    public void startup() {
        zookeeperClient.subscribeLeaderChangeListener(this);
        elect();
    }

    public void elect() {
        var leaderId = serverId;
        try {
            zookeeperClient.tryCreatingLeaderPath(leaderId);
            this.currentLeader = serverId;
            onBecomingLeader();
        } catch (ZkNodeExistsException e) {
            // 리더 후보에서 물러난다.
            this.currentLeader = zookeeperClient.getLeaderId();
        }
    }

    @Override
    public void handleDataDeleted(String dataPath) {
        elect();
    }
}

class ZookeeperClient {
    public void subscribeLeaderChangeListener(IZkDataListener listener) {
        zkClient.subscribeDataChange(LeaderPath, listener);
    }
}
