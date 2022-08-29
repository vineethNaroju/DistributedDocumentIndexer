package server.ratis;

import common.Config;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class DIRaftServer implements Closeable {

    private final String name;

    public final RaftServer raftServer;

    public final ClientId clientId;

    public final RaftGroup raftGroup;

    private static final DIMetadata metadata = new DIMetadata();

    private final DIStateMachine stateMachine = new DIStateMachine(metadata);

    public DIRaftServer(int id) throws Exception {
        name = "DIRaftServer-" + id;

        clientId = ClientId.valueOf(UUID.nameUUIDFromBytes(name.getBytes()));

        RaftProperties raftProperties = new RaftProperties();

        List<RaftPeer> raftPeersList = new ArrayList<>();

        for(String peerIp : Config.clusterIps) {
            raftPeersList.add(RaftPeer.newBuilder().setId(peerIp)
                    .setAddress(peerIp).build());
        }

        raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(Config.groupUUID), raftPeersList);

        RaftPeer currentRaftPeer = raftPeersList.get(id);

        final int serverPort = NetUtils.createSocketAddr(currentRaftPeer.getAddress()).getPort();

        File storageDir = new File("port" + serverPort);

        RaftServerConfigKeys.setStorageDir(raftProperties, Collections.singletonList(storageDir));

        GrpcConfigKeys.Server.setPort(raftProperties, serverPort);

        raftServer = RaftServer.newBuilder()
                .setGroup(raftGroup)
                .setProperties(raftProperties)
                .setServerId(currentRaftPeer.getId())
                .setStateMachine(stateMachine)
                .build();

        raftServer.start();
    }

    @Override
    public void close() throws IOException {
        raftServer.close();
    }

    public String checkStatus() {
        return raftServer.getLifeCycleState().toString();
    }

    //TODO
    public void insertDocument(String document) throws IOException {

        if (isLeaderAndReady()) {

        }
    }

    //TODO
    public int getFrequency(String word) {
        return -1;
    }

    public boolean isLeaderAndReady() {
        try {
            RaftServer.Division division = raftServer.getDivision(raftGroup.getGroupId());
            if(division != null && division.getInfo().isLeaderReady()) return true;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return false;
    }
}
