package server.ratis;

import common.Config;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
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

    public static void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    public String insertDocument(String document) {

        if (isLeaderAndReady()) {
            RaftClientRequest req = createWriteRequest(document);
            RaftClientReply reply;

            try {
                reply = raftServer.submitClientRequestAsync(req).get();
            } catch (Exception e) {
                e.printStackTrace();
                return e.toString();
            }

            if(reply == null) {
                print("Got empty reply for req:" + req);
                return "empty reply from raft server";
            }

            return handleReply(reply);
        }

        return "current raft server is not leader or not ready yet";
    }

    public int getSingleWordFrequency(String word) {
        return metadata.getSingleWordFrequency(word);
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

    private RaftClientRequest createWriteRequest(String document) {
        return RaftClientRequest.newBuilder()
                .setMessage(Message.valueOf(document))
                .setType(RaftClientRequest.writeRequestType())
                .setGroupId(raftGroup.getGroupId())
                .setServerId(raftServer.getId())
                .setClientId(ClientId.randomId())
                .build();
    }

    public String handleReply(RaftClientReply reply) {
        if(reply.isSuccess()) {
            return "success";
        }

        NotLeaderException notLeaderException = reply.getNotLeaderException();

        if(notLeaderException != null) {
            return notLeaderException.toString();
        }

        LeaderNotReadyException leaderNotReadyException = reply.getLeaderNotReadyException();

        if(leaderNotReadyException != null) {
            return leaderNotReadyException.toString();
        }

        StateMachineException stateMachineException = reply.getStateMachineException();

        if(stateMachineException != null) {
            return stateMachineException.toString();
        }

        RaftException raftException =  reply.getException();

        if(raftException != null) {
            return raftException.toString();
        }

        return reply.toString();
    }
}
