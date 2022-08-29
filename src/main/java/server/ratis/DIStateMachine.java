package server.ratis;

import org.apache.ratis.statemachine.impl.BaseStateMachine;

public class DIStateMachine extends BaseStateMachine {

    private final DIMetadata metadata;

    DIStateMachine(DIMetadata md) {
        metadata = md;
    }
}
