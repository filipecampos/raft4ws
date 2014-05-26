/*******************************************************************************
 * Copyright (c) 2014 Filipe Campos.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package pt.uminho.di.raft.entities.states;

import org.apache.log4j.Logger;
import pt.uminho.di.raft.entities.Server;

public class LeaderTask extends ServerStateTask {

    static Logger logger = Logger.getLogger(LeaderTask.class);

    public LeaderTask(Server srv) {
        super(srv);

        getServer().initializeLeaderVariables();
        
        // •	Upon election:
                //          send initial empty AppendEntries RPCs (heartbeat) to each server;
                //          repeat during idle periods to prevent election timeouts (§5.2)
        getServer().sendHeartbeatOrEntries(false);

        logger.debug(getServer().getIdString() + " LeaderTask has initialized!");
    }

    @Override
    public void execute()
    {
        getServer().sendHeartbeatOrEntries(true);
    }

    @Override
    public void timeout() {
        logger.debug(getServer().getIdString() + " Leader sending heartbeats or entries...");
        // •	Upon election:
                //          send initial empty AppendEntries RPCs (heartbeat) to each server;
                //          repeat during idle periods to prevent election timeouts (§5.2)
        execute();
    }

    @Override
    public void heartbeat() {
        logger.error(getServer().getIdString() + "Leader received heartbeat from another leader! Stepping down...");
        // turn into follower
        getServer().getTimeoutTask().setNextState(pt.uminho.di.raft.entities.states.State.Follower);
        getServer().getTimeoutTask().heartbeat();
    }
}
