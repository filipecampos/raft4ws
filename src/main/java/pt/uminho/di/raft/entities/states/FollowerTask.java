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

public class FollowerTask extends ServerStateTask {

    static Logger logger = Logger.getLogger(FollowerTask.class);

    public FollowerTask(Server srv)
    {
        super(srv);
    }

    @Override
    public void timeout()
    {
        // •	If election timeout elapses without receiving AppendEntries RPC
        // from current leader or granting vote to candidate: convert to candidate
          getServer().getTimeoutTask().setNextState(pt.uminho.di.raft.entities.states.State.Candidate);
          getServer().getTimeoutTask().notifyTask();
    }

    @Override
    public void heartbeat()
    {
        logger.debug(getServer().getIdString() + "Follower processing heartbeat...");
        getServer().getTimeoutTask().heartbeat();
    }

    @Override
    public void execute()
    {
        // do nothing
    }
}
