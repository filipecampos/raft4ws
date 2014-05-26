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

package pt.uminho.di.raft.entities.workers;

import org.apache.log4j.Logger;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.entities.ServerClient;

public class SearchingTask extends ServerWorkingTask {

    static Logger logger = Logger.getLogger(SearchingTask.class);

    private ServerClient serverClient;

    public SearchingTask(ServerClient cli, Server srv)
    {
        super(srv);
        serverClient = cli;
    }

    @Override
    public void run()
    {
        serverClient.searchRaftDevices();
    }
}
