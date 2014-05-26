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

package pt.uminho.di.raft.service.log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

public class HSQLStateMachine implements StateMachine {

    static Logger logger = Logger.getLogger(HSQLStateMachine.class);

    private org.hsqldb.server.Server hsqlServer = null;
    private Connection connection = null;
    private String idString;

    public HSQLStateMachine()
    {
        hsqlServer = new org.hsqldb.server.Server();
    }

    @Override
    public void initialize(String id)
    {
        idString = "[Server " + id + "] - ";
		hsqlServer.setLogWriter(null);
		hsqlServer.setSilent(true);
		hsqlServer.setDatabaseName(0, "smdb" + id);
		hsqlServer.setDatabasePath(0, "file:smdb" + id);

		hsqlServer.start();

		// making a connection
		try
        {
//			Class.forName("org.hsqldb.jdbcDriver");
			connection = DriverManager.getConnection("jdbc:hsqldb:file:smdb" + id, "sa", ""); // can through sql exception
        } catch (SQLException e2) {
			e2.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
		}

        logger.debug(idString + "StateMachine initialized!");
    }

    @Override
    public void initializeDB()
    {

        try
        {
            connection.prepareStatement("drop table commands if exists;").execute();
            connection.prepareStatement("create table commands (id integer, uid varchar(40) not null, command varchar(20) not null, parameters varchar(20));").execute();
        } catch (SQLException e2) {
            e2.printStackTrace();
        }
        logger.debug(idString + "StateMachine created table commands!");
    }

    public void insertIntoDB()
    {
        try
        {
//			connection.prepareStatement("insert into commands (id, command) values (1, 'x=5');").execute();
//            connection.prepareStatement("insert into commands (id, command) values (2, 'y=8');").execute();

			// query from the db
			ResultSet rs = connection.prepareStatement("select id, command from commands;").executeQuery();
			rs.next();
			logger.debug(String.format(idString + "ID: %1d, Command: %1s", rs.getInt(1), rs.getString(2)));
            rs.next();
			logger.debug(String.format(idString + "ID: %1d, Command: %1s", rs.getInt(1), rs.getString(2)));

		} catch (SQLException e2) {
			e2.printStackTrace();
		}
    }

    @Override
    public void shutdown()
    {
        hsqlServer.stop();
        hsqlServer = null;
        logger.debug(idString + "Stopped HSQL Server!");
    }

    public static void main(String[] args) {
        String id = "";

        if(args.length > 0)
            id = args[0];

        HSQLStateMachine sm = new HSQLStateMachine();
        sm.initialize(id);
        sm.initializeDB();

        sm.shutdown();
    }

    @Override
    public boolean insertIntoDB(LogEntry entry)
    {
        boolean success = true;
        try
        {
            logger.debug(idString + "StateMachine inserting entry with UID: " + entry.getUid() + "; Index: " + entry.getIndex() + ";  Command: " + entry.getCommand() + "; Parameters: " + entry.getParameters());
            connection.prepareStatement("insert into commands (id, uid, command, parameters) values (" + entry.getIndex() + ", '" + entry.getUid() + "', '" + entry.getCommand() + "', '" + entry.getParameters() + "');").execute();
            logger.debug(idString + "StateMachine inserted entry with UID: " + entry.getUid() + "; Index: " + entry.getIndex() + ";  Command: " + entry.getCommand() + "; Parameters: " + entry.getParameters());

            if(logger.isDebugEnabled())
            {
                // query from the db
                ResultSet rs = connection.prepareStatement("select id, command from commands where id=" + entry.getIndex() + ";").executeQuery();
                rs.next();
                logger.debug(idString + String.format("ID: %1d, Command: %1s", rs.getInt(1), rs.getString(2)));
            }

        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            success = false;
        }

        return success;
    }
}
