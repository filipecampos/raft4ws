/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.uminho.di.raft.service.log;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import java.io.File;

/**
 *
 * @author fcampos
 */
public class BDBStateMachine implements StateMachine
{

    static Logger logger = Logger.getLogger(BDBStateMachine.class);
    Environment myDbEnvironment = null;
    Database myDatabase = null;
    Database myClassDb = null;
    EntryBinding dataBinding = null;
    private String idString;

    public BDBStateMachine()
    {

    }

    @Override
    public void initialize(String id)
    {
        idString = "[Server " + id + "] - ";
        // Open the environment. Create it if it does not already exist.
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        String filename = "export/dbEnv" + id;
        File dbFile = new File(filename);
        if(!dbFile.exists())
        {
            dbFile.mkdirs();
            logger.info(idString + "Created dirs for " + filename);
        }

        myDbEnvironment = new Environment(dbFile, envConfig);

        // Open the database. Create it if it does not already exist.
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        // Make it a temporary database
        dbConfig.setTemporary(true);
        myDatabase = myDbEnvironment.openDatabase(null,
            "sampleDatabase",
            dbConfig);

        // Open the database that you use to store your class information.
        // The db used to store class information does not require duplicates
        // support.
        dbConfig.setSortedDuplicates(false);
        myClassDb = myDbEnvironment.openDatabase(null, "classDb",
            dbConfig);

        // Instantiate the class catalog
        StoredClassCatalog classCatalog = new StoredClassCatalog(myClassDb);

        // Create the binding
        dataBinding = new SerialBinding(classCatalog, LogEntry.class);

        logger.info(idString + "StateMachine " + id + " initialized!");
    }

    @Override
    public void initializeDB()
    {
        // no need to re-create database
    }

    @Override
    public void shutdown()
    {
        // then close the database and environment here
        if(myDatabase != null)
        {
            myDatabase.close();
            myClassDb.close();
        }

        if(myDbEnvironment != null)
        {
            myDbEnvironment.close();
        }
        logger.info(idString + "Stopped BerkeleyDB!");
    }

    @Override
    public boolean insertIntoDB(LogEntry entry)
    {
        boolean success = true;

        // Create the DatabaseEntry for the data. Use the EntryBinding object
        // that was just created to populate the DatabaseEntry
        DatabaseEntry theData = new DatabaseEntry();
        dataBinding.objectToEntry(entry, theData);

        // Create the DatabaseEntry for the key
        DatabaseEntry theKey = null;
        try
        {
            theKey = new DatabaseEntry(entry.getUid().getBytes("UTF-8"));

            // Put it as normal
            OperationStatus status = myDatabase.put(null, theKey, theData);
            logger.debug(idString + "Put status = " + status);

            success = status.equals(OperationStatus.SUCCESS);
        } catch(UnsupportedEncodingException ex)
        {
            logger.error(ex.getMessage(), ex);
        }

        return success;
    }

    public void test()
    {
        try
        {
            // The data data
            LogEntry data2Store = new LogEntry("blabla", 9, 7, "x=4", "vars");
            logger.debug(idString + "Inserting Data2Store: " + data2Store);
            insertIntoDB(data2Store);


            data2Store = new LogEntry("nhacamacaca", 10, 8, "q=2", "more");
            logger.debug(idString + "Inserting Data2Store: " + data2Store);
            insertIntoDB(data2Store);

            // Create DatabaseEntry objects for the key and data
            DatabaseEntry theRetrievedData = new DatabaseEntry();

            // Do the get as normal
//            myDatabase.get(null, theKey, theRetrievedData, LockMode.DEFAULT);
            myDatabase.get(null, new DatabaseEntry("blabla".getBytes("UTF-8")), theRetrievedData, LockMode.DEFAULT);
            LogEntry retrievedData = (LogEntry) dataBinding.entryToObject(theRetrievedData);
            logger.debug(idString + "Retrieved Data: " + retrievedData);

            theRetrievedData = new DatabaseEntry();
            myDatabase.get(null, new DatabaseEntry("nhacamacaca".getBytes("UTF-8")), theRetrievedData, LockMode.DEFAULT);
            // Recreate the MyData object from the retrieved DatabaseEntry using
            // the EntryBinding created above
            retrievedData = (LogEntry) dataBinding.entryToObject(theRetrievedData);
            logger.debug(idString + "Retrieved Data: " + retrievedData);

        } catch(DatabaseException dbe)
        {
            // Exception handling goes here
            java.util.logging.Logger.getLogger(BDBStateMachine.class.getName()).log(Level.SEVERE, null, dbe);
        } catch(UnsupportedEncodingException ex)
        {
            java.util.logging.Logger.getLogger(BDBStateMachine.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void main(String[] args)
    {
        String id = "";

        if(args.length > 0)
            id = args[0];

        BDBStateMachine sm = new BDBStateMachine();
        sm.initialize(id);
//        sm.initializeDB();
        sm.test();

        sm.shutdown();
    }
}
