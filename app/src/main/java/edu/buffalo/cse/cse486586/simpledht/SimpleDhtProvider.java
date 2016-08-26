package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider implements Constants {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    //private static final  Semaphore querySemaphore = new Semaphore(1);

    public static Cursor globalCursor = null;
    public static int globalDeleteCount = -1;
    static NodeDetails myNodeDetails;
    static List<NodeDetails> chordNodeList;
    //boolean isStandAloneMode = false;
    public static Context context = null;


    //nodeId will contain value like 5554
    private String getMyNodeId() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String nodeId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return nodeId;
    }

    //initialize node Details for this avd
    private void init() {
        myNodeDetails = new NodeDetails();
        try {
            //port will store value of port used for connection
            //eg: port = 5554*2 = 11108
            String port = String.valueOf(Integer.parseInt(getMyNodeId()) * 2);
            //nodeIdHash will store hash of nodeId =>
            // eg: nodeIdHash = hashgen(5554)
            String nodeIdHash = genHash(getMyNodeId());
            myNodeDetails.setPort(port);
            myNodeDetails.setNodeIdHash(nodeIdHash);
            myNodeDetails.setPredecessorPort(port);
            myNodeDetails.setSuccessorPort(port);
            myNodeDetails.setSuccessorNodeIdHash(nodeIdHash);
            myNodeDetails.setPredecessorNodeIdHash(nodeIdHash);
            myNodeDetails.setFirstNode(true);
            context = getContext();

            //initialize arraylist with mynode if node is master node
            if (getMyNodeId().equalsIgnoreCase(masterPort)) {
                chordNodeList = new ArrayList<NodeDetails>();
                chordNodeList.add(myNodeDetails);
            }

        } catch (Exception e) {
            Log.e(TAG,"**************************Exception in init()**********************");
            e.printStackTrace();
        }
    }

    @Override
    public boolean onCreate() {
        init();

        //create server task to accept requests from other nodes
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 25);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
            return false;
        }

        //when node joins request node 5554 (port 11108) to get node details

        Message message = new Message();
        message.setMessageType(MessageType.NodeJoin);
        message.setNodeDetails(myNodeDetails);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

        return true;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */

            String key = values.getAsString(KEY_FIELD);
            String value = values.getAsString(VALUE_FIELD);
            String keyHash="";
            try{
                 keyHash = genHash(key);
            }catch (Exception e){
                Log.e(TAG,"***********************Exception in Insert************************");
                e.printStackTrace();
            }

            if(lookup(keyHash)){
                context = getContext();
                SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
                SharedPreferences.Editor editor = sharedPref.edit();
                editor.putString(key, value);
                editor.commit();
                Log.e(TAG, "Values inserted inside content provider : " + "key : " + key + "value : " + value);
            }else{
                Message message = new Message();
                message.setMessageType(MessageType.Insert);
                message.setNodeDetails(myNodeDetails);
                message.setKey(key);
                message.setValue(value);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            }

            return uri;

    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        context = getContext();
        String[] cursorColumns = new String[]{"key", "value"};
        Cursor returnCursor = null;
        MatrixCursor cursor = new MatrixCursor(cursorColumns);
        String key = selection;
        String keyHash = "";
        String value = "";
        try{
            keyHash = genHash(key);
        }catch (Exception e){
            Log.e(TAG,"***********************Exception in Query...genHash()************************");
            e.printStackTrace();
        }

        if (key.equalsIgnoreCase("*")) {
            returnCursor = getGlobalDump();
        } else if (key.equalsIgnoreCase("@")) {
            returnCursor = getLocalDump();
        } else {
            if(lookup(keyHash)){
                context = getContext();
                SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
                value = sharedPref.getString(key, "DEFAULT");
                String[] row = new String[]{key, value};
                cursor.addRow(row);
                returnCursor = cursor;
                Log.e(TAG, "Values retrieved : " + "key : " + key + "value : " + value);
            }else{
                Message message = new Message();
                message.setMessageType(MessageType.SingleQuery);
                message.setNodeDetails(myNodeDetails);
                message.setKey(key);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                /*try {
                    querySemaphore.acquire();
                }catch(Exception e){
                    Log.e(TAG,"***********************Exception in QuerySemaphore************************");
                    e.printStackTrace();
                }*/
                Log.e(TAG, "##################################################");
                Log.e(TAG, "Busy Waiting started...");
                while(globalCursor == null){
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Log.e(TAG,"***********************Exception in Query...sleep()************************");
                        e.printStackTrace();
                    }
                }
                returnCursor = globalCursor;
                Log.e(TAG, "Values retrieved and stored in global cursor inside query() before assigning null: " + DatabaseUtils.dumpCursorToString(globalCursor));
                globalCursor = null;
                Log.e(TAG,"Busy Waiting finished...");
                Log.e(TAG, "Values retrieved and stored in returnCursor inside query(): " + DatabaseUtils.dumpCursorToString(returnCursor));
                Log.e(TAG, "Values retrieved and stored in global cursor inside query() after assigning null: " + DatabaseUtils.dumpCursorToString(globalCursor));
                Log.e(TAG, "##################################################");
            }
        }
        Log.e(TAG, "Values retrieved and stored in cursor : " + DatabaseUtils.dumpCursorToString(returnCursor));
        return returnCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        context = getContext();

        int deleteCount = 0;

        String key = selection;
        String keyHash = "";
        try{
            keyHash = genHash(key);
        }catch (Exception e){
            Log.e(TAG,"***********************Exception in Query...genHash()************************");
            e.printStackTrace();
        }

        if (key.equalsIgnoreCase("*")) {
            deleteCount = deleteGlobalDump();
        } else if (key.equalsIgnoreCase("@")) {
            deleteCount = deleteLocalDump();
        } else {
            if(lookup(keyHash)){
                context = getContext();
                SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
                SharedPreferences.Editor editor = sharedPref.edit();
                editor.remove(key);
                editor.apply();
                deleteCount = 1;
                Log.e(TAG, "Deleted keys-value paris : " + deleteCount + " Deleted key : " + key );
            }else{
                Message message = new Message();
                message.setMessageType(MessageType.SingleDelete);
                message.setNodeDetails(myNodeDetails);
                message.setKey(key);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                Log.e(TAG, "##################################################");
                Log.e(TAG, "Busy Waiting started...");
                while(globalDeleteCount < 0){
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Log.e(TAG,"***********************Exception in Query...sleep()************************");
                        e.printStackTrace();
                    }
                }
                Log.e(TAG,"Busy Waiting finished...");
                deleteCount = globalDeleteCount;
                Log.e(TAG, "globalDeleteCount before assigning 0: " + globalDeleteCount);
                globalDeleteCount = -1;
                Log.e(TAG, "DeleteCount : " + deleteCount);
                Log.e(TAG, "globalDeleteCount before assigning 0: " + globalDeleteCount);
                Log.e(TAG, "##################################################");
            }
        }
        Log.e(TAG, "DeleteCount : " + deleteCount);
        return deleteCount;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    private Cursor getGlobalDump() {
        Log.e(TAG, "Querying Global dump");
        Cursor returnCursor = null;
        Message message = new Message();
        message.setMessageType(MessageType.GlobalDump);
        message.setNodeDetails(myNodeDetails);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
        Log.e(TAG, "##################################################");
        Log.e(TAG, "Busy Waiting started...");

        while(globalCursor == null){
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Log.e(TAG,"***********************Exception in getGlobalDump...sleep()************************");
                e.printStackTrace();
            }
        }
        returnCursor = globalCursor;
        Log.e(TAG, "Values retrieved and stored in global cursor inside query() before assigning null: " + DatabaseUtils.dumpCursorToString(globalCursor));
        globalCursor = null;
        Log.e(TAG,"Busy Waiting finished...");
        Log.e(TAG, "Values retrieved and stored in returnCursor inside query(): " + DatabaseUtils.dumpCursorToString(returnCursor));
        Log.e(TAG, "Values retrieved and stored in global cursor inside query() after assigning null: " + DatabaseUtils.dumpCursorToString(globalCursor));
        Log.e(TAG, "##################################################");

        return returnCursor;
    }

    private MatrixCursor getLocalDump() {
        String[] cursorColumns = new String[]{"key", "value"};
        MatrixCursor cursor = new MatrixCursor(cursorColumns);
        Log.e(TAG, "Querying Local dump");
        Log.e(TAG, "Value dump from avd : " + getMyNodeId());
            SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
            Map<String, ?> keys = sharedPref.getAll();
            for (Map.Entry<String, ?> entry : keys.entrySet()) {
                Log.e(TAG, entry.getKey() + ": " + entry.getValue().toString());
                String[] row = new String[]{entry.getKey(), entry.getValue().toString()};
                cursor.addRow(row);
            }
        return cursor;
    }


    private int deleteGlobalDump() {
        Log.e(TAG, "Deleting Global dump");
        int deleteCount = -1;
        Message message = new Message();
        message.setMessageType(MessageType.GlobalDelete);
        message.setNodeDetails(myNodeDetails);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
        Log.e(TAG, "##################################################");
        Log.e(TAG, "Busy Waiting started...");
        while(globalDeleteCount < 0){
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Log.e(TAG,"***********************Exception in getGlobalDump...sleep()************************");
                e.printStackTrace();
            }
        }
        Log.e(TAG,"Busy Waiting finished...");
        deleteCount = globalDeleteCount;
        Log.e(TAG, "globalDeleteCount before assigning -1: " + globalDeleteCount);
        globalDeleteCount = -1;

        Log.e(TAG, "deleteCount inside deleteGlobalDump(): " + deleteCount);
        Log.e(TAG, "globalDeleteCount after assigning -1: " + globalDeleteCount);
        Log.e(TAG, "##################################################");

        return deleteCount;
    }


    private int deleteLocalDump() {
        int deleteCount = -1;
        SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();
        deleteCount = sharedPref.getAll().size();
        editor.clear();
        editor.commit();
        Log.e(TAG, "Values Deleted : " + deleteCount);
        return deleteCount;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean lookup(String keyHash){
        boolean retVal = false;
        if(myNodeDetails.isFirstNode()){
            if (keyHash.compareTo(myNodeDetails.getPredecessorNodeIdHash()) > 0 ||
                    keyHash.compareTo(myNodeDetails.getNodeIdHash()) <= 0) {
                retVal = true;
            } else
                retVal = false;

        }else {
            if (keyHash.compareTo(myNodeDetails.getPredecessorNodeIdHash()) > 0 &&
                    keyHash.compareTo(myNodeDetails.getNodeIdHash()) <= 0) {
                retVal = true;
            } else
                retVal = false;
        }
        return retVal;
    }



}
