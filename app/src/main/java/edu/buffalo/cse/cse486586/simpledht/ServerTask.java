package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by tushar on 4/22/16.
 */

public class ServerTask extends AsyncTask<ServerSocket, String, Void> implements Constants{
    static final String TAG = ServerTask.class.getSimpleName();
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        ObjectInputStream inputStream;
        ObjectOutputStream outputStream;
        Socket socket;
        Integer deleteCount = -1;

        Message message = new Message();
        while(true) {
            try {
                socket = serverSocket.accept();
                //Initialize input and output stream for full duplex sockect connection
                inputStream = new ObjectInputStream(socket.getInputStream());
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                message = (Message) inputStream.readObject();
                switch (message.getMessageType()) {
                    case NodeJoin:
                        message.setNodeDetails(nodeJoin(message));
                        nodeJoinNotification();
                        break;
                    case NodeJoinNotification:
                        updateMyNodeDetails(message);
                        break;
                    case Insert:
                        insertValues(message);
                        break;
                    case SingleQuery:
                        message = singleQuery(message);
                        outputStream.writeObject(message);
                        outputStream.flush();
                        break;
                    case GlobalDump:
                        List<Message> messageList = getLocalDump();
                        outputStream.writeObject(messageList);
                        outputStream.flush();
                        break;
                    case SingleDelete:
                        deleteCount = singleDelete(message);
                        outputStream.writeObject(deleteCount);
                        outputStream.flush();
                        break;
                    case GlobalDelete:
                        deleteCount = executeLocalDelete();
                        outputStream.writeObject(deleteCount);
                        outputStream.flush();
                        break;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private NodeDetails nodeJoin(Message message) {
        Log.e(TAG, "Node Join started");

        NodeDetails newNode = message.getNodeDetails();
        //checking if new node is the master node.
        if(newNode.getPort().equalsIgnoreCase(SimpleDhtProvider.myNodeDetails.getPort())){
            Log.e(TAG, "New Node is 11108" );
            return SimpleDhtProvider.myNodeDetails;
        }
        String newPort = newNode.getPort();
        String newNodeIdHash = newNode.getNodeIdHash();
        NodeDetails prevNode = new NodeDetails();
        NodeDetails nextNode = new NodeDetails();
        Log.e(TAG, "New Node is : " + newPort);

        for (int i = 0; i <= SimpleDhtProvider.chordNodeList.size(); i++) {
            if(i == 0){
                prevNode = SimpleDhtProvider.chordNodeList.get(SimpleDhtProvider.chordNodeList.size()-1);
            }else{
                prevNode = SimpleDhtProvider.chordNodeList.get(i-1);
            }

            if (i == SimpleDhtProvider.chordNodeList.size()) {
                nextNode = SimpleDhtProvider.chordNodeList.get(0);
                break;
            }else {
                nextNode = SimpleDhtProvider.chordNodeList.get(i);
            }

            if (newNode.getNodeIdHash().compareTo(nextNode.getNodeIdHash()) < 0) {
                break;
            }
        }
        newNode.setSuccessorPort(prevNode.getSuccessorPort());
        newNode.setPredecessorPort(nextNode.getPredecessorPort());
        newNode.setSuccessorNodeIdHash(prevNode.getSuccessorNodeIdHash());
        newNode.setPredecessorNodeIdHash(nextNode.getPredecessorNodeIdHash());
        prevNode.setSuccessorPort(newPort);
        prevNode.setSuccessorNodeIdHash(newNodeIdHash);
        nextNode.setPredecessorPort(newPort);
        nextNode.setPredecessorNodeIdHash(newNodeIdHash);
        SimpleDhtProvider.chordNodeList.add(newNode);
        Collections.sort(SimpleDhtProvider.chordNodeList, new NodeComparator());
        for (NodeDetails node:SimpleDhtProvider.chordNodeList) {
            node.setFirstNode(false);
        }
        SimpleDhtProvider.chordNodeList.get(0).setFirstNode(true);
        Log.e(TAG, "Node list : " + SimpleDhtProvider.chordNodeList.toString());
        return newNode;
    }

    private void nodeJoinNotification(){
        Message message;
        ObjectOutputStream outputStream;
        Socket socket;
        for (NodeDetails node: SimpleDhtProvider.chordNodeList) {
            try {
                message = new Message();
                message.setMessageType(MessageType.NodeJoinNotification);
                message.setNodeDetails(node);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getPort()));
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                outputStream.writeObject(message);
                outputStream.flush();
            }catch(Exception e){
                Log.e(TAG,"********************** Exception in nodeJoinNotification");
                e.printStackTrace();
            }
        }
    }

    private void updateMyNodeDetails(Message message){
        SimpleDhtProvider.myNodeDetails = message.getNodeDetails();
        Log.e(TAG,"SimpleDhtProvider.myNodeDetails : " + SimpleDhtProvider.myNodeDetails.toString());
    }

    private void insertValues(Message message){
        ContentResolver contentResolver = SimpleDhtProvider.context.getContentResolver();
        ContentValues contentValues = new ContentValues();
        contentValues.put(KEY_FIELD, message.getKey());
        contentValues.put(VALUE_FIELD, message.getValue());
        contentResolver.insert(mUri, contentValues);
    }

    private Message singleQuery(Message message){
        ContentResolver contentResolver = SimpleDhtProvider.context.getContentResolver();
        Cursor cursor = contentResolver.query(mUri, null, message.getKey(), null, null);
        cursor.moveToFirst();
        message.setKey(cursor.getString(cursor.getColumnIndex(KEY_FIELD)));
        message.setValue(cursor.getString(cursor.getColumnIndex(VALUE_FIELD)));
        Log.e(TAG, "****************************************");
        Log.e(TAG, "key from cursor : " + cursor.getString(cursor.getColumnIndex(KEY_FIELD)) + " value from cursor : " + cursor.getString(cursor.getColumnIndex(VALUE_FIELD)));
        Log.e(TAG, "key from message : " + message.getKey() + " value from message : " + message.getValue());
        Log.e(TAG, "****************************************");
        return message;
    }

    private List<Message> getLocalDump(){
        ContentResolver contentResolver = SimpleDhtProvider.context.getContentResolver();
        Cursor cursor = contentResolver.query(mUri, null, "@", null, null);
        List<Message> messageList = new ArrayList<Message>();
        Message msg;
        Log.e(TAG, "Values retrieved and stored in cursor inside ServerTask() : " + DatabaseUtils.dumpCursorToString(cursor));
        for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
            msg = new Message();
            msg.setKey(cursor.getString(cursor.getColumnIndex(KEY_FIELD)));
            msg.setValue(cursor.getString(cursor.getColumnIndex(VALUE_FIELD)));
            messageList.add(msg);
        }
        Log.e(TAG, "****************************************");
        Log.e(TAG, "messageList : " + messageList.toString());
        Log.e(TAG, "****************************************");

        return messageList;
    }

    private Integer singleDelete(Message message){
        int deleteCount = -1;
        ContentResolver contentResolver = SimpleDhtProvider.context.getContentResolver();
        deleteCount = contentResolver.delete(mUri, message.getKey(), null);

        Log.e(TAG, "****************************************");

        Log.e(TAG, "DeleteCount in Server : " + deleteCount);
        Log.e(TAG, "****************************************");
        return deleteCount;
    }

    private Integer executeLocalDelete(){
        Integer deleteCount = -1;
        ContentResolver contentResolver = SimpleDhtProvider.context.getContentResolver();
        deleteCount = contentResolver.delete(mUri,  "@",  null);

        Log.e(TAG, "****************************************");
        Log.e(TAG, "deleteCount : " + deleteCount);
        Log.e(TAG, "****************************************");

        return deleteCount;
    }

}
