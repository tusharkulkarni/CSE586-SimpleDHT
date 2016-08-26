package edu.buffalo.cse.cse486586.simpledht;

import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.List;

/**
 * Created by tushar on 4/22/16.
 */


public class ClientTask extends AsyncTask<Message, Void, Void> implements Constants {
    static final String TAG = ClientTask.class.getSimpleName();
    @Override
    protected Void doInBackground(Message... msg) {

        Message message = msg[0];

        switch (message.getMessageType()){
            case NodeJoin: requestNodeJoin(message);
                break;
            case Insert:passInsertMessage(message);
                break;
            case SingleQuery: singleQuery(message);
                break;
            case GlobalDump: queryGlobalDump(message);
                break;
            case SingleDelete: singleDelete(message);
                break;
            case GlobalDelete: executeGlobalDelete(message);
                break;
        }

        return null;
    }

    private void requestNodeJoin(Message message){
        ObjectOutputStream outputStream;
        Socket socket;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT));
            outputStream = new ObjectOutputStream(socket.getOutputStream());

            socket.setSoTimeout(2000);
            Log.e(TAG, "Join Message to send is : " + message.toString());
            outputStream.writeObject(message);
            outputStream.flush();
        } catch (Exception e) {
            Log.e(TAG, "************************Exception in ClientTask().requestNodeJoin()******************");
            e.printStackTrace();
        }
    }

    private void passInsertMessage(Message message){
        ObjectOutputStream outputStream;
        Socket socket;
        String keyHash = "";
        try {
            keyHash = genHash(message.getKey());
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(SimpleDhtProvider.myNodeDetails.getSuccessorPort()));
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            Log.e(TAG, "Insert Message sent to : " + SimpleDhtProvider.myNodeDetails.getSuccessorPort());
            Log.e(TAG, "Predecessor PortHash : " +SimpleDhtProvider.myNodeDetails.getPredecessorNodeIdHash() +  " KeyHash : " + keyHash+ " MyPortHash : " + SimpleDhtProvider.myNodeDetails.getNodeIdHash() );
            outputStream.writeObject(message);
            outputStream.flush();
        } catch (Exception e) {
            Log.e(TAG, "************************Exception in ClientTask().passInsertMessage()******************");
            e.printStackTrace();
        }
    }

    private void singleQuery(Message message){
        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        Socket socket;
        String keyHash = "";
        String[] cursorColumns = new String[]{"key", "value"};
        MatrixCursor cursor = new MatrixCursor(cursorColumns);
        try {
            keyHash = genHash(message.getKey());
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(SimpleDhtProvider.myNodeDetails.getSuccessorPort()));
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            inputStream = new ObjectInputStream(socket.getInputStream());
            Log.e(TAG, "Predecessor PortHash : " + SimpleDhtProvider.myNodeDetails.getPredecessorNodeIdHash() + " KeyHash : " + keyHash + " MyPortHash : " + SimpleDhtProvider.myNodeDetails.getNodeIdHash());
            outputStream.writeObject(message);
            outputStream.flush();
            message = (Message)inputStream.readObject();
            Log.e(TAG, "****************************************");
            Log.e(TAG, "key from message : " + message.getKey() + " value from message : " + message.getValue());
            String[] row = new String[]{message.getKey(), message.getValue()};
            cursor.addRow(row);
            SimpleDhtProvider.globalCursor = cursor;
            Log.e(TAG, "Values retrieved and stored in local cursor inside ClientTask(): " + DatabaseUtils.dumpCursorToString(cursor));
            Log.e(TAG, "Values retrieved and stored in global cursor inside ClientTask() : " + DatabaseUtils.dumpCursorToString(SimpleDhtProvider.globalCursor));
            Log.e(TAG, "****************************************");
        } catch (Exception e) {
            Log.e(TAG, "************************Exception in ClientTask().singleQuery()******************");
            e.printStackTrace();
        }
        //querySemaphore.release();
    }

    private void singleDelete(Message message)z{
        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        Socket socket;
        Integer deleteCount=0;


        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(SimpleDhtProvider.myNodeDetails.getSuccessorPort()));
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            inputStream = new ObjectInputStream(socket.getInputStream());
            Log.e(TAG, "****************************************");
            Log.e(TAG, "key to be deleted retrieved from message : " + message.getKey());

            outputStream.writeObject(message);
            outputStream.flush();
            deleteCount = (Integer)inputStream.readObject();

            SimpleDhtProvider.globalDeleteCount = deleteCount;
            Log.e(TAG, "DeleteCount inside ClientTask(): " + deleteCount);
            Log.e(TAG, "VGlobal Delete Count inside ClientTask() : " + SimpleDhtProvider.globalDeleteCount);
            Log.e(TAG, "****************************************");
        } catch (Exception e) {
            Log.e(TAG, "************************Exception in ClientTask().singleQuery()******************");
            e.printStackTrace();
        }
    }

    private void queryGlobalDump(Message message){
        String[] cursorColumns = new String[]{"key", "value"};
        MatrixCursor cursor = new MatrixCursor(cursorColumns);
        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        Socket socket;
        List<Message> messageList;
        for (String port: REMOTE_PORTS) {
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                inputStream = new ObjectInputStream(socket.getInputStream());

                outputStream.writeObject(message);
                outputStream.flush();
                messageList = (List<Message>)inputStream.readObject();

                for (Message msg: messageList) {
                    Log.e(TAG, "****************************************");
                    Log.e(TAG, "key from message : " + msg.getKey() + " value from message : " + msg.getValue());
                    String[] row = new String[]{msg.getKey(), msg.getValue()};
                    cursor.addRow(row);

                    Log.e(TAG, "Values retrieved and stored in local cursor inside ClientTask(): " + DatabaseUtils.dumpCursorToString(cursor));
                    Log.e(TAG, "Values retrieved and stored in global cursor inside ClientTask() : " + DatabaseUtils.dumpCursorToString(SimpleDhtProvider.globalCursor));
                    Log.e(TAG, "****************************************");
                }

            } catch (Exception e) {
                Log.e(TAG, "************************Exception in ClientTask().singleQuery()******************");
                e.printStackTrace();
            }
        }
        SimpleDhtProvider.globalCursor = cursor;
    }

    private void executeGlobalDelete(Message message){
        int deleteCount = 0;
        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        Socket socket;

        for (String port: REMOTE_PORTS) {
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                inputStream = new ObjectInputStream(socket.getInputStream());

                outputStream.writeObject(message);
                outputStream.flush();
                deleteCount += (Integer)inputStream.readObject();

            } catch (Exception e) {
                Log.e(TAG, "************************Exception in ClientTask().singleQuery()******************");
                e.printStackTrace();
            }
        }
        SimpleDhtProvider.globalDeleteCount = deleteCount;
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


}

