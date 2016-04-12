package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

class Node implements Serializable {

    String msgType;
    String key;
    String value;
    String uriString;
    String requestingPort;
    TreeMap<String, String> map = null;
    HashMap<String, String> cursorMap;
    public void Node() {
        msgType = null;
        key = null;
        value = null;
        map = null;
        uriString = null;
        requestingPort = null;
    }

    public void initCursorMap(){
        this.cursorMap= new HashMap<String, String>();
    }

    public void addPortToMap(String port) {
        try {
            map.put(genHash(port), port);
        } catch (NoSuchAlgorithmException e) {
            Log.e("DeviceMap Class", e.toString());
        }
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getMsgType() {
        return this.msgType;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setUriString(String uriString) {
        this.uriString = uriString;
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

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    public static TreeMap<String, String> DeviceMap = new TreeMap<String, String>();
    public String[] columns = new String[]{"key", "value"};
    public static MatrixCursor matrixCursor;
    public static String myPort;
    public static String portStr;
    public static boolean result_ready = false;

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        new NewDeviceClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, portStr);

        return true;
    }
    private class NewDeviceClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Log.e(TAG, " Requesting AVD0 for Node Join");

            //Creating a node to inform all avds about new incoming node
            try {
                //Informing AVD0 about new AVD
                String port5554 = String.valueOf((Integer.parseInt("5554") * 2));
                Log.e(TAG, "Sending Request to " + port5554);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port5554));
                ObjectOutputStream outn = new ObjectOutputStream(socket.getOutputStream());
                Node node = new Node();
                node.setMsgType("NEW AVD JOIN REQUEST");
                node.requestingPort = msgs[0];

                Log.e(TAG, "Sending request for port " + node.requestingPort);
                //outn.reset();
                outn.writeObject(node);
                Log.e(TAG, "Here..1");

                // outn.flush();
                Log.e(TAG, "Here..1");
                // outn.close();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "UnknownHostException" + e.toString());
            } catch (IOException e) {
                Log.e(TAG, "IOException " + e.toString() + " " + e.getMessage());
            }
            Log.e(TAG, "END OF NewDeviceClientTask");
            return null;
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Context context1 = getContext();
        String key = values.get("key").toString();
        String value = values.get("value").toString();
        String uriString = uri.toString();
        Log.e(TAG, "At Insert of ContentProvider for key " + key + " and value " + values);

        //Find AVD to Insert
        String hkey = null;
        String portToInsertH = null;
        try {
            hkey = genHash(key);
            Log.e(TAG, "Hash Key generated :: " + hkey);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, e.toString());
        }
        if (DeviceMap.isEmpty()) {
            Log.e(TAG, "DeviceMap is empty...So inserting in this AVD only :: " + portStr);
            try {
                DeviceMap.put(genHash(portStr), portStr);
                portToInsertH = genHash(portStr);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, e.toString() + e.getMessage());
            }
        } else if (hkey.compareTo(DeviceMap.lastKey()) > 0 || hkey.compareTo(DeviceMap.firstKey()) <= 0) {
            //if hkey is greter than greatest key in map, it should go to first AVD
            portToInsertH = DeviceMap.firstKey();
        } else {
            String curr;
            String prev = DeviceMap.lastKey();
            Iterator itr = DeviceMap.keySet().iterator();
            while (itr.hasNext()) {
                curr = (String) itr.next();
                if (hkey.compareTo(curr) <= 0 && hkey.compareTo(prev) > 0) {
                    portToInsertH = curr;
                    Log.e(TAG,"about to break inserting in" + portToInsertH  );
                    break;
                } else {
                    prev = curr;
                }
            }
        }

        if (portToInsertH == null) {
            Log.e(TAG, "Some Problem with Insert algo...Unable to decide device to insert");
            return null;
        }

        String portToInsert = DeviceMap.get(portToInsertH);
        Log.e(TAG, "Decided AVD for message " + key + " is " + portToInsert);
//        String portToInsert = getAVD(key);

        // If port to insert is this AVD
        if (portToInsert.equals(portStr)) {
            Log.e(TAG, "In STORE KEY/VALUE in this AVD...");

            Log.e(TAG, "filename :: " + key);
            Log.e(TAG, "message :: " + value);

            FileOutputStream outputStream;

            try {
                outputStream = context1.openFileOutput(key, context1.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }
        }
        //if Not this AVD then
        else {
//            try {
            Log.e(TAG, "Creating Client Task to request " + portToInsert + " to Insert Key " + key);
            new InsertClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, key, value, uriString, portToInsert);
            Log.e(TAG, "Got Back from Insert Client Task");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
        }

        return uri;
    }
    private class InsertClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.e(TAG, "Key in Client :: " + msgs[0]);
                Log.e(TAG, "Value in Client :: " + msgs[1]);

                ObjectOutputStream out;
                ObjectInputStream in;
                String key = msgs[0];
                String value = msgs[1];
                String uriString = msgs[2];
                String portToInsert = msgs[3];

                String port = String.valueOf((Integer.parseInt(portToInsert) * 2));
                Log.e(TAG, "Selected AVD to store message ::" + port);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));

                Node node = new Node();
                node.setMsgType("STORE KEY/VALUE");
                node.setKey(key);
                node.setValue(value);
                node.setUriString(uriString);
                node.requestingPort = myPort;

                out = new ObjectOutputStream(socket.getOutputStream());
                out.reset();
                out.writeObject(node);
                out.flush();
                out.close();
                socket.close();
                return null;
            } catch (UnknownHostException e1) {
                Log.e(TAG, e1.toString());
            } catch (IOException e1) {
                Log.e(TAG, e1.toString());
            }
            Log.e(TAG, "END OF InsertClientTask");
            return null;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Context context2 = getContext();
        Log.e(TAG, "In query for :: " + selection);
        String key = selection;

        matrixCursor = new MatrixCursor(columns);

        if (key.equals("@")) {
            matrixCursor = getAllLocalFiles();
            return matrixCursor;
        } else if (key.equals("*")) {
            synchronized (this) {
                Log.e(TAG, "Start Async Task For * ");
                try {
                    new QueryStarTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            return matrixCursor;
        } else {
            String portToQuery = getAVD(key);
            Log.e(TAG, "Port to Query is :: " + portToQuery);

            // If port to Query is this AVD
            if (portToQuery.equals(portStr)) {
                Log.e(TAG, "In QUERY KEY in same AVD..");
                String value = null;
                Log.e(TAG, "Query for file:: " + key);
                File file = context2.getFileStreamPath(key);
                Log.e(TAG, "File object :: " + file.toString());
                if (file.exists()) {
                    Log.e(TAG, "File Exists");
                    FileReader fr;
                    BufferedReader br;
                    //Read File using buffer reader and file reader
                    try {
                        fr = new FileReader(file);
                        br = new BufferedReader(fr);

                        //Get value by reading the file
                        value = br.readLine();
                        Log.e(TAG, "File content of " + key + " is  :: " + value);

                        //Writing values to Cursor
                        matrixCursor.addRow(new Object[]{key, value});

                    } catch (FileNotFoundException e) {
                        Log.e(TAG, e.toString());
                    } catch (IOException e) {
                        Log.e(TAG, e.toString());
                    }
                } else {
                    Log.e(TAG, "File " + key + " is not in " + myPort + "port");
                }
                // Message not in this AVD
            } else {

                synchronized (this) {
                    Log.e(TAG, "Starting Query Client Task for " + key);
                    try {
                        Log.e(TAG, matrixCursor.getCount() + "");
                        Log.e(TAG, "||||||||||||");
                        new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection, portToQuery).get();
                        Log.e(TAG, "||||||||||||");
                        Log.e(TAG, matrixCursor.getCount() + "");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
            return matrixCursor;
        }
//
//        synchronized (this) {
//            try {
//                Log.e(TAG, "||||||||||||");
//                //new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection).get();
//                new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,selection,"").get();
//
//                Log.e(TAG, "||||||||||||");
//            } catch (Exception e) {
//                Log.e(TAG, "error at get " + e.toString());
//            }
//            Log.e(TAG, "Task Started");
////            while (true) {
////                if (result_ready) {
////                    break;
////                }
////            }
//            Log.e(TAG, "Result is ready now :: " + result_ready);
//            return matrixCursor;
//        }
    }
    private class QueryClientTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... params) {

            String key = params[0];
            String portToQuery = params[1];

//            Log.e(TAG, "In QueryClientTask");
            ObjectOutputStream out;
            ObjectInputStream in;

            try {
                String port = String.valueOf((Integer.parseInt(portToQuery) * 2));
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                out = new ObjectOutputStream(socket.getOutputStream());

                Node node = new Node();
                node.setMsgType("QUERY KEY");
                node.setKey(key);
                node.requestingPort = myPort;

                out.writeObject(node);
                out.flush();
                Log.e(TAG, "Sent query request to appropriate AVD  ::" + port + " for  key " + key);

                in = new ObjectInputStream(socket.getInputStream());
                Node node_rec = (Node) in.readObject();
                Log.e(TAG, "Recieved Node Back " + node_rec.getMsgType() + node_rec.key + node_rec.value);
                String key1 = node_rec.key;
                String value1 = node_rec.value;

                publishProgress(key1 + "-" + value1);
                result_ready = true;

                socket.close();
                return null;
                //return null;
//                        break;
//                    } else {
//                        prev = curr;
//                    }
//                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException" + e.toString());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);
            String[] ret = new String[2];
            ret[0] = values[0].split("-")[0];
            ret[1] = values[0].split("-")[1];
            matrixCursor =  new MatrixCursor(columns);
            matrixCursor.addRow(ret);
            Log.e(TAG, "Finally: " + ret[0] + " " + ret[1]);
//            result_ready = true;
        }
    }

    private class QueryStarTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... params) {

            matrixCursor = new MatrixCursor(columns);
            matrixCursor = getAllFiles();
            return null;
        }
        public MatrixCursor getAllFiles(){

            Log.e(TAG,"In Query getAllFiles ");
            HashMap<String, String> map = new HashMap<String, String>();

            for(String portString : DeviceMap.values()) {
                try {
                    Log.e(TAG,"Request all files from port :: "+ portString);
                    HashMap<String, String> tempMap = new HashMap<String, String>();

                    String port = String.valueOf((Integer.parseInt(portString) * 2));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    Log.e(TAG, "Here2!!....");
                    Node node = new Node();
                    node.setMsgType("QUERY STAR");
                    node.requestingPort = myPort;

                    Log.e(TAG, "Here2!!....");
                    out.writeObject(node);
                    out.flush();

                    Log.e(TAG, "Here2!!....");
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    Log.e(TAG,"Wait to get Reply from server");
                    Node node_rec = (Node) in.readObject();
                    Log.e(TAG, "Recieved Node Back for Star query" + node_rec.getMsgType());
                    tempMap = node_rec.cursorMap;
//                    Log.e(TAG, "No of Rows from port  " + portString+ " is "+tempMap.size());
                    map.putAll(tempMap);
                    Log.e(TAG, "No of Rows accumulated " + map.size());
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException" + e.toString());
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            MatrixCursor mCursor = new MatrixCursor(columns);
            Iterator it = map.keySet().iterator();
            String key ;
            String value;
            String[] row = new String[2];
            while (it.hasNext()) {
                key = (String) it.next();
                value = map.get(key);
                row[0] = key;
                row[1] = value;
                mCursor.addRow(row);
            }

            Log.e(TAG,"Before returning from getAllFiles :: size - "+mCursor.getCount());
            return mCursor;
        }
    }

    public MatrixCursor getAllLocalFiles() {

        Log.e(TAG,"In getAllLocalFiles");
        Context context_at = getContext();
        MatrixCursor mCursor = new MatrixCursor(columns);

        String[] fileList = context_at.fileList();
        for (String fileName : fileList) {
            String key = fileName;
            String value = null;
//            Log.e(TAG, "Query for file:: " + key);
            File file = context_at.getFileStreamPath(key);
            Log.e(TAG, "File object :: " + file.toString());
            if (file.exists()) {
                Log.e(TAG, "File Exists");
                FileReader fr;
                BufferedReader br;
                //Read File using buffer reader and file reader
                try {
                    fr = new FileReader(file);
                    br = new BufferedReader(fr);

                    //Get value by reading the file
                    value = br.readLine();
                    Log.e(TAG, "File content of " + key + " is  :: " + value);

                    //Writing values to Cursor
                    mCursor.addRow(new Object[]{key, value});

                } catch (FileNotFoundException e) {
                    Log.e(TAG, e.toString());
                } catch (IOException e) {
                    Log.e(TAG, e.toString());
                }
            } else {
                Log.e(TAG, "File " + key + " is not in " + myPort + "port");
            }
        }
        return mCursor;
    }

    public String getAVD(String key) {
        //Find AVD to Query from
        String hkey = null;
        String portToQueryH = null;
        try {
            hkey = genHash(key);
            Log.e(TAG, "Hash Key generated :: " + hkey);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, e.toString());
        }

        if (hkey.compareTo(DeviceMap.lastKey()) > 0 || hkey.compareTo(DeviceMap.firstKey()) <= 0) {
            //if hkey is greter than greatest key in map, it should go to first AVD
            portToQueryH = DeviceMap.firstKey();
        } else {
            String curr;
            String prev = DeviceMap.lastKey();
            Iterator itr = DeviceMap.keySet().iterator();
            while (itr.hasNext()) {
                curr = (String) itr.next();
                if (hkey.compareTo(curr) <= 0 && hkey.compareTo(prev) > 0) {
                    portToQueryH = curr;
                    break;
                } else {
                    prev = curr;
                }
            }
        }

        if (portToQueryH == null) {
            Log.e(TAG, "Some Problem with Query algo...Unable to decide device to insert");
        }

        String portToQuery = DeviceMap.get(portToQueryH);
        Log.e(TAG, "Decided AVD for querying message " + key + " is " + portToQuery);

        return portToQuery;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        Context context_del = getContext();
        Log.e(TAG, "In delete for " + selection);
        String key = selection;
        if(key.equals("@")){
            for(String file : context_del.fileList()){
                context_del.deleteFile(file);
            }
        }
        else if(key.equals("*")){
            new DeleteTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,key,null);
        }

        String portToDelete = getAVD(key);
        if(portToDelete.equals(portStr)){
            context_del.deleteFile(key);
        }

        new DeleteTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,key,portToDelete);

        return 0;
    }
    private class DeleteTask extends AsyncTask<String, String, Void>{
        @Override
        protected Void doInBackground(String... params) {

            String key = params[0];
            if(key.equals("*")){
                for(String portString : DeviceMap.values()) {
                    try {
                        Log.e(TAG,"Request delete all files from port :: "+ portString);

                        String port = String.valueOf((Integer.parseInt(portString) * 2));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                        Log.e(TAG, "Here2!!....");
                        Node node = new Node();
                        node.setMsgType("DELETE STAR");
                        node.requestingPort = myPort;

                        Log.e(TAG, "Here2!!....");
                        out.writeObject(node);
                        out.flush();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException" + e.toString());
                    }
                }
            }
            else{
                String portToDelete = params[1];
                Log.e(TAG, "Going to delete key " + key + " from " + portToDelete);
                try {
                    String port = String.valueOf((Integer.parseInt(portToDelete) * 2));
                    Socket socket = null;
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    Node node = new Node();
                    node.setMsgType("DELETE FILE");
                    node.setKey(key);
                    node.requestingPort = myPort;

                    out.writeObject(node);
                    out.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }


                Node node = new Node();
                node.setMsgType("DELETE FILE");
            }

            return null;
        }
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while (true) {
                synchronized (this){
                try {
                    Log.e(TAG, "I am before socket.accept in Provider");
                    Socket clientSocket = serverSocket.accept();
                    Log.e(TAG, "Server accepted socket");
                    ObjectInputStream in;
                    ObjectOutputStream out;

                    Log.e(TAG, "***************");

                    in = new ObjectInputStream(clientSocket.getInputStream());
                    Node node = (Node) in.readObject();
                    Log.e(TAG, "Message Type : " + node.getMsgType());

                    //If message is for Insert from 1st person client
                    if (node.getMsgType().equals("NEW AVD JOIN REQUEST")) {
                        try {
                            Log.e(TAG, "New join request by " + node.requestingPort);
                            String requestingPort = node.requestingPort;
                            DeviceMap.put(genHash(requestingPort), requestingPort);
                            Node node1 = new Node();
                            node1.setMsgType("UPDATE DEVICE MAP TABLE");
                            node1.map = DeviceMap;
                            for (String portStr : DeviceMap.keySet()) {
                                String port1 = DeviceMap.get(portStr);
                                String port = String.valueOf((Integer.parseInt(port1) * 2));
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                                out = new ObjectOutputStream(socket.getOutputStream());
                                out.writeObject(node1);
                                out.flush();
                                out.close();
                            }
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                    } else if (node.getMsgType().equals("UPDATE DEVICE MAP TABLE")) {
                        Log.e(TAG, "Updating new map");
                        DeviceMap = node.map;
                        Iterator itr1 = DeviceMap.keySet().iterator();
                        while (itr1.hasNext()) {
                            Log.e(TAG, DeviceMap.get(itr1.next()));
                        }
                    } else if (node.getMsgType().equals("STORE KEY/VALUE")) {
                        Context context1 = getContext();

                        Log.e(TAG, "In STORE KEY/VALUE ...");
                        String filename = node.key;
                        String value = node.value;

                        Log.e(TAG, "filename :: " + filename);
                        Log.e(TAG, "message :: " + value);

                        FileOutputStream outputStream;

                        try {
                            outputStream = context1.openFileOutput(filename, context1.MODE_PRIVATE);
                            outputStream.write(value.getBytes());
                            outputStream.close();
                        } catch (Exception e) {
                            Log.e(TAG, e.toString());
                        }
                        getContext().getContentResolver().notifyChange(Uri.parse(node.uriString), null);
                    } else if (node.getMsgType().equals("QUERY KEY")) {
                        Log.e(TAG, "In QUERY KEY");
                        Context context2 = getContext();
                        String value = null;
                        String key = node.key;
                        Log.e(TAG, "Query for file:: " + key);
                        File file = context2.getFileStreamPath(key);
                        Log.e(TAG, "File object :: " + file.toString());
                        if (file.exists()) {
                            Log.e(TAG, "File Exists");
                            FileReader fr;
                            BufferedReader br;
                            //Read File using buffer reader and file reader
                            try {
                                fr = new FileReader(file);
                                br = new BufferedReader(fr);

                                //Get value by reading the file
                                value = br.readLine();
                                Log.e(TAG, "File content of " + key + " is  :: " + value);

                                //Writing values to node
                                node.setMsgType("QUERY REPLY");
                                node.setValue(value);

                            } catch (FileNotFoundException e) {
                                Log.e(TAG, e.toString());
                            } catch (IOException e) {
                                Log.e(TAG, e.toString());
                            }
                        }
                        //Send file back to the requesting port
                        ObjectOutputStream out1 = new ObjectOutputStream(clientSocket.getOutputStream());
                        Log.e(TAG, "Replying for query with value :: " + node.value);
                        out1.writeObject(node);
                        out1.flush();
//                        out1.close();
                    } else if (node.getMsgType().equals("QUERY STAR")) {
                        Log.e(TAG, "In query * at server...");
                        Context context_str = getContext();
//                        MatrixCursor localCursor = new MatrixCursor(columns);
                        HashMap<String, String> starMap = new HashMap<String, String>();

                        String[] fileList = context_str.fileList();
                        for (String fileName : fileList) {
                            String key = fileName;
                            String value = null;
                            File file = context_str.getFileStreamPath(key);
                            Log.e(TAG, "File object :: " + file.toString());
                            if (file.exists()) {
                                Log.e(TAG, "File Exists");
                                FileReader fr;
                                BufferedReader br;
                                //Read File using buffer reader and file reader
                                try {
                                    fr = new FileReader(file);
                                    br = new BufferedReader(fr);

                                    //Get value by reading the file
                                    value = br.readLine();
                                    Log.e(TAG, "File content of " + key + " is  :: " + value);

                                    //Writing values to Cursor
                                    starMap.put(key, value);
                                } catch (FileNotFoundException e) {
                                    Log.e(TAG, e.toString());
                                } catch (IOException e) {
                                    Log.e(TAG, e.toString());
                                }
                            } else {
                                Log.e(TAG, "File " + key + " is not in " + myPort + "port");
                            }
                        }

                        Node node1 = new Node();
                        node1.initCursorMap();
                        node1.setMsgType("STAR REPLY FROM " + portStr);
                        node1.cursorMap.putAll(starMap);
                        Log.e(TAG, "Size of map I am sending" + node1.cursorMap.size());
                        ObjectOutputStream out2 = new ObjectOutputStream(clientSocket.getOutputStream()); // can be a problem
                        Log.e(TAG, "Replying matrix cursor from  :: " + portStr);
                        out2.writeObject(node1);
                        out2.flush();
                    } else if (node.getMsgType().equals("DELETE FILE")) {
                        Log.e(TAG, "Deleting  files from " + portStr);
                        Context context_del = getContext();
                        String key = node.key;
                        context_del.deleteFile(key);
                    } else if (node.getMsgType().equals("DELETE STAR")) {
                        Log.e(TAG, "Deleting all files from " + portStr);
                        Context context_del = getContext();
                        for (String file : context_del.fileList()) {
                            context_del.deleteFile(file);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "Server vala exception!!!" + e.toString());
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, e.toString());
                }
            }
        }
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
}
