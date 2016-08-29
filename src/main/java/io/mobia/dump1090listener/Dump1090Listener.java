package io.mobia.dump1090listener;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

/**
 * @author Koen Aerts
 * @version 1.0.0
 */
public class Dump1090Listener {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
    private static final String csvDelimiter = ",";
    private static final Pattern csvPattern = Pattern.compile(
      "(\\" + csvDelimiter + "|^)" + // Delimiters.
      "(?:\"([^\"]*(?:\"\"[^\"]*)*)\"|" + // Quoted fields.
      "([^\"\\" + csvDelimiter + "]*))" // Standard fields.
      , Pattern.CASE_INSENSITIVE
    );

    public static void main(String[] args) throws Throwable {
        String host = "localhost";
        int port = 30003;
        int retrywaitms = 5000;
        boolean persist = false;
        String colName = "messages";
        String dbName = "test";
        String dbUrl = "mongodb://localhost:27017";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--host") && (i+1) < args.length) {
                host = args[i+1];
                i++;
            } else if (args[i].equals("--port") && (i+1) < args.length) {
                port = Integer.parseInt(args[i+1]);
                i++;
            } else if (args[i].equals("--retrywaitms") && (i+1) < args.length) {
                retrywaitms = Integer.parseInt(args[i+1]);
                i++;
            } else if (args[i].equals("--mongouri") && (i+1) < args.length) {
                dbUrl = args[i+1];
                i++;
            } else if (args[i].equals("--mongodb") && (i+1) < args.length) {
                dbName = args[i+1];
                i++;
            } else if (args[i].equals("--collection") && (i+1) < args.length) {
                colName = args[i+1];
                i++;
            } else if (args[i].equals("--persist")) {
                persist = true;
            } else if (args[i].equals("--help")) {
                System.out.println("usage: " + Dump1090Listener.class.getSimpleName() + " [options]");
                System.out.println("Options:");
                System.out.println("  --host          IP or hostname that dump1090 is running on. Default is localhost");
                System.out.println("  --port          dump1090 TCP port. Default is 30003");
                System.out.println("  --retrywaitms   ms to wait for retry in case of connection error. Only used when");
                System.out.println("                  --persist is specified. Default value is 5000");
                System.out.println("  --mongouri      URI of MongoDB where data should be stored to. Default is");
                System.out.println("                  mongodb://localhost:27017");
                System.out.println("  --mongodb       name of MongoDB database. Default is test");
                System.out.println("  --collection    name of MongoDB collection where data should be stored into.");
                System.out.println("                  Default is messages");
                System.out.println("  --persist       when specified, retry a failed TCP connection");
                System.exit(0);
            } else {
                System.out.println("Invalid parameter '" + args[i] + "'. Use --help for more info.");
                System.exit(1);
            }
        }
        Dump1090Listener listener = new Dump1090Listener();
        //listener.test(dbUrl, dbName, colName);
        listener.process(host, port, persist, retrywaitms, dbUrl, dbName, colName);
    }

    public void test(String dbUrl, String dbName, String colName) throws Throwable {
        MongoClient mongoClient = new MongoClient(new MongoClientURI(dbUrl));
        mongoClient.setWriteConcern(WriteConcern.MAJORITY);
        MongoDatabase mongoDb = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = mongoDb.getCollection(colName);

        storeRecord(col, "MSG,3,111,11111,502C8F,111111,2015/05/01,20:28:11.080,2015/05/01,20:28:11.071,,33975,,,50.33498,5.93336,,,,,,");
        storeRecord(col, "MSG,3,111,11111,\"502C8F , ABC\",111111,2015/05/01,20:28:11.080,2015/05/01,20:28:11.071,,33975,,,50.33498,5.93336,,,,,,");
        //storeRecord(col, "MSG,8,111,11111,AC46C3,111111,2016/08/23,13:23:13.359,2016/08/23,13:23:13.331,,,,,,,,,,,,0");
        //storeRecord(col, "MSG,2,496,603,400CB6,13168,2008/10/13,12:24:32.414,2008/10/13,12:28:52.074,,,0,76.4,258.3,54.05735,-4.38826,,,,,,0");
        //storeRecord(col, "MSG,3,496,211,4CA2D6,10057,2008/11/28,14:53:50.594,2008/11/28,14:58:51.153,,37000,,,51.45735,-1.02826,,,0,0,0,0");
        //storeRecord(col, "MSG,3,111,11111,502C8F,111111,2015/05/01,20:28:11.080,2015/05/01,20:28:11.071,,33975,,,50.33498,5.93336,,,,,,0");

        mongoClient.close();
    }

    public void process(String host, int port, boolean persist, int retrywaitms, String dbUrl, String dbName, String colName) throws Throwable {

        InetAddress address = InetAddress.getByName(host);
        do {
            MongoClient mongoClient = null;
            Socket connection = null;
            try {
                System.out.println("Connecting to MongoDB...");
                mongoClient = new MongoClient(new MongoClientURI(dbUrl));
                mongoClient.setWriteConcern(WriteConcern.MAJORITY);
                MongoDatabase mongoDb = mongoClient.getDatabase(dbName);
                MongoCollection<Document> col = mongoDb.getCollection(colName);

                System.out.println("Listening to port " + port + " on " + host);
                connection = new Socket(address, port);
                BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                while ((line = br.readLine()) != null) {
                    storeRecord(col, line);
                }
            } catch (Throwable t) {
                System.out.println(t.getMessage());
                if (connection != null) {
                    System.out.println("Disconnecting from dump1090...");
                    try {
                        connection.close();
                    } catch (Throwable tt) {
                        System.out.println(tt.getMessage());
                    }
                    connection = null;
                }
                if (mongoClient != null) {
                    System.out.println("Disconnecting from MongoDB...");
                    try {
                        mongoClient.close();
                    } catch (Throwable tt) {
                        System.out.println(tt.getMessage());
                    }
                    mongoClient = null;
                }
            } finally {
                if (persist) {
                    System.out.println("Trying to re-establish connection after " + retrywaitms + "ms...");
                    Thread.currentThread().sleep(retrywaitms);
                }
            }
        } while (persist);
    }

    private void storeRecord(MongoCollection<Document> col, String record) throws Throwable {
        try {
            Document doc = generateJsonDocument(record);
            if (doc != null) {
                col.insertOne(doc);
                System.out.println(record);
            }
        } catch (Throwable t) {
            System.out.println("Unexpected error: " + record);
        }
    }

    private Document generateJsonDocument(String record) throws Throwable {
        String[] fields = splitRecord(record);
        if (!"MSG".equalsIgnoreCase(fields[0])) {
            System.out.println("Only interested in message type 'MSG': " + record);
            return null;
        }
        if (fields.length != 22) {
            System.out.println("Expected 22 fields in this message. Instead received " + fields.length + ": " + record);
            return null;
        }
        Document doc = new Document("messagetype", fields[0]);

        int transmissionType = Integer.parseInt(fields[1]);

        doc.append("transmissiontype", transmissionType);
        doc.append("icao24", fields[4]);
        doc.append("messageloggedts", sdf.parse(fields[8] + " " + fields[9]));

        switch (transmissionType) {
        case 1 : // ES Identification and Category
            doc.append("callsign", fields[10]);
            break;
        case 2 : // ES Surface Position Message
            doc.append("altitude", fields[11].isEmpty() ? "" : Integer.parseInt(fields[11]));
            doc.append("groundspeed", fields[12].isEmpty() ? "" : Integer.parseInt(fields[12]));
            doc.append("track", fields[13].isEmpty() ? "" : Integer.parseInt(fields[13]));
            doc.append("position", Arrays.asList(Double.parseDouble(fields[15]), Double.parseDouble(fields[14])));
            doc.append("ground", fields[21].isEmpty() ? "" : Integer.parseInt(fields[21]));
            break;
        case 3 : // ES Airborne Position Message
            doc.append("altitude", fields[11].isEmpty() ? "" : Integer.parseInt(fields[11]));
            doc.append("position", Arrays.asList(Double.parseDouble(fields[15]), Double.parseDouble(fields[14])));
            doc.append("alert", fields[18].isEmpty() ? "" : Integer.parseInt(fields[18]));
            doc.append("emergency", fields[19].isEmpty() ? "" : Integer.parseInt(fields[19]));
            doc.append("spi", fields[20].isEmpty() ? "" : Integer.parseInt(fields[20]));
            doc.append("ground", fields[21].isEmpty() ? "" : Integer.parseInt(fields[21]));
            break;
        case 4 : // ES Surface Position Message
            doc.append("groundspeed", fields[12].isEmpty() ? "" : Integer.parseInt(fields[12]));
            doc.append("track", fields[13].isEmpty() ? "" : Integer.parseInt(fields[13]));
            doc.append("verticalrate", fields[16].isEmpty() ? "" : Integer.parseInt(fields[16]));
            break;
        case 5 : // Surveillance Alt Message
            doc.append("altitude", fields[11].isEmpty() ? "" : Integer.parseInt(fields[11]));
            doc.append("alert", fields[18].isEmpty() ? "" : Integer.parseInt(fields[18]));
            doc.append("spi", fields[20].isEmpty() ? "" : Integer.parseInt(fields[20]));
            doc.append("ground", fields[21].isEmpty() ? "" : Integer.parseInt(fields[21]));
            break;
        case 6 : // Surveillance ID Message
            doc.append("altitude", fields[11].isEmpty() ? "" : Integer.parseInt(fields[11]));
            doc.append("squawk", fields[17].isEmpty() ? "" : Integer.parseInt(fields[17]));
            doc.append("alert", fields[18].isEmpty() ? "" : Integer.parseInt(fields[18]));
            doc.append("emergency", fields[19].isEmpty() ? "" : Integer.parseInt(fields[19]));
            doc.append("spi", fields[20].isEmpty() ? "" : Integer.parseInt(fields[20]));
            doc.append("ground", fields[21].isEmpty() ? "" : Integer.parseInt(fields[21]));
            break;
        case 7 : // Air To Air Message
            doc.append("altitude", fields[11].isEmpty() ? "" : Integer.parseInt(fields[11]));
            doc.append("ground", fields[21].isEmpty() ? "" : Integer.parseInt(fields[21]));
            break;
        case 8 : // All Call Reply
            doc.append("ground", fields[21].isEmpty() ? "" : Integer.parseInt(fields[21]));
            break;
        }
        return doc;
// MSG,6,111,11111,C06B60,111111,2016/08/22,22:35:20.965,2016/08/22,22:35:20.934,,,,,,,,5231,0,0,0,0
// MSG,7,111,11111,C06B60,111111,2016/08/22,22:35:21.312,2016/08/22,22:35:21.264,,10200,,,,,,,,,,0
/*
MSG,8,111,11111,AC46C3,111111,2016/08/23,13:23:13.359,2016/08/23,13:23:13.331,,,,,,,,,,,,0
MSG,7,111,11111,AC46C3,111111,2016/08/23,13:23:13.571,2016/08/23,13:23:13.529,,12925,,,,,,,,,,0
MSG,7,111,11111,AC46C3,111111,2016/08/23,13:23:14.184,2016/08/23,13:23:14.180,,12950,,,,,,,,,,0
MSG,7,111,11111,AC46C3,111111,2016/08/23,13:23:15.297,2016/08/23,13:23:15.294,,13000,,,,,,,,,,0
MSG,7,111,11111,AC46C3,111111,2016/08/23,13:23:15.570,2016/08/23,13:23:15.557,,13025,,,,,,,,,,0
MSG,7,111,11111,AC46C3,111111,2016/08/23,13:23:15.660,2016/08/23,13:23:15.625,,13025,,,,,,,,,,0
MSG,6,111,11111,AC46C3,111111,2016/08/23,13:23:17.159,2016/08/23,13:23:17.132,,,,,,,,4210,0,0,0,0
MSG,8,111,11111,C044D1,111111,2016/08/23,13:27:39.940,2016/08/23,13:27:39.941,,,,,,,,,,,,
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:40.644,2016/08/23,13:27:40.602,,8600,,,,,,,,,,0
MSG,8,111,11111,C044D1,111111,2016/08/23,13:27:41.848,2016/08/23,13:27:41.843,,,,,,,,,,,,
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:41.963,2016/08/23,13:27:41.914,,8600,,,,,,,,,,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:42.301,2016/08/23,13:27:42.301,,8700,,,,,,,,,,0
MSG,5,111,11111,C044D1,111111,2016/08/23,13:27:43.253,2016/08/23,13:27:43.222,,8700,,,,,,,0,,0,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:43.566,2016/08/23,13:27:43.548,,8700,,,,,,,,,,0
MSG,8,111,11111,C044D1,111111,2016/08/23,13:27:43.862,2016/08/23,13:27:43.814,,,,,,,,,,,,
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:45.017,2016/08/23,13:27:44.991,,8700,,,,,,,,,,0
MSG,5,111,11111,C044D1,111111,2016/08/23,13:27:46.054,2016/08/23,13:27:46.038,,8700,,,,,,,0,,0,0
MSG,8,111,11111,C044D1,111111,2016/08/23,13:27:46.226,2016/08/23,13:27:46.174,,,,,,,,,,,,
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:46.509,2016/08/23,13:27:46.497,,8700,,,,,,,,,,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:47.092,2016/08/23,13:27:47.086,,8700,,,,,,,,,,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:47.271,2016/08/23,13:27:47.222,,8700,,,,,,,,,,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:48.495,2016/08/23,13:27:48.465,,8700,,,,,,,,,,0
MSG,8,111,11111,C044D1,111111,2016/08/23,13:27:49.758,2016/08/23,13:27:49.712,,,,,,,,,,,,
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:50.047,2016/08/23,13:27:50.036,,8800,,,,,,,,,,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:52.306,2016/08/23,13:27:52.268,,8800,,,,,,,,,,0
MSG,8,111,11111,C044D1,111111,2016/08/23,13:27:53.813,2016/08/23,13:27:53.775,,,,,,,,,,,,
MSG,8,111,11111,C044D1,111111,2016/08/23,13:27:54.916,2016/08/23,13:27:54.888,,,,,,,,,,,,
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:55.126,2016/08/23,13:27:55.086,,8800,,,,,,,,,,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:55.609,2016/08/23,13:27:55.606,,8800,,,,,,,,,,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:27:58.534,2016/08/23,13:27:58.494,,8900,,,,,,,,,,0
MSG,8,111,11111,C044D1,111111,2016/08/23,13:27:59.739,2016/08/23,13:27:59.735,,,,,,,,,,,,
MSG,7,111,11111,C044D1,111111,2016/08/23,13:28:01.572,2016/08/23,13:28:01.570,,8900,,,,,,,,,,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:28:01.851,2016/08/23,13:28:01.834,,8900,,,,,,,,,,0
MSG,5,111,11111,C044D1,111111,2016/08/23,13:28:01.855,2016/08/23,13:28:01.834,,8900,,,,,,,0,,0,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:28:02.069,2016/08/23,13:28:02.033,,8900,,,,,,,,,,0
MSG,7,111,11111,C044D1,111111,2016/08/23,13:28:05.069,2016/08/23,13:28:05.046,,9000,,,,,,,,,,0

*/
    }

    private String[] splitRecord(String record) {
        List<String> fields = new ArrayList<String>();
        Matcher m = csvPattern.matcher(record);
        while (m.find()) {
            fields.add(m.group(3) == null ? m.group(2).replaceAll("\"\"", "\"").trim() : m.group(3).trim());
        }
        return fields.toArray(new String[0]);
    }
}
