package initdb;

import java.sql.*;
import java.util.*;
import java.io.*;

// java populate movies.dat movie_countries.dat movie_genres.dat movie_directors.dat movie_actors.dat tags.dat movie_tags.dat user_taggedmovies.dat user_ratedmovies.dat

public class Populate{
    public static void main(String args[]){
        String[] ars = {"movies.dat", "movie_countries.dat", "movie_genres.dat", "movie_locations.dat", "movie_directors.dat", "movie_actors.dat", "tags.dat", "movie_tags.dat", "user_ratedmovies.dat"};
        Populate letsDoThis = new Populate();
        letsDoThis.run(ars);
    }

    private void run(String args[]){
        Connection con = null;
        try{
            // connection
            System.out.println("Connecting...");
            con = openConnection();

            //clean table
            System.out.println("Clean table start...");
            cleanTable(con);

            //create table
            System.out.println("create table start...");
            createTable(con);

            //insert data into tables
            System.out.println("insert values start...");
            readDatFileAndPublish(args, con);


        }catch(SQLException e){
            System.err.println("Error occurs when communicating with the database server" + e.getMessage());

        }catch(ClassNotFoundException e){
            System.err.println("Cannot find the database driver");

        }catch (Exception e){
            System.err.println("something wrong: " + e.getMessage());

        }finally{
            // close connection
            System.out.println("Disconnecting...");
            closeConnection(con);
        }
    }

    private void readDatFileAndPublish(String args[], Connection con) throws Exception{

        // populate Movies ----------------------------------------------------
        String filename = args[0];
        String filename2 = args[1];
        System.out.println("start insert data for table Movies with " + filename + ", " + filename2);
        String table_name = "Movies";
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + table_name +" VALUES(?,?,?,?,?,?,?,?,?,?)" );

        // read the file via Scanner
        Scanner scanner = new Scanner( new FileInputStream( new File(filename)));
        Scanner scanner2 = new Scanner( new FileInputStream( new File(filename2)));

        if(scanner.hasNextLine()){
            // discard the first line (column name)
            String[] columns = scanner.nextLine().split("\t", -1);
            String[] columns2 = scanner2.nextLine().split("\t", -1);
            String text1 = "";
            String text2 = "";

            // send the query to server
            while(scanner.hasNextLine()){
                text1 = scanner.nextLine();
                text2 = scanner2.nextLine();
                columns = text1.split("\t", -1);
                columns2 = text2.split("\t", -1);
                pstmt.setString(1, columns[0]); //mid
                pstmt.setString(2, columns2[1]); //country
                pstmt.setString(3, columns[1]); //title
                pstmt.setInt(4, Integer.valueOf(columns[5])); //year
                if(columns[7].equals("\\N")) pstmt.setNull(5, java.sql.Types.FLOAT);
                else pstmt.setFloat(5, Float.parseFloat(columns[7])); //rtAllCriticsRating
                if(columns[8].equals("\\N")) pstmt.setNull(6, java.sql.Types.INTEGER);
                else pstmt.setInt(6, Integer.valueOf(columns[8])); //rtAllCriticsNumReviews
                if(columns[12].equals("\\N")) pstmt.setNull(7, java.sql.Types.FLOAT);
                else pstmt.setFloat(7, Float.parseFloat(columns[12])); //rtTopCriticsRating
                if(columns[13].equals("\\N")) pstmt.setNull(8, java.sql.Types.INTEGER);
                else pstmt.setInt(8, Integer.valueOf(columns[13])); //rtTopCriticsNumReviews
                if(columns[17].equals("\\N")) pstmt.setNull(9, java.sql.Types.FLOAT);
                else pstmt.setFloat(9, Float.parseFloat(columns[17])); //rtAudienceRating
                if(columns[18].equals("\\N")) pstmt.setNull(10, java.sql.Types.INTEGER);
                else pstmt.setInt(10, Integer.valueOf(columns[18])); //rtAudienceNumRatings

                pstmt.addBatch();
//                pstmt.executeUpdate();
            }
            int[] updateCounts = pstmt.executeBatch();
            checkUpdateCounts(updateCounts);
            pstmt.close();
        }
        scanner.close();
        scanner2.close();

        // populate movie_genres ----------------------------------------------------
        filename = args[2];
        System.out.println("start insert data for table movie_genres with " + filename);
        table_name = "movie_genres";
        pstmt = con.prepareStatement("INSERT INTO " + table_name +" VALUES(?,?)" );

        // read the file via Scanner
        scanner = new Scanner( new FileInputStream( new File(filename)));

        if(scanner.hasNextLine()){
            // print the original text content from the first line
            // System.out.println( scanner.nextLine() );

            // discard the first line (column name)
            String[] columns = scanner.nextLine().split("\t", -1);
            //System.out.println("Columns: " + columns.length);

            // send the query to server
            while(scanner.hasNextLine()){
                columns = scanner.nextLine().split("\t", -1);
                pstmt.setString(1, columns[0]);
                pstmt.setString(2, columns[1]);
                pstmt.addBatch();
//                pstmt.executeUpdate();
            }
            int[] updateCounts = pstmt.executeBatch();
            checkUpdateCounts(updateCounts);
            pstmt.close();
        }
        scanner.close();

        //populate movie location
        filename = args[3];
        System.out.println("start insert data for table Movie_location with " + filename);
        table_name = "Movie_location";
        pstmt = con.prepareStatement("INSERT INTO " + table_name +" VALUES(?,?)");
        Set<String> set = new HashSet<>();

        scanner = new Scanner(new FileInputStream(new File(filename)));
        if(scanner.hasNextLine()){
            // print the original text content from the first line
            // System.out.println( scanner.nextLine() );

            // discard the first line (column name)
            String[] columns = scanner.nextLine().split("\t", -1);
            //System.out.println("Columns: " + columns.length);

            // send the query to server
            while(scanner.hasNextLine()){
                columns = scanner.nextLine().split("\t", -1);
                if(columns.length > 2 && columns[1].length() > 0 && set.add(columns[1])){
                    pstmt.setString(1, columns[0]);
                    pstmt.setString(2, columns[1]);
                    pstmt.addBatch();
                }
            }
            int[] updateCounts = pstmt.executeBatch();
            checkUpdateCounts(updateCounts);
            pstmt.close();
        }
        scanner.close();


        // populate Directors ----------------------------------------------------
        filename = args[4];
        System.out.println("start insert data for table Directors with " + filename);
        table_name = "Directors";
        pstmt = con.prepareStatement("INSERT INTO " + table_name +" VALUES(?,?,?)" );

        // read the file via Scanner
        scanner = new Scanner( new FileInputStream( new File(filename)));

        if(scanner.hasNextLine()){
            // print the original text content from the first line
            // System.out.println( scanner.nextLine() );

            // discard the first line (column name)
            String[] columns = scanner.nextLine().split("\t", -1);
            //System.out.println("Columns: " + columns.length);

            // send the query to server
            while(scanner.hasNextLine()){
                columns = scanner.nextLine().split("\t", -1);
                pstmt.setString(1, columns[1]);
                pstmt.setString(2, columns[2]);
                pstmt.setString(3, columns[0]);
                pstmt.addBatch();
//                pstmt.executeUpdate();
            }
            int[] updateCounts = pstmt.executeBatch();
            checkUpdateCounts(updateCounts);
            pstmt.close();
        }
        scanner.close();

        // populate Actors ----------------------------------------------------
        filename = args[5];
        System.out.println("start insert data for table Actors with " + filename);
        table_name = "Actors";
        pstmt = con.prepareStatement("INSERT INTO " + table_name +" VALUES(?,?,?)" );

        // read the file via Scanner
        scanner = new Scanner( new FileInputStream( new File(filename)));

        if(scanner.hasNextLine()){
            // print the original text content from the first line
            // System.out.println( scanner.nextLine() );

            // discard the first line (column name)
            String[] columns = scanner.nextLine().split("\t", -1);
            //System.out.println("Columns: " + columns.length);

            // send the query to server
            while(scanner.hasNextLine()){
                columns = scanner.nextLine().split("\t", -1);
                pstmt.setString(1, columns[1]);
                pstmt.setString(2, columns[2]);
                pstmt.setString(3, columns[0]);
                pstmt.addBatch();
//                pstmt.executeUpdate();
            }
            int[] updateCounts = pstmt.executeBatch();
            checkUpdateCounts(updateCounts);
            pstmt.close();
        }
        scanner.close();

        // populate Tags ----------------------------------------------------
        filename = args[6];
        System.out.println("start insert data for table Tags with " + filename);
        table_name = "Tags";
        pstmt = con.prepareStatement("INSERT INTO " + table_name +" VALUES(?,?)" );


        scanner = new Scanner( new FileInputStream( new File(filename)));

        if(scanner.hasNextLine()){
            // print the original text content from the first line
            // System.out.println( scanner.nextLine() );

            // discard the first line (column name)
            String[] columns = scanner.nextLine().split("\t", -1);
            //System.out.println("Columns: " + columns.length);

            // send the query to server
            while(scanner.hasNextLine()){
                columns = scanner.nextLine().split("\t", -1);
                if(set.add(columns[1])){
                    pstmt.setString(1, columns[0]);
                    pstmt.setString(2, columns[1]);
                    pstmt.addBatch();
                }
            }
            int[] updateCounts = pstmt.executeBatch();
            checkUpdateCounts(updateCounts);
            pstmt.close();
        }
        scanner.close();

        // populate movie_tags ----------------------------------------------------
        filename = args[7];
        System.out.println("start insert data for table movie_tags with " + filename);
        table_name = "movie_tags";
        pstmt = con.prepareStatement("INSERT INTO " + table_name +" VALUES(?,?,?)" );

        // read the file via Scanner
        scanner = new Scanner( new FileInputStream( new File(filename)));

        if(scanner.hasNextLine()){
            // print the original text content from the first line
            //System.out.println( scanner.nextLine() );

            // discard the first line (column name)
            String[] columns = scanner.nextLine().split("\t", -1);
            //System.out.println("Columns: " + columns.length);

            // send the query to server
            while(scanner.hasNextLine()){
                columns = scanner.nextLine().split("\t", -1);
                pstmt.setString(1, columns[0]);
                pstmt.setString(2, columns[1]);
                pstmt.setFloat(3, Float.parseFloat(columns[2]));
                pstmt.addBatch();
//                pstmt.executeUpdate();
            }
            int[] updateCounts = pstmt.executeBatch();
            checkUpdateCounts(updateCounts);
//            pstmt.close();
            pstmt.close();
        }
        scanner.close();

        // populate user_ratedmovies ----------------------------------------------------
        filename = args[8];
        System.out.println("start insert data for table user_ratedmovies with " + filename);
        table_name = "user_ratedmovies";
        pstmt = con.prepareStatement("INSERT INTO " + table_name +" VALUES(?,?,?,?)" );

        // read the file via Scanner
        scanner = new Scanner( new FileInputStream( new File(filename)));

        if(scanner.hasNextLine()){
            // print the original text content from the first line
            // System.out.println( scanner.nextLine() );

            // discard the first line (column name)
            String[] columns = scanner.nextLine().split("\t", -1);
            //System.out.println("Columns: " + columns.length);

            // send the query to server
            while(scanner.hasNextLine()){
                columns = scanner.nextLine().split("\t", -1);
                pstmt.setString(1, columns[0]);
                pstmt.setString(2, columns[1]);
                pstmt.setFloat(3, Float.parseFloat(columns[2]));
                // 3 date_day columns[3]
                // 4 date_month columns[4]
                // 5 date_year columns[5]
                // ("2013-09-04");
                pstmt.setDate(4, java.sql.Date.valueOf( columns[5] + "-" + columns[4] +"-" +columns[3]));
                pstmt.addBatch();
//                pstmt.executeUpdate();
            }
            int[] updateCounts = pstmt.executeBatch();
            checkUpdateCounts(updateCounts);
            pstmt.close();
        }
        scanner.close();

        con.commit();
    }

    private static void checkUpdateCounts(int[] updateCounts) {
        for (int i = 0; i < updateCounts.length; i++) {
            if (updateCounts[i] >= 0) {
//                System.out.println("OK; updateCount=" + updateCounts[i]);
            } else if (updateCounts[i] == Statement.SUCCESS_NO_INFO) {
//                System.out.println("OK; updateCount=Statement.SUCCESS_NO_INFO at " + i);
            } else if (updateCounts[i] == Statement.EXECUTE_FAILED) {
                System.out.println("Failure; updateCount=Statement.EXECUTE_FAILED at " + i);
            }
        }
    }

    private void cleanTable(Connection con) throws SQLException{
        String[] table_names = {"user_ratedmovies", "movie_tags", "Tags", "Actors", "Directors", "Movie_location", "movie_genres", "Movies"};
        String query = "";
        Statement stmt = con.createStatement();
        for (int i = 0; i<table_names.length ; ++i ) {
            try{
                query = "DROP TABLE " + table_names[i];
                stmt.executeUpdate(query);
            } catch(SQLException e){
                System.err.println("Error occurs when communicating with the database server" + e.getMessage());

            }
        }
        stmt.close();
    }

    private Connection openConnection() throws SQLException, ClassNotFoundException{
        //
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());

        //
        String host = "localhost";
        String port = "49161";
        String dbName = "xe";
        String userName = "system";
        String password = "oracle";

        //
        String dbURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName;
        return DriverManager.getConnection(dbURL, userName, password);
    }

    private void closeConnection(Connection con){
        try{
            con.close();
        }catch(SQLException e){
            System.err.println("Cannot close Connection: " + e.getMessage());
        }
    }
    private void createTable(Connection con) throws SQLException{
        String[] slqs = {"CREATE TABLE Movies(mid VARCHAR(5),country VARCHAR(50),title VARCHAR(150),year NUMBER,rtAllCriticsRating NUMBER,rtAllCriticsNumReviews NUMBER,rtTopCriticsRating NUMBER,rtTopCriticsNumReviews NUMBER,rtAudienceRating NUMBER,rtAudienceNumRatings NUMBER,PRIMARY KEY(mid))",
                "CREATE TABLE movie_genres(mid VARCHAR(5),genre VARCHAR(25),PRIMARY KEY (mid, genre),FOREIGN KEY (mid) REFERENCES Movies(mid))",
                "CREATE TABLE Movie_location(mid VARCHAR(5),location VARCHAR(50),PRIMARY KEY (mid, location),FOREIGN KEY (mid) REFERENCES Movies(mid))",
                "CREATE TABLE Directors(did VARCHAR(40),dname VARCHAR(40),mid VARCHAR(5),PRIMARY KEY( did, mid ),FOREIGN KEY (mid) REFERENCES Movies(mid))",
                "CREATE TABLE Actors( aid VARCHAR(50),aname VARCHAR(50),mid VARCHAR(5),PRIMARY KEY( aid, mid),FOREIGN KEY (mid) REFERENCES Movies(mid))",
                "CREATE TABLE Tags(tid VARCHAR(5),value VARCHAR(50),PRIMARY KEY( tid ))",
                "CREATE TABLE movie_tags(mid VARCHAR(5),tid VARCHAR(5),tagWeight NUMBER,PRIMARY KEY (mid, tid),FOREIGN KEY (mid) REFERENCES Movies(mid))",
                "CREATE TABLE user_ratedmovies(userid VARCHAR(5),mid VARCHAR(5),rating NUMBER,YYYYMMdd DATE,PRIMARY KEY (userid, mid),FOREIGN KEY (mid) REFERENCES Movies(mid))"};
        Statement stmt = con.createStatement();
        for (int i = 0; i<slqs.length ; ++i ) {
            stmt.executeUpdate(slqs[i]);
        }
        stmt.close();
    }
}