package mlb;
/**
 * @author Roman Yasinovskyy
 */
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.ResultSet;

public class DatabaseWriter {
    public final String SQLITEDBPATH = "jdbc:sqlite:data/";
    /**
     * @param filename (JSON file)
     * @return League
     */
    public ArrayList<Team> readTeamFromJson(String filename) {
        ArrayList<Team> league = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jsonParser;
        
        try {
            jsonParser = jsonFactory.createParser(new File(filename));
            jsonParser.nextToken();
            while (jsonParser.nextToken() == JsonToken.START_OBJECT) {
                Team team = mapper.readValue(jsonParser, Team.class);
                String img = "images/mlb_logo_"+team.getAbbreviation().toLowerCase()+".jpg";
                //System.out.println(img);
                team.setLogo(readLogoFile(img));
                league.add(team);
            }
            jsonParser.close();
        } catch (IOException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return league;
    }
    /**
     * @param filename (TXT file)
     * @return Addresses
     */
    public ArrayList<Address> readAddressFromTxt(String filename) {
        ArrayList<Address> addressBook = new ArrayList<>();
        try {
            Scanner fs = new Scanner(new File(filename));
            while (fs.hasNextLine()) {
                // TODO: Parse each line into an object of type Address and add it to the ArrayList                
                String[] array = fs.nextLine().split("\t");                
                // Address(String _team, String _site, String _street, String _city, String _state, String _zip, String _phone, String _url)                
                //System.out.println(array[1]);                
                Address addrInst = new Address(array[0],array[1],array[2],array[3],array[4],array[5],array[6],array[7]);                
                //System.out.println(addrInst);                
                addressBook.add(addrInst);                
            }
        } catch (IOException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return addressBook;
    }
    public ArrayList<Player> readPlayerFromCsv(String filename) {
        ArrayList<Player> roster = new ArrayList<>();
        CSVReader reader = null;        
        // TODO: Read the CSV file, create an object of type Player from each line and add it to the ArrayList       
        
        try {            
            reader = new CSVReader(new FileReader("data/mlb_players.csv"),',','"',1);            
            String[] players = null;
            while((players = reader.readNext())!=null){            	
                // Player(String _id, String _name, String _team, String _position)
                Player eachPlayer = new Player(players[0],players[1],players[4],players[2]);                
                roster.add(eachPlayer);
            }
        }
        catch(Exception e1)
        {
            e1.printStackTrace();
        }
        
        return roster;   
    }
    /**
     * Create tables cities and teams
     * @param db_filename
     * @throws SQLException 
     */
    public void createTables(String db_filename) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        Statement statement = db_connection.createStatement();
        
        statement.executeUpdate("DROP TABLE IF EXISTS team;");
        statement.executeUpdate("CREATE TABLE team ("
                            + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                            + "id TEXT NOT NULL,"
                            + "abbr TEXT NOT NULL,"
                            + "name TEXT NOT NULL,"
                            + "conference TEXT NOT NULL,"
                            + "division TEXT NOT NULL,"
                            + "logo BLOB);");
        
        statement.execute("PRAGMA foreign_keys = ON;");
        
        statement.executeUpdate("DROP TABLE IF EXISTS player;");
        statement.executeUpdate("CREATE TABLE player ("
                            + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                            + "id TEXT NOT NULL,"
                            + "name TEXT NOT NULL,"
                            + "team TEXT NOT NULL,"
                            + "position TEXT NOT NULL,"
                            + "FOREIGN KEY (team) REFERENCES team(idpk));");

        statement.executeUpdate("DROP TABLE IF EXISTS address;");
        statement.executeUpdate("CREATE TABLE address ("
                            + "idpk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                            + "team TEXT NOT NULL,"
                            + "site TEXT NOT NULL,"
                            + "street TEXT NOT NULL,"
                            + "city TEXT NOT NULL,"
                            + "state TEXT NOT NULL,"
                            + "zip TEXT NOT NULL,"
                            + "phone TEXT NOT NULL,"
                            + "url TEXT NOT NULL,"
                            + "FOREIGN KEY (team) REFERENCES team(idpk));");
        db_connection.close();
    }
   /**
     * Read the file and returns the byte array
     * @param filename
     * @return the bytes of the file
     */
    private byte[] readLogoFile(String filename) {
        ByteArrayOutputStream byteArrOutStream = null;
        try {
            File fileIn = new File(filename);
            FileInputStream fileInStream = new FileInputStream(fileIn);
            byte[] buffer = new byte[1024];
            byteArrOutStream = new ByteArrayOutputStream();
            for (int len; (len = fileInStream.read(buffer)) != -1;) {
                byteArrOutStream.write(buffer, 0, len);
            }
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
        } catch (IOException e2) {
            System.err.println(e2.getMessage());
        }
        return byteArrOutStream != null ? byteArrOutStream.toByteArray() : null;
    }
    /**
     * @param db_filename
     * @param league 
     * @throws java.sql.SQLException 
     */
    public void writeTeamTable(String db_filename, ArrayList<Team> league) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        // TODO: Write an SQL statement to insert a new team into a table        
        String sql = "INSERT INTO team (id, abbr, name, conference, division,logo) VALUES (?,?,?,?,?,?)";
        for (Team team: league) {
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            // TODO: match parameters of the SQL statement and team id, abbreviation, name, conference, division, and logo            
            statement_prepared.setString(1, team.getId());
            statement_prepared.setString(2, team.getAbbreviation());
            statement_prepared.setString(3, team.getName());
            statement_prepared.setString(4, team.getConference());
            statement_prepared.setString(5, team.getDivision());
            statement_prepared.setBytes(6, team.getLogo());
            statement_prepared.executeUpdate();         
            System.out.println(team.getName());
        }
        
        db_connection.close();
    }
    /**
     * @param db_filename 
     * @param addressBook 
     * @throws java.sql.SQLException 
     */
    public void writeAddressTable(String db_filename, ArrayList<Address> addressBook) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        for (Address address: addressBook) {
            // TODO: Write an SQL statement to insert a new address into a table
            String sql = "INSERT INTO address (team,site,street,city,state,zip,phone,url) VALUES (?,?,?,?,?,?,?,?)";
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            // TODO: match parameters of the SQL statement and address site, street, city, state, zip, phone, and url
            
            
            //Dealing with foreign key by doing sub-query
            Statement stmt; // = null;
            stmt = db_connection.createStatement();           
            String sub_query;
            sub_query = "Select idpk from team where name = '" + address.getTeam() +"';" ;
            ResultSet result;
            result = stmt.executeQuery(sub_query);            
            int team_id = result.getInt("idpk"); //result.getString("idpk")
            
            
            statement_prepared.setInt(1, team_id); 
            statement_prepared.setString(2, address.getSite());
            statement_prepared.setString(3, address.getStreet());
            statement_prepared.setString(4, address.getCity());
            statement_prepared.setString(5, address.getState());
            statement_prepared.setString(6, address.getZip());
            statement_prepared.setString(7, address.getPhone());
            statement_prepared.setString(8, address.getUrl());
            statement_prepared.executeUpdate();
            System.out.println(address.getTeam());
        }
        
        db_connection.close();
    }
    /**
     * @param db_filename 
     * @param roster 
     * @throws java.sql.SQLException 
     */
    public void writePlayerTable(String db_filename, ArrayList<Player> roster) throws SQLException {
        Connection db_connection = DriverManager.getConnection(SQLITEDBPATH + db_filename);
        for (Player player: roster) {
            // TODO: Write an SQL statement to insert a new player into a table
            String sql = "INSERT INTO player (id,name,team,position) VALUES (?,?,?,?)";
            PreparedStatement statement_prepared = db_connection.prepareStatement(sql);
            // TODO: match parameters of the SQL statement and player id, name, position
            
            
            //Dealing with foreign key by doing sub-query
            Statement stmt = null;
            stmt = db_connection.createStatement();           
            String sub_query;
            sub_query = "Select idpk from team where name = '" + player.getTeam()+"';";
            ResultSet result = stmt.executeQuery(sub_query);
            int team_id = result.getInt("idpk"); //result.getString("idpk")
            
            
            statement_prepared.setString(1, player.getId());
            statement_prepared.setString(2, player.getName());            
            statement_prepared.setInt(3, team_id);  
            statement_prepared.setString(4, player.getPosition());
            statement_prepared.executeUpdate();
            System.out.println(player.getName());
        }
        
        db_connection.close();
    }
}
