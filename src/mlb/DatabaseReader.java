package mlb;
/**
 * @author Roman Yasinovskyy
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;

public class DatabaseReader {
    private Connection db_connection;
    private final String SQLITEDBPATH = "jdbc:sqlite:data/mlb.sqlite";
    
    public DatabaseReader() { }
    /**
     * Connect to a database (file)
     */
    public void connect() {
        try {
            this.db_connection = DriverManager.getConnection(SQLITEDBPATH);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReaderGUI.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    /**
     * Disconnect from a database (file)
     */
    public void disconnect() {
        try {
            this.db_connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReaderGUI.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    /**
     * Populate the list of divisions
     * @param divisions
     */
    public void getDivisions(ArrayList<String> divisions) {
        Statement stat;
        ResultSet results;
        
        this.connect();
        try {
            stat = this.db_connection.createStatement();
            // TODO: Write an SQL statement to retrieve a league (conference) and a division 
            // TODO: Add all 6 combinations to the ArrayList divisions
            
            ArrayList<String> conferences = new ArrayList<String>();          
            String sql = "select conference from team group by conference;";
            ResultSet result;
            result = stat.executeQuery(sql);            
            
            while (result.next()) {
                String conf = result.getString("conference");               
                conferences.add(conf);
            }            
            
            Iterator itr = conferences.iterator();  
            while(itr.hasNext()){                
                String sql2 = "select conference,division from team where conference = '"+itr.next()+"' group by division;";
                ResultSet result2 = stat.executeQuery(sql2);                
                while (result2.next()) {                    
                    divisions.add(result2.getString("conference")+" | "+result2.getString("division"));
                }
            }           
            
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            this.disconnect();
        }
    }
    /**
     * Read all teams from the database
     * @param confDiv
     * @param teams
     */
    public void getTeams(String confDiv, ArrayList<String> teams) {
        Statement stat;
        ResultSet results;
        String conference = confDiv.split(" | ")[0];
        String division = confDiv.split(" | ")[2];
        
        this.connect();
        try {
            stat = this.db_connection.createStatement();
            // TODO: Write an SQL statement to retrieve a teams from a specific division  
            // TODO: Add all 5 teams to the ArrayList teams
            
            String sql = "select name from team where conference = '"+conference+"' and division = '"+division+"';";
            results = stat.executeQuery(sql);            
            while (results.next()) {
                String team_name = results.getString("name");               
                teams.add(team_name);
            }            
            results.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            this.disconnect();
        }
    }
    /**
     * @param teamName
     * @return Team info
     */
    public Team getTeamInfo(String teamName) {
        Team team = null;
        // TODO: Retrieve team info (roster, address, and logo) from the database
        
        //String sql3 = "select team,site,street,city,state,zip,phone,url from address where team = '"+team_idpk+"';";
        //String sql = "select idpk from team where name = '"+teamName+"';";
        
        
        Statement stat;
        ResultSet results;  
        
        //team = new Team();
        ArrayList<Player> roster = new ArrayList<Player>();
        
        this.connect();
        try {
            String sql_g = "select id,abbr,conference, division from team where name = '"+teamName+"';";        
            Statement stat_g;
            ResultSet result_g;
            stat_g = this.db_connection.createStatement();          
            result_g = stat_g.executeQuery(sql_g);
            String conf_g = result_g.getString("conference");
            String div_g = result_g.getString("division");
            String id_g = result_g.getString("id");
            String abbr_g = result_g.getString("abbr");
            
            //public Team(String _id, String _abbr, String _name, String _conf, String _div) {
            team = new Team(id_g,abbr_g,teamName,conf_g,div_g);
            
            
            
            stat = this.db_connection.createStatement();          
            String sql = "select idpk from team where name = '"+teamName+"';";
            results = stat.executeQuery(sql);
            int team_idpk = results.getInt("idpk");            
            
            Statement stat2 = this.db_connection.createStatement();
            ResultSet results2;
            String sql2;
            sql2 = "select id,name,team,position from player where team = '"+team_idpk+"';";
            results2 = stat2.executeQuery(sql2);
            while (results2.next()) {
                String player_id = results2.getString("id");
                String player_name = results2.getString("name");
                String player_team = results2.getString("team");
                String player_position = results2.getString("position");
                Player player1 = new Player(player_id,player_name,player_team,player_position);                
                roster.add(player1);               
            }
            team.setRoster(roster);
            //System.out.println("roster size: " + roster.size());
            
            
            //Retrieving team address from the DB
            Statement stat3;
            stat3 = this.db_connection.createStatement(); 
            ResultSet results3;
            String sql3 = "select team,site,street,city,state,zip,phone,url from address where team = '"+team_idpk+"';";
            results3 = stat.executeQuery(sql3);            
            
            String a_team = results3.getString("team");            
            String a_site = results3.getString("site");
            String a_street = results3.getString("street");
            String a_city = results3.getString("city");
            String a_state = results3.getString("state");
            String a_zip = results3.getString("zip");
            String a_phone = results3.getString("phone");
            String a_url = results3.getString("url");
            
            Address addr = new Address(a_team,a_site,a_street,a_city,a_state,a_zip,a_phone,a_url);
            team.setAddress(addr);
            
            
            //Retrieving Logo from DB
            Statement stat4;
            stat4 = this.db_connection.createStatement();     
            ResultSet results4;
            String sql4 = "select logo from team where name = '"+teamName+"';";
            results4 = stat.executeQuery(sql4); 
            byte[] logobytes = results4.getBytes("logo");
            team.setLogo(logobytes);           
                      
            results.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            this.disconnect();
        }        
        
        return team;
    }
}
