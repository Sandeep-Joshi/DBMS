package edu.stevens.cs562.DB;

import java.io.Console;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
 
public class QueryProgram {
	protected String USR = "postgres";
	protected String PWD = "cs561dbms";
	protected String URL = "jdbc:postgresql://localhost:5432/CS561";
	protected String TBLNAME = "sales";
	// Hashmap to store the calculated aggregate values for each group
	private HashMap<RecordKey, MFStructure> recordDetails;
 
	public QueryProgram() {
		recordDetails = new HashMap<RecordKey, MFStructure>();
	}

	public static void main(String[] args)	{
		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Success loading Driver!");
		} catch(Exception e) {
			System.out.println("Fail loading Driver!");
			e.printStackTrace();
		}
			
		QueryProgram sales = new QueryProgram();

	// Parse the arguments passed from command line interface
			
		if (args.length == 1) {
			sales.TBLNAME = args[1];
		} else if(args.length == 2) {
			sales.TBLNAME = args[0];
			String[] tempArray = args[1].split("@");
			sales.USR = tempArray[0];
			sales.URL = "jdbc:postgresql://" + tempArray[1];
			Console c = System.console();
			if(c == null) {
				System.err.println("No console.");
				System.exit(1);
			}
			char[] pass = c.readPassword("Enter password: ");
			sales.PWD = new String(pass);
		}
		
		sales.fetchData();	
		sales.generateReport();		
	}
 
	// The class for implementing the MF structure
	public class MFStructure {
		private int quant;
		private String state;
		private int month;
		private int year;
		private int day;
		private String prod;
		private String cust;
		public void setDataValues(ResultSet rs) throws SQLException {
			quant = rs.getInt("quant");
			state = rs.getString("state");
			month = rs.getInt("month");
			year = rs.getInt("year");
			day = rs.getInt("day");
			prod = rs.getString("prod");
			cust = rs.getString("cust");
		}
		protected int avg_quant = 0;
		protected int countAvg = 0;
		protected int Y_avg_quant = 0;
		protected int Y_countAvg = 0;
		protected int X_avg_quant = 0;
		protected int X_countAvg = 0;
		protected int Z_avg_month = 0;
		protected int Z_countAvg = 0;
		public void avg(ResultSet rs) throws SQLException {
			setDataValues(rs);
			if(true) {
				avg_quant += quant;
				countAvg += 1;
			}
		}
		public void Y_avg(ResultSet rs) throws SQLException {
			setDataValues(rs);
			if(state.equals("NY") && X_avg_quant>quant) {
				Y_avg_quant += quant;
				Y_countAvg += 1;
			}
		}
		public void X_avg(ResultSet rs) throws SQLException {
			setDataValues(rs);
			if(state.equals("NJ") && ((year>=1990) && (year<=1995)) && avg_quant>quant) {
				X_avg_quant += quant;
				X_countAvg += 1;
			}
		}
		public void Z_avg(ResultSet rs) throws SQLException {
			setDataValues(rs);
			if(state.equals("CT") && ((year>=1990) && (year<=1995)) && Y_avg_quant>X_avg_quant) {
				Z_avg_month += month;
				Z_countAvg += 1;
			}
		}
 	}
 
	public void fetchData() {
		try {
// Connect to database and fetch the resultsets for each aggregate separately.
			Connection conn = DriverManager.getConnection(this.URL, this.USR, this.PWD);
			System.out.println("Success connecting server!");
			Statement stmt = conn.createStatement();
			ResultSet rs0= stmt.executeQuery("SELECT * FROM " + this.TBLNAME);
			while (rs0.next()) {
				RecordKey key = new RecordKey(rs0.getString("cust"), rs0.getString("prod"), rs0.getInt("year"));
				MFStructure content;
				content = this.generateContent("0", "avg", key, rs0);
				recordDetails.put(key, content);
			}
			Iterator<Entry<RecordKey,MFStructure>> it0 = recordDetails.entrySet().iterator();
			while(it0.hasNext()) {
				Map.Entry<RecordKey,MFStructure> pairs = (Map.Entry<RecordKey,MFStructure>)it0.next();
				(pairs.getValue()).avg_quant = calAvg((pairs.getValue()).avg_quant, (pairs.getValue()).countAvg);
			}
			ResultSet rs1= stmt.executeQuery("SELECT * FROM " + this.TBLNAME);
			while (rs1.next()) {
				RecordKey key = new RecordKey(rs1.getString("cust"), rs1.getString("prod"), rs1.getInt("year"));
				MFStructure content;
				content = this.generateContent("X", "avg", key, rs1);
				recordDetails.put(key, content);
			}
			Iterator<Entry<RecordKey,MFStructure>> it1 = recordDetails.entrySet().iterator();
			while(it1.hasNext()) {
				Map.Entry<RecordKey,MFStructure> pairs = (Map.Entry<RecordKey,MFStructure>)it1.next();
				(pairs.getValue()).X_avg_quant = calAvg((pairs.getValue()).X_avg_quant, (pairs.getValue()).X_countAvg);
			}
			ResultSet rs2= stmt.executeQuery("SELECT * FROM " + this.TBLNAME);
			while (rs2.next()) {
				RecordKey key = new RecordKey(rs2.getString("cust"), rs2.getString("prod"), rs2.getInt("year"));
				MFStructure content;
				content = this.generateContent("Y", "avg", key, rs2);
				recordDetails.put(key, content);
			}
			Iterator<Entry<RecordKey,MFStructure>> it2 = recordDetails.entrySet().iterator();
			while(it2.hasNext()) {
				Map.Entry<RecordKey,MFStructure> pairs = (Map.Entry<RecordKey,MFStructure>)it2.next();
				(pairs.getValue()).Y_avg_quant = calAvg((pairs.getValue()).Y_avg_quant, (pairs.getValue()).Y_countAvg);
			}
			ResultSet rs3= stmt.executeQuery("SELECT * FROM " + this.TBLNAME);
			while (rs3.next()) {
				RecordKey key = new RecordKey(rs3.getString("cust"), rs3.getString("prod"), rs3.getInt("year"));
				MFStructure content;
				content = this.generateContent("Z", "avg", key, rs3);
				recordDetails.put(key, content);
			}
			Iterator<Entry<RecordKey,MFStructure>> it3 = recordDetails.entrySet().iterator();
			while(it3.hasNext()) {
				Map.Entry<RecordKey,MFStructure> pairs = (Map.Entry<RecordKey,MFStructure>)it3.next();
				(pairs.getValue()).Z_avg_month = calAvg((pairs.getValue()).Z_avg_month, (pairs.getValue()).Z_countAvg);
			}
		} catch(SQLException e) {
			System.out.println("Connection URL or username or password errors!");
			e.printStackTrace();
		}
	}
	// check if the key exists if not then create a new MFStructure and return it.
	public MFStructure generateContent(String gvKey, String aggFunc, RecordKey key, ResultSet rs) throws SQLException {
		MFStructure tempVal;
		if(recordDetails.containsKey(key))
			tempVal = recordDetails.get(key);
		else
			tempVal = new MFStructure();
		switch(gvKey + "_" + aggFunc ) {
			case("0_avg"): 
				tempVal.avg(rs);
			break;
			case("X_avg"): 
				tempVal.X_avg(rs);
			break;
			case("Y_avg"): 
				tempVal.Y_avg(rs);
			break;
			case("Z_avg"): 
				tempVal.Z_avg(rs);
			break;
		}
		return tempVal;
	}
	// the class for the key of the hashmap for the record details
	class RecordKey {
		public String cust;
		public String prod;
		public int year;
		public RecordKey(String cust,String prod,int year) {
			this.cust = cust;
			this.prod = prod;
			this.year = year;
		}
		public boolean equals(Object obj) {
			if (!(obj instanceof RecordKey)) {
				return false;
			} else {
				RecordKey that = (RecordKey)obj;
				return this.cust.equals(that.cust) && this.prod.equals(that.prod) && this.year == that.year ;
			}
		}
		public int hashCode() {
			int hash = 3;
			hash = 31 * hash + (this.cust != null ? this.cust.hashCode() : 0);
			hash = 31 * hash + (this.prod != null ? this.prod.hashCode() : 0);
			hash = 31 * hash + (this.year);
			return hash;
		}
	}
 
	// function to generate the actual output table, the result.
	public int calAvg(int sum, int count) {
		if(count == 0)
			return 0;
		return ((int)(sum/count));
	}

	public void generateReport() {
		Iterator<Entry<RecordKey, MFStructure>> it = recordDetails.entrySet().iterator();
		System.out.println();
		System.out.println();
		System.out.printf("%-9s %-9s %-9s %-16s %-14s %-16s %-16s \n", "CUST","PROD","YEAR","Z_AVG_MONTH","AVG_QUANT","X_AVG_QUANT","Y_AVG_QUANT");
// Get key, value pairs and display the projected attribute for each key
		while(it.hasNext()) {
			Map.Entry<RecordKey, MFStructure> pairs = (Map.Entry<RecordKey, MFStructure>)it.next();
			System.out.printf("%-9s %-9s %-9s %11s %14s %16s %16s \n", (pairs.getKey()).cust,(pairs.getKey()).prod,(pairs.getKey()).year,Integer.toString((pairs.getValue()).Z_avg_month),Integer.toString((pairs.getValue()).avg_quant),Integer.toString((pairs.getValue()).X_avg_quant),Integer.toString((pairs.getValue()).Y_avg_quant));
		}
	}
 }
