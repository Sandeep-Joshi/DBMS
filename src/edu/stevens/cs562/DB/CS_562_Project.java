package edu.stevens.cs562.DB;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CS_562_Project {
	/**
	 * Class Variable declarations
	 */
	private static String USR;
	private static String PWD;
	private static String URL;
	private static String TBLNAME;
	private static String WHERE_CLAUSE = "";
	private static HashMap<String, String> columnDataType = new HashMap<String, String>();
	private static int noOfGroupingVars;
	private static ArrayList<String> projectedAttributesList = new ArrayList<String>();
	private static ArrayList<String> groupingAttributesList = new ArrayList<String>();
	private static ArrayList<String> groupingVariableNameList = new ArrayList<String>();
	private static HashMap<String, GroupingVariable> groupingVariableObjMap = new HashMap<String, GroupingVariable>();
	private static BufferedWriter writer;
	
	/**
	 * Main method
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			writer = new BufferedWriter(new FileWriter("src/edu/stevens/cs562/DB/QueryProgram.java"));
		
			fetchDBConnectionData();
			populateDataTypeMap(TBLNAME);
			generateDataFromUserInput();
			genJavaCode();
			
			System.out.println("done");
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			writer.close();
		}
	}
	
	/**
	 * Method used to fetch DB Connection data from file
	 * @throws FileNotFoundException
	 */
	public static void fetchDBConnectionData() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File("resources/dbConnectionInput.txt"));
		
		System.out.println("Enter DB Username: ");
		USR = scanner.nextLine();
		System.out.println("Enter DB Password: ");
		PWD = scanner.nextLine();
		System.out.println("Enter DB URL: ");
		URL = scanner.nextLine();
		System.out.println("Enter DB Table Name: ");
		TBLNAME = scanner.nextLine();
		
		scanner.close();
	}
	
	/**
	 * Method used to create a Map to table column names as keys & their respective data types as values
	 * @param tableName
	 */
	public static void populateDataTypeMap(String tableName) {
		String sql = "SELECT column_name, data_type FROM INFORMATION_SCHEMA.Columns WHERE table_name = '" + TBLNAME + "'";

		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Success loading Driver!");
		} catch(Exception e) {
			System.out.println("Fail loading Driver!");
			e.printStackTrace();
		}

		try {
			Connection conn = DriverManager.getConnection(URL, USR, PWD);
			System.out.println("Success connecting server!");
	
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
	
			while (rs.next()) 
			{
				String columnName = rs.getString("column_name");
				String dataType = rs.getString("data_type");
				String javaDataType = "";
		
				switch(dataType) {
					case "character varying":
					case "character":
					javaDataType = "String";
					break;
					case "integer":
					javaDataType = "int";
					break;
				}
				columnDataType.put(columnName, javaDataType);
			}
		} catch(SQLException e) {
			System.out.println("Connection URL or username or password errors!");
			e.printStackTrace();
		}
	}
	
	/**
	 * Method used to fetch user input from file and process the data
	 * @throws FileNotFoundException
	 */
	public static void generateDataFromUserInput() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File("resources/input.txt"));
		
		ArrayList<String> aggregateFunctionList = new ArrayList<String>();
		
		System.out.println("Input No. of Grouping Variables: ");
		noOfGroupingVars = Integer.parseInt(scanner.nextLine());
		
		System.out.println("Input Projected Attributes");
		fetchUserInput("Enter attribute", projectedAttributesList, scanner);
		
		System.out.println("Input Where Clause: ");
		WHERE_CLAUSE = scanner.nextLine();
		
		System.out.println("Input Grouping Attributes");
		fetchUserInput("Enter attribute", groupingAttributesList, scanner);
		
		System.out.println("Input Aggregate Functions");
		fetchUserInput("Enter aggregate", aggregateFunctionList, scanner);
		
		processAggregateFunctionList(aggregateFunctionList);
		
		populateGroupingVariablePredicate(scanner);
		
		scanner.close();
	}
	
	/**
	 * Method used to fetch multiple value input from user
	 * @param msg
	 * @param inputList
	 * @param scanner
	 */
	public static void fetchUserInput(String msg, ArrayList<String> inputList, Scanner scanner) {
		String input = "";
		int counter = 1;
		
		System.out.println(msg + " " + counter + " [\'Q\' or \'q\' to quit] :");
		input = scanner.nextLine();
		
		while(!input.equalsIgnoreCase("q")) {
			inputList.add(input);
			counter++;
			
			System.out.println(msg + " " + counter + " [\'Q\' or \'q\' to quit] :");
			input = scanner.nextLine();
		}
	}
	
	/**
	 * Method used to create the GroupingVariable & GroupingVariableAggregate objects
	 * @param inputList
	 */
	public static void processAggregateFunctionList(ArrayList<String> inputList) {
		String name = "", aggregateFunction = "", column = ""; 
		StringTokenizer st;
		Iterator<String> itr = inputList.iterator();
		
		while(itr.hasNext()) {
			int counter = 1;
			String tempStr = itr.next();
			st = new StringTokenizer(tempStr, "_");
			
			if(st.countTokens() == 2) {
				name = "0";
				counter++;
			}
			
			while(st.hasMoreElements()) {
				switch(counter) {
					case 1:
						name = (String)st.nextElement();
						break;
					case 2:
						aggregateFunction = (String)st.nextElement();
						break;
					case 3:
						column = (String)st.nextElement();
						break;
				}
				counter++;
			}
			
			if(!groupingVariableNameList.contains(name))
				groupingVariableNameList.add(name);
			
			GroupingVariableAggregate gvaObj = new GroupingVariableAggregate();
			gvaObj.setAggregateFunction(aggregateFunction);
			gvaObj.setColumn(column);
			
			if(groupingVariableObjMap.containsKey(name)) {
				groupingVariableObjMap.get(name).getAggregateList().add(gvaObj);
			} else {
				GroupingVariable gvObj = new GroupingVariable();
				gvObj.setName(name);
				gvObj.getAggregateList().add(gvaObj);
				
				groupingVariableObjMap.put(name, gvObj);
			}
		}
	}
	
	/**
	 * Method used to populate the condition property in GroupingVariableAggregate objects
	 * @param scanner
	 */
	public static void populateGroupingVariablePredicate(Scanner scanner) {
		String condition = "";
		Collections.sort(groupingVariableNameList);

		Iterator<String> itr = groupingVariableNameList.iterator();

		while(itr.hasNext()) {
			String groupingVariable = itr.next();
	
			if(groupingVariable.equals("0")) {
				condition = "true";
			} else {
				System.out.println("Input Predicate for Grouping Variable " + groupingVariable + " :");
				condition = parsePredicateStr(scanner.nextLine());
			}
	
			GroupingVariable gvObj = groupingVariableObjMap.get(groupingVariable);
	
			gvObj.setCondition(condition);
		}
	}
	
	/**
	 * Method used to format the grouping variable predicate into an equivalent condition in Java code
	 * @param inputStr
	 * @return
	 */
	public static String parsePredicateStr(String inputStr) {
		String AND_REGEX = "(( )*)(a|A)(n|N)(d|D)(( )*)";
		String[] andArray = inputStr.split(AND_REGEX);
		StringBuffer finalStr = new StringBuffer("");

		if(andArray.length > 1) {
			for(int i=0; i<andArray.length; i++) {
				String tempStr = "";
				tempStr = processORCondition(andArray[i]);
		
				if(i==0)
					finalStr.append(tempStr);
				else
					finalStr.append(" && " + tempStr);
			}
		} else {
			finalStr.append(processORCondition(andArray[0]));
		}

		return processPredicate(finalStr.toString());
	}

	/**
	 * Method acts as a helper method to parsePredicateStr(String) method
	 * @param inputStr
	 * @return
	 */
	private static String processORCondition(String inputStr) {
		String OR_REGEX = "(( )*)(o|O)(r|R)(( )*)";
		String BLANK_REGEX = "( )*";
		String[] orArray = inputStr.split(OR_REGEX);
		Pattern p = Pattern.compile(BLANK_REGEX);
		Matcher m;
		StringBuffer tempStr = new StringBuffer("");

		if(orArray.length > 1) {
			for(int i=0; i<orArray.length; i++) {
				m = p.matcher(orArray[i]);
				orArray[i] = m.replaceAll("");
			
				if(i==0)
					tempStr.append(orArray[i]);
				else
					tempStr.append(" || " + orArray[i]);
			}
		} else {
			m = p.matcher(orArray[0]);
			orArray[0] = m.replaceAll("");
			tempStr.append(orArray[0]);
		}

		return tempStr.toString();
	}
	
	/**
	 * Method acts as a helper method to parsePredicateStr(String) method
	 * @param inputStr
	 * @return
	 */
	public static String processPredicate(String inputStr) {
		StringBuffer sBuffer = new StringBuffer("");
		StringTokenizer st = new StringTokenizer(inputStr, " ");
		boolean isFirstElement = true;
		ArrayList<Character> charList = new ArrayList<Character>();
		charList.add('!');
		charList.add('(');

		while(st.hasMoreElements()) {
			String tmpStr = "";
			String element = (String)st.nextElement();
	
			if(!isFirstElement)
				sBuffer.append(" ");
	
			if(element.contains("!='") || element.contains("<>'")) {
				tmpStr = element.contains("!='")?element.replace("!='", ".equals(\""):element.replace("<>'", ".equals(\"");
				tmpStr = tmpStr.replace("'", "\")");
		
				for (int index = 0; index < tmpStr.length(); index++) {
					char aChar = tmpStr.charAt(index);
			
					if(!charList.contains(aChar)) {
						sBuffer.append("!");
						sBuffer.append(tmpStr.substring(index, tmpStr.length()));
						break;
					} else {
						sBuffer.append(aChar);
					}
				}
			} else if(element.contains("='")) {
				tmpStr = element.replace("='", ".equals(\"");
				tmpStr = tmpStr.replace("'", "\")");
				sBuffer.append(tmpStr);
			} else {
				sBuffer.append(element);
			}
	
			isFirstElement = false;
		}
		return sBuffer.toString();
	}
	
	/**
	 * Method that generates the code for QueryProgram.java file
	 * @throws IOException
	 */
	public static void genJavaCode() throws IOException {
		// writes the required libraries to the QueryProgram.java
		importFromFile("resources/libraries.txt");
		outputToFile(" ", true, writer);
		// create class and add member fields including hashmap for record details
		addMembersToClass();
		outputToFile(" ", true, writer);
		// writes the main function code 
		importFromFile("resources/mainFunctionCode.txt");
		outputToFile(" ", true, writer);
		// writes the code for creating MFStructure to the output program 
		createMFStruture();
		outputToFile(" ", true, writer);
		// writes the code for fetching records and calculating the required aggregates 
		addFetchDataFunction();
		outputToFile(" ", true, writer);
		// writes the code for generating the final output for the query
		addReportFunction();
		outputToFile(" }", true, writer);
	}
	
	/**
	 * Method adds class variables to QueryProgram.java file
	 * @throws IOException
	 */
	private static void addMembersToClass() throws IOException {
		outputToFile("public class QueryProgram {", true, writer);
		outputToFile("	protected String USR = \"" + USR + "\";" , true, writer);
		outputToFile("	protected String PWD = \"" + PWD + "\";", true, writer);
		outputToFile("	protected String URL = \"" + URL + "\";", true, writer);
		outputToFile("	protected String TBLNAME = \"" + TBLNAME + "\";", true, writer);
		outputToFile("	// Hashmap to store the calculated aggregate values for each group", true, writer);
		outputToFile("	private HashMap<RecordKey, MFStructure> recordDetails;", true, writer);	// for storing the key (RecordKey object) and value (MFStructure object) pair
	}

	/**
	 * Method to create code for the MF structure
	 * @throws IOException
	 */
	private static void createMFStruture() throws IOException {
		outputToFile("	// The class for implementing the MF structure", true, writer);
		outputToFile("	public class MFStructure {", true, writer);

		BufferedWriter writerTemp = new BufferedWriter(new FileWriter("resources/temp.txt"));
		outputToFile("		public void setDataValues(ResultSet rs) throws SQLException {", true, writerTemp);
		
		// create column variables
		Iterator<Entry<String, String>> it = columnDataType.entrySet().iterator();
		
		while(it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>)it.next();
			outputToFile("		private " + pairs.getValue() + " " + pairs.getKey() + ";", true, writer);
			outputToFile("			" + pairs.getKey() + " = " + "rs.get" + Character.toUpperCase(pairs.getValue().charAt(0)) 
					+ pairs.getValue().substring(1) + "(\"" + pairs.getKey() + "\");", true, writerTemp);
		}
		
		outputToFile("		}", true, writerTemp);
		writerTemp.close();
		String str;
		BufferedReader in = new BufferedReader(new FileReader("resources/temp.txt"));
		
		while((str = in.readLine()) != null)
			outputToFile(str, true, writer);
		
		in.close();
		
		// create aggregate function variables
		writerTemp = new BufferedWriter(new FileWriter("resources/temp.txt"));
		Iterator<Entry<String, GroupingVariable>> it1 = groupingVariableObjMap.entrySet().iterator();
		
		while(it1.hasNext()) {
			Map.Entry<String, GroupingVariable> pairs = (Map.Entry<String, GroupingVariable>)it1.next();
			String prefixAgg = (pairs.getKey()).equals("0") ? "" : pairs.getKey() + "_";
			
			for(GroupingVariableAggregate s: (pairs.getValue()).getAggregateList()) {
				switch(s.getAggregateFunction()) {
				case "sum":
					outputToFile("		protected int " + prefixAgg + "sum_" + s.getColumn() + " = 0;", true, writer);
					outputToFile("		public void " + prefixAgg + "sum(ResultSet rs) throws SQLException {", true, writerTemp);
					outputToFile("			setDataValues(rs);", true, writerTemp);
					outputToFile("			if(" + pairs.getValue().getCondition() + ") {", true, writerTemp);
					outputToFile("				" + prefixAgg + "sum_" + s.getColumn() + " += " + s.getColumn()+ ";", true, writerTemp);
					outputToFile("			}", true, writerTemp);
					outputToFile("		}", true, writerTemp);
					break;
				case "count":
					outputToFile("		protected int " + prefixAgg + "count_" + s.getColumn() + " = 0;", true, writer);
					outputToFile("		public void " + prefixAgg + "count(ResultSet rs) throws SQLException {", true, writerTemp);
					outputToFile("			setDataValues(rs);", true, writerTemp);
					outputToFile("			if(" + pairs.getValue().getCondition() + ") {", true, writerTemp);
					outputToFile("				" + prefixAgg + "count_" + s.getColumn() + " += 1;", true, writerTemp);
					outputToFile("			}", true, writerTemp);
					outputToFile("		}", true, writerTemp);
					break;
				case "avg":
					outputToFile("		protected int " + prefixAgg + "avg_" + s.getColumn() + " = 0;", true, writer);
					outputToFile("		protected int " + prefixAgg + "countAvg = 0;", true, writer);
					outputToFile("		public void " + prefixAgg + "avg(ResultSet rs) throws SQLException {", true, writerTemp);
					outputToFile("			setDataValues(rs);", true, writerTemp);
					outputToFile("			if(" + pairs.getValue().getCondition() + ") {", true, writerTemp);
					outputToFile("				" + prefixAgg + "avg_" + s.getColumn() + " += " + s.getColumn()+ ";", true, writerTemp);
					outputToFile("				" + prefixAgg + "countAvg += 1;", true, writerTemp);
					outputToFile("			}", true, writerTemp);
					outputToFile("		}", true, writerTemp);
					break;
				case "max":
					outputToFile("		protected int " + prefixAgg + "max_" + s.getColumn() + " = 0;", true, writer);
					outputToFile("		public void " + prefixAgg + "max(ResultSet rs) throws SQLException {", true, writerTemp);
					outputToFile("			setDataValues(rs);", true, writerTemp);
					outputToFile("			if(" + pairs.getValue().getCondition() + ") {", true, writerTemp);
					outputToFile("				if(" + prefixAgg + "max_" + s.getColumn() + " <" + s.getColumn() + ")", true, writerTemp);
					outputToFile("					" + prefixAgg + "max_" + s.getColumn() + " =" + s.getColumn() + ";", true, writerTemp);
					outputToFile("			}", true, writerTemp);
					outputToFile("		}", true, writerTemp);
					break;
				case "min":
					outputToFile("		protected int " + prefixAgg + "min_" + s.getColumn() + " = 0;", true, writer);
					outputToFile("		public void " + prefixAgg + "min(ResultSet rs) throws SQLException {", true, writerTemp);
					outputToFile("			setDataValues(rs);", true, writerTemp);
					outputToFile("			if(" + pairs.getValue().getCondition() + ") {", true, writerTemp);
					outputToFile("				if(" + prefixAgg + "min_" + s.getColumn() + " >" + s.getColumn() + ")", true, writerTemp);
					outputToFile("					" + prefixAgg + "min_" + s.getColumn() + " =" + s.getColumn() + ";", true, writerTemp);
					outputToFile("			}", true, writerTemp);
					outputToFile("		}", true, writerTemp);
					break;
				}
			}
		}
		
		writerTemp.close();
		in = new BufferedReader(new FileReader("resources/temp.txt"));
		
		while((str = in.readLine()) != null)
			outputToFile(str, true, writer);
		
		in.close();
		outputToFile(" 	}", true, writer);
	}

	/**
	 * Method to create code for fetching data and calculating the aggregate functions
	 * @throws IOException
	 */
	private static void addFetchDataFunction() throws IOException {
		outputToFile("	public void fetchData() {", true, writer);
		outputToFile("		try {", true, writer);
		outputToFile("// Connect to database and fetch the resultsets for each aggregate separately.", true, writer);
		outputToFile("			Connection conn = DriverManager.getConnection(this.URL, this.USR, this.PWD);", true, writer);
		outputToFile("			System.out.println(\"Success connecting server!\");", true, writer);
		outputToFile("			Statement stmt = conn.createStatement();", true, writer);

		BufferedWriter writerTemp = new BufferedWriter(new FileWriter("resources/temp.txt"));
		outputToFile("	// check if the key exists if not then create a new MFStructure and return it.", true, writerTemp);
		outputToFile("	public MFStructure generateContent(String gvKey, String aggFunc, RecordKey key, ResultSet rs) throws SQLException {", true, writerTemp);
		outputToFile("		MFStructure tempVal;", true, writerTemp);
		outputToFile("		if(recordDetails.containsKey(key))", true, writerTemp);
		outputToFile("			tempVal = recordDetails.get(key);", true, writerTemp);
		outputToFile("		else", true, writerTemp);
		outputToFile("			tempVal = new MFStructure();", true, writerTemp);
		outputToFile("		switch(gvKey + \"_\" + aggFunc ) {", true, writerTemp);
		
		int count = 0;
		
		for(String sGV : groupingVariableNameList) {
			if(WHERE_CLAUSE.length() == 0)
				outputToFile("			ResultSet rs" + count + "= stmt.executeQuery(\"SELECT * FROM \" + this.TBLNAME);", true, writer);
			else
				outputToFile("			ResultSet rs" + count + "= stmt.executeQuery(\"SELECT * FROM \" + this.TBLNAME " + "+ \" WHERE " + WHERE_CLAUSE + "\");", true, writer);
			outputToFile("			while (rs" + count + ".next()) {", true, writer);
			outputToFile("				RecordKey key = new RecordKey(", false, writer);
			
			StringBuffer strBuffer = new StringBuffer();
			
			for(String s: groupingAttributesList) {
				if((columnDataType.get(s)).equals("String"))
					strBuffer.append("rs" + count + ".getString(\"" + s + "\"), ");
				if((columnDataType.get(s)).equals("int"))
					strBuffer.append("rs" + count + ".getInt(\"" + s + "\"), ");		
			}
			
			strBuffer.deleteCharAt(strBuffer.length() - 1);
			strBuffer.deleteCharAt(strBuffer.length() - 1);
			strBuffer.append(");");
			outputToFile(strBuffer.toString(), true, writer);
			outputToFile("				MFStructure content;", true, writer);
			boolean hasAvg = false;
			String prefixV = "";
			String avgCol = "";
			
			for(GroupingVariableAggregate s: (groupingVariableObjMap.get(sGV)).getAggregateList()) {
				prefixV = sGV.equals("0") ? "" : sGV + "_";
				outputToFile("				content = this.generateContent(\"" + sGV + "\", \"" + s.getAggregateFunction() + "\", key, rs" + count + ");", true, writer);
				outputToFile("				recordDetails.put(key, content);", true, writer);	
				outputToFile("			case(\"" + sGV + "_" + s.getAggregateFunction() + "\"): ", true, writerTemp);
				outputToFile("				tempVal." + prefixV + s.getAggregateFunction() + "(rs);", true, writerTemp);
				outputToFile("			break;", true, writerTemp);
				if((s.getAggregateFunction()).equals("avg")) {
					hasAvg = true;
					avgCol = s.getColumn();
				}
			}
			
			outputToFile("			}", true, writer);
			
			if(hasAvg) {
				outputToFile("			Iterator<Entry<RecordKey,MFStructure>> it" + count + " = recordDetails.entrySet().iterator();", true, writer);
				outputToFile("			while(it" + count + ".hasNext()) {", true, writer);
				outputToFile("				Map.Entry<RecordKey,MFStructure> pairs = (Map.Entry<RecordKey,MFStructure>)it" + count + ".next();", true, writer);
				outputToFile("				(pairs.getValue())." + prefixV + "avg_" + avgCol + " = calAvg((pairs.getValue())." + prefixV + "avg_" + avgCol + ", (pairs.getValue())." + prefixV + "countAvg);", true, writer);
				outputToFile("			}", true, writer);
			}
			count++;
		}

		outputToFile("		}", true, writerTemp);
		outputToFile("		return tempVal;", true, writerTemp);
		outputToFile("	}", true, writerTemp);
		writerTemp.close();

		outputToFile("		} catch(SQLException e) {", true, writer);
		outputToFile("			System.out.println(\"Connection URL or username or password errors!\");", true, writer);
		outputToFile("			e.printStackTrace();", true, writer);
		outputToFile("		}", true, writer);
		outputToFile("	}", true, writer);
		
		String str;
		BufferedReader in = new BufferedReader(new FileReader("resources/temp.txt"));
		
		while((str = in.readLine()) != null)
			outputToFile(str, true, writer);
		
		in.close();
		
		outputToFile("	// the class for the key of the hashmap for the record details", true, writer);
		outputToFile("	class RecordKey {", true, writer);
		
		for(String s: groupingAttributesList) {
			outputToFile("		public " + columnDataType.get(s) + " " + s + ";", true, writer);
		}
		
		outputToFile("		public RecordKey(", false, writer);
		StringBuffer temp = new StringBuffer();
		
		for(String s: groupingAttributesList) {
			temp.append(columnDataType.get(s) + " " + s + ",");
		}
		
		temp.deleteCharAt(temp.length() - 1);
		temp.append(") {");
		outputToFile(temp.toString(), true, writer);
		
		for(String s: groupingAttributesList) {
			outputToFile("			this." + s + " = " + s + ";", true, writer);
		}
		
		outputToFile("		}", true, writer);
		outputToFile("		public boolean equals(Object obj) {", true, writer);
		outputToFile("			if (!(obj instanceof RecordKey)) {", true, writer);	
		outputToFile("				return false;", true, writer);        
		outputToFile("			} else {", true, writer);    
		outputToFile("				RecordKey that = (RecordKey)obj;", true, writer);
		outputToFile("				return ", false, writer);
		temp.setLength(0);
		
		for(String s: groupingAttributesList) {
			if(columnDataType.get(s).equals("String"))
				temp.append("this." + s + ".equals(that." + s + ") && ");
			if(columnDataType.get(s).equals("int"))
				temp.append("this." + s + " == that." + s + " && ");
		}
		
		temp.delete(temp.length() - 3, temp.length());
		temp.append(";");
		outputToFile(temp.toString(), true, writer);
		outputToFile("			}", true, writer);
		outputToFile("		}", true, writer);
		outputToFile("		public int hashCode() {", true, writer);
		outputToFile("			int hash = " + groupingAttributesList.size() + ";", true, writer);
		
		for(String s: groupingAttributesList) {
			if(columnDataType.get(s).equals("String"))
				outputToFile("			hash = 31 * hash + (this." + s + " != null ? this." + s + ".hashCode() : 0);", true, writer);
			if(columnDataType.get(s).equals("int"))
				outputToFile("			hash = 31 * hash + (this." + s + ");", true, writer);
		}
		
		outputToFile("			return hash;", true, writer);	
		outputToFile("		}", true, writer);
		outputToFile("	}", true, writer);
	}

	/**
	 * Method to create code for generating the output result for the query program
	 * @throws IOException
	 */
	private static void addReportFunction() throws IOException {
		// Function to generate report 
		outputToFile("	// function to generate the actual output table, the result.", true, writer);
		outputToFile("	public int calAvg(int sum, int count) {", true, writer);
		outputToFile("		if(count == 0)", true, writer);
		outputToFile("			return 0;", true, writer);
		outputToFile("		return ((int)(sum/count));", true, writer);
		outputToFile("	}", true, writer);
		outputToFile("", true, writer);
		outputToFile ("	public void generateReport() {", true, writer);
			// Use the Iterator to iterate through the HashMap entries
		outputToFile ("		Iterator<Entry<RecordKey, MFStructure>> it = recordDetails.entrySet().iterator();", true, writer);
		outputToFile ("		System.out.println();", true, writer);
		outputToFile ("		System.out.println();", true, writer);
		
		StringBuffer tempStrFormat1 = new StringBuffer();
		StringBuffer tempStrFormat2 = new StringBuffer();
		StringBuffer tempStrColumn = new StringBuffer();
		StringBuffer tempStrValues = new StringBuffer();
		boolean firstValueCol = true;
		
		for(String s : projectedAttributesList) {
			int formatLen = s.length() + 5;
			tempStrFormat1.append("%-" + formatLen + "s ");
			tempStrColumn.append("\"" + s.toUpperCase() + "\",");
			
			if(groupingAttributesList.contains(s)) {
				tempStrFormat2.append("%-" + formatLen + "s ");
				tempStrValues.append("(pairs.getKey())." + s + ",");
			} else {
				if(firstValueCol) {
					tempStrFormat2.append("%" + s.length() + "s ");
					firstValueCol = false;
				} else {
						tempStrFormat2.append("%" + formatLen + "s ");
				}
				tempStrValues.append("Integer.toString((pairs.getValue())." + s + "),");
			}
		}
		
		tempStrFormat1.append("\\n");
		tempStrFormat2.append("\\n");
		tempStrColumn.deleteCharAt(tempStrColumn.length() - 1);
		tempStrValues.deleteCharAt(tempStrValues.length() - 1);
		outputToFile("		System.out.printf(\"" + tempStrFormat1.toString() + "\", " + tempStrColumn.toString() + ");", true, writer);
		outputToFile("// Get key, value pairs and display the projected attribute for each key", true, writer);
		outputToFile("		while(it.hasNext()) {", true, writer);	
		outputToFile("			Map.Entry<RecordKey, MFStructure> pairs = (Map.Entry<RecordKey, MFStructure>)it.next();", true, writer);
		outputToFile("			System.out.printf(\"" + tempStrFormat2.toString() + "\", " + tempStrValues.toString() + ");", true, writer);
		outputToFile("		}", true, writer);
		outputToFile("	}", true, writer);
	}

	/**
	 * Method used read data from file
	 * @param filename
	 * @throws IOException
	 */
	private static void importFromFile(String filename) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(filename));
		String str;
		
		while((str = in.readLine()) != null) 
			outputToFile(str, true, writer);
		
		in.close();
	}
	
	/**
	 * Method used to write data to file
	 * @param str
	 * @param lineFlag
	 * @param writer
	 * @throws IOException
	 */
	private static void outputToFile(String str, boolean lineFlag, BufferedWriter writer) throws IOException {
		if(lineFlag) {
			writer.write(str);
			writer.newLine();
		} else {
			writer.write(str);
		}
	}
}
