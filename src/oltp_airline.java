import java.security.SecureRandom;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class oltp_airline implements Runnable{

	private Thread t;
	private String threadName;
	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static SecureRandom rnd = new SecureRandom();
	private static Statement st = null;
	Connection conn = null;
    
	oltp_airline( String name){
	       threadName = name;
	       System.out.println("Creating " +  threadName );
	}
	
	public static void main(String[] args) {
		
		//Call cleanup method to clean up tables created by previous run
		cleanup();
		String temp=args[0];
		String temp2[]=new String[2];
		temp2=temp.split("=");
		System.out.println(temp2[0] +"-----"+temp2[1]);
		int nthread = Integer.parseInt(temp2[1]);
		System.out.println(nthread);
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		
		//Run 20 thread each 3 seconds
		Runnable periodicTask = new Runnable() {
		    public void run() {
		        // Invoke method(s) to do the work
		    	for(int i=0; i<nthread; i++){
					oltp_airline T = new oltp_airline( "Thread"+i);
				    T.start();
				}
		    }
		};
		
		//After each 3 seconds : 20 Threads run
		executor.scheduleAtFixedRate(periodicTask, 0, 10, TimeUnit.SECONDS);
		
		//testing();
	}
	
	//FUnction to test
	public static void testing(){

		// TODO Auto-generated method stub
		
		
		// Getting connection from createConnection Class
		Connection conn = createConnection.createStatement();				
				
				//create a statement for this connection
				try {
					//st = conn.createStatement();
					
					//Get random customerID
					int custID = getCustID(conn);
					//System.out.println(custID);
					//Get random seat class - Business/Economy/FirstClass
					char ticketType = getTicketType(conn);
					//System.out.println(ticketType);
					//Get random TripDate from the DB
					Date TripDate = getTripDate(conn);
					//System.out.println(TripDate);
					//Get random tripType - one way or round trip
					String tripType = getTripType();
					//System.out.println(tripType);
					//Get random reservation ID
					String reserId = randomString(10);
					//System.out.println("Reservation ID : "+reserId);
					//Get random payment ID
					String payId = randomString(10);
					//System.out.println("Payment ID : "+payId);
					//Get random trip ID
					String tripID = randomString(10);
					//System.out.println("Trip ID : "+tripID);
					//Get random tripType - one way or round trip
					String payType = getPaymentType();
					//System.out.println(payType);
					//Get origin and destination
					String[] loc = new String[2];
					loc = getOrgDest(conn);
					//System.out.println("Origin is : "+loc[0]);
					//System.out.println("Destination is : "+loc[1]);
					//System.out.println("Connectivity is : "+loc[2]);
					//System.out.println("Stops is : "+loc[3]);
					
					//Get the connectivity
					int connectivity = Integer.parseInt(loc[2]);
					//Get the stops
					int stops = Integer.parseInt(loc[3]);
					
					int count = checkavailability(conn, TripDate, loc[0], loc[1]);
					System.out.println("count is : "+count);
					
					//Direct Flights : Without transfers
					if(count>0 & stops == 0){ //Seats are there on that day
						oneTransaction(conn, custID, ticketType, TripDate, tripType, reserId, payId, tripID, payType, loc);
					}
					//One Stop flight : One transfer
					if(count>0 & stops == 1){
						//System.out.println("In the twoFlightsinOneTransaction if cond");
						twoFlightsinOneTransaction(conn, custID, ticketType, TripDate, tripType, reserId, payId, tripID, payType, loc);
						//System.out.println("Completed twoFlightsinOneTransaction if cond");
					}
					//Two stops flight : Two transfers
					if(count>0 & stops == 2){
						threeFlightsinOneTransaction(conn, custID, ticketType, TripDate, tripType, reserId, payId, tripID, payType, loc);
					}
					
					//Get the results from the table
					/*ResultSet rs = executeQuery("SELECT * from airlines");
		            while(rs.next()){
		               System.out.println( rs.getString(1) + " : " + rs.getString(2));
		            }*/
		            
				} catch (SQLException e) {
					System.err.println("Could not create statement");
					e.printStackTrace();
				}finally{
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						System.err.println("Could not close connection");
					}
				}
	
	}
	
	//Function to perform cleanup of the temp tables before each run
	private static void cleanup(){
		// Getting connection from createConnection Class
		Connection conn = createConnection.createStatement();
		
		//Get the random tickettype by calling postgres procedure
		try {
			CallableStatement st = conn.prepareCall("{ call cleanup( ) }");
			st.execute();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			try {
				st.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			System.out.println("Can't clean up the previous run tables. Check cleanup.");
			e.printStackTrace();
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Can't clean up the previous run tables. Can't close connection.");
				e.printStackTrace();
			}
		}
		
	}
	
	//Function to run SQL query
	private static ResultSet executeQuery(String sqlStatement) {
		try {
			return st.executeQuery(sqlStatement);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	//Function to get custID randomly from the DB
	public static int getCustID(Connection conn) throws SQLException{
		
		//Get the random customerID by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ ? = call returncustomer( ? ) }");
		st.registerOutParameter(1, Types.INTEGER);
		st.setInt(2, 7);
		st.execute();
		int  custID = st.getInt(1);
		st.close();
		
		return custID;
	}
	
	//Function to get getTicketType randomly from the DB
	public static char getTicketType(Connection conn) throws SQLException{
		
		//Get the random tickettype by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ ? = call returntickettype( ? ) }");
		st.registerOutParameter(1, Types.CHAR);
		st.setInt(2, 3);
		st.execute();
		char ticketType = st.getString(1).charAt(0);
		st.close();
		
		return ticketType;
	}
	
	//Function to get getTripDate randomly from the DB
	public static Date getTripDate(Connection conn) throws SQLException{
		
		//Get the random tripdate by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ ? = call returnDate( ? ) }");
		st.registerOutParameter(1, Types.DATE);
		st.setInt(2, 2);
		st.execute();
		Date TripDate = st.getDate(1);
		st.close();
		
		return TripDate;
	}

	//Randomly generate TripType - oneway or roundtrip
	public static String getTripType(){
		String[] ttypes = {"OneWay"};
		Random r = new Random();
		String tripType = ttypes[r.nextInt(ttypes.length)];
		return tripType;
	}
	
	//Get the random reservationid, tripID, paymentID
	public static String randomString( int len ){
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) 
	      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
	   return sb.toString();
	}
	
	//Randomly generate Payment Type - Card, Cash, Check
	public static String getPaymentType(){
		String[] ptypes = {"Card", "Cash", "Check"};
		Random r = new Random();
		String paymentType = ptypes[r.nextInt(ptypes.length)];
		return paymentType;
	}
	
	//Function to get origin and dest randomly from the DB
	public static String[] getOrgDest(Connection conn) throws SQLException{
		String[] loc = new String[4];
		
		//Get the random origin and dest by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ call returnorgdest( ?, ?, ?, ?, ? ) }");
		st.setInt(1, 7);
		st.registerOutParameter(2, Types.VARCHAR);
		st.registerOutParameter(3, Types.VARCHAR);
		st.registerOutParameter(4, Types.INTEGER);
		st.registerOutParameter(5, Types.INTEGER);
		st.execute();
		
		loc[0] = st.getString(2);
		loc[1] = st.getString(3);
		int connective = st.getInt(4);
		loc[2] = Integer.toString(connective);
		int stop = st.getInt(5);
		loc[3] = Integer.toString(stop);
		st.close();
		
		return loc;
	}
	
	//Function to get getTicketType randomly from the DB
	public synchronized static int checkavailability(Connection conn, Date date, String org, String dest) throws SQLException{
		
		//Check availability by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ ? = call checkavailability( ?, ?, ? ) }");
		st.registerOutParameter(1, Types.INTEGER);
		st.setDate(2, date);
		st.setString(3, org);
		st.setString(4, dest);
		st.execute();
		int count = st.getInt(1);
		st.close();
		
		return count;
	}
	
	//Function to get getTicketType randomly from the DB
	public synchronized static int checkavailabilityInClass(Connection conn, Date date, String org, String dest, char seatclass) throws SQLException{
		
		//Check availability by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ ? = call checkavailabilityinclass( ?, ?, ?, ? ) }");
		st.registerOutParameter(1, Types.INTEGER);
		st.setDate(2, date);
		st.setString(3, org);
		st.setString(4, dest);
		st.setString(5, String.valueOf(seatclass));
		st.execute();
		int count = st.getInt(1);
		st.close();
		
		return count;
	}
	
	//Function to run one-way transaction - no transfers, direct flights
	public static void oneTransaction(Connection conn, int custID, char ticketType, Date TripDate, 
			String tripType, String reserId, String payId, String tripID, String payType, String[] loc) {
		try{

			
			PreparedStatement pst = null;
		    ResultSet rs = null;
		    PreparedStatement pst1 = null;
		    ResultSet rs1 = null;
		    PreparedStatement pst2 = null;
		    ResultSet rs2 = null;
		    PreparedStatement pst3 = null;
		    ResultSet rs3 = null;
		    PreparedStatement pst4 = null;
		    ResultSet rs4 = null;
		    PreparedStatement pst5 = null;
		    PreparedStatement pst6 = null;
		    PreparedStatement pst8 = null;
		    PreparedStatement pst9 = null;
		    PreparedStatement pst10 = null;
		    PreparedStatement pst11 = null;
		    PreparedStatement pst12 = null;
		    ResultSet rs12 = null;
		    PreparedStatement pst13 = null;
		    
			//To start the transaction turn off the auto-commit
			conn.setAutoCommit(false);
			//Check if seat is present in the particular flight
			int count = checkavailability(conn, TripDate, loc[0], loc[1]);
			if(count > 0){
				//Get the availid from the availability table
				String q1 = "SELECT B.availid FROM availability B "+
							"where B.flightdate = '"+TripDate+"' and B.seatclass = '"+ticketType+
							"' and B.origin = '"+loc[0]+"' and B.destination = '"+loc[1]+
							"' order by random() LIMIT 1 for Update;";
				pst = conn.prepareStatement(q1);
	            rs = pst.executeQuery();
	            
	            //if there are no rows returned then  - particular seatclass
	            if (!rs.next()) {                            //if rs.next() returns false then there are no rows.
	            	System.out.println("There is no seat with seat class "+ticketType+" on "+TripDate+" between "+loc[0]+" and "+loc[1]);
	            }
	            else {
	            	int avail = rs.getInt(1);
	            	//System.out.println("Availability id is : "+avail);
	            	//Get the flightid
	            	String q2 = "select A.flightid from availability A where A.availid = "+avail+";"; 
	            	pst1 = conn.prepareStatement(q2);
	                rs1 = pst1.executeQuery();
	                rs1.next();
	                String flid = rs1.getString(1); //Got the flight id here
	                //System.out.println("Flight id is : "+flid);
	                
	                
	                
	                //1. Now update the table flight_customer (flightid, reservationid, customerid, flightdate)
	                String q3 = "INSERT INTO flight_customer (flightid, reservationid, customerid, flightdate) values (?, ?, ?, ?)";
	            	pst2 = conn.prepareStatement(q3);
	            	pst2.setString(1, flid);
	                pst2.setString(2, reserId);
	                pst2.setInt(3, custID);
	                pst2.setDate(4, TripDate);
	                pst2.executeUpdate();
	                
	                //For payment we need to find cost from the flight_price table
	                String q4 = "select A.total from flight_price A where A.flightid = '"+flid+"' and A.seatclass = '"+ticketType+"';";
	                pst3 = conn.prepareStatement(q4);
	                rs3 = pst3.executeQuery();
	                rs3.next();
	                int cst = rs3.getInt(1);  //Cost of the seat on this flight
	                //System.out.println("Flight cost is : "+cst);
	                
	                //Get the flight seatnum
	            	String q5 = "select A.seatnum from availability A where A.availid = "+avail+";"; 
	            	pst4 = conn.prepareStatement(q5);
	                rs4 = pst4.executeQuery();
	                rs4.next();
	                String flst = rs4.getString(1); //Got the flight seat
	                //System.out.println("Flight seat is : "+flst);
	                
	                //2. Now update this information in the trip table
	                String q7 = "INSERT INTO trip (tripid, triplegid, flightid, flightdate, tripstatus, origin, destination) values (?,?,?,?,?,?,?)";
	                pst5 = conn.prepareStatement(q7);
	            	pst5.setString(1, tripID);
	                pst5.setString(2, "1");
	                pst5.setString(3, flid);
	                pst5.setDate(4, TripDate);
	                pst5.setString(5, "OnTime");
	                pst5.setString(6, loc[0]);
	                pst5.setString(7, loc[1]);
	                pst5.executeUpdate();
	                
	                //3. Now update this information in the tripdetails
	                String q6 = "INSERT INTO tripdetails (reservationid, tripid, triplegid, seatnum, legcost) values (?, ?, ?, ?, ?)";
	                pst6 = conn.prepareStatement(q6);
	            	pst6.setString(1, reserId);
	                pst6.setString(2, tripID);
	                pst6.setString(3, "1");
	                pst6.setString(4, flst);
	                pst6.setInt(5, cst);
	                pst6.executeUpdate();             
	                
	                //4. Now update this information in the trip_location
	                String q8 = "INSERT INTO trip_location (tripid, triplegid, locationid) values (?, ?, ?)";
	                pst8 = conn.prepareStatement(q8);
	            	pst8.setString(1, tripID);
	                pst8.setString(2, "1");
	                pst8.setString(3, loc[0]);
	                pst8.executeUpdate();
	                
	                Date dt = new java.sql.Date(System.currentTimeMillis());
	                
	                //5. Now update this information in the reservation table
	                String q9 = "INSERT INTO reservation (reservationid, reservationdate, reservationtype, customerid, paymentid, reservationcost) values (?,?,?,?,?,?)";
	                pst9 = conn.prepareStatement(q9);
	            	pst9.setString(1, reserId);
	                pst9.setDate(2, dt);
	                pst9.setString(3, tripType);
	                pst9.setInt(4, custID);
	                pst9.setString(5, payId);
	                pst9.setInt(6, cst);
	                pst9.executeUpdate();
	                
	                //6. Now update this information in the payment table
	                String q10 = "INSERT INTO payment (paymentid, paymentdate, paymenttype, paidamount) values (?,?,?,?)";
	                pst10 = conn.prepareStatement(q10);
	                pst10.setString(1, payId);
	                pst10.setDate(2, dt);
	                pst10.setString(3, payType);
	                pst10.setInt(4, cst);
	                pst10.executeUpdate();
	                
	                //Get the aircraftId
	            	String q12 = "select A.aircraftid from availability A where A.availid = "+avail+";"; 
	            	pst12 = conn.prepareStatement(q12);
	                rs12 = pst12.executeQuery();
	                rs12.next();
	                String arftid = rs12.getString(1); //Got the aircraft id
	                //System.out.println("Aircraft ID  is : "+arftid);
	                
	                //Now we need to add this row to booked table and remove this row from the availability table
	                String q11 = "INSERT INTO BookedReservation (availId, customerID, reservationID, flightDate, flightID, "
	                				+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
	                pst11 = conn.prepareStatement(q11);
	            	pst11.setInt(1, avail);
	                pst11.setInt(2, custID);
	                pst11.setString(3, reserId);
	                pst11.setDate(4, TripDate);
	                pst11.setString(5, flid);
	                pst11.setString(6, flst);
	                pst11.setString(7, String.valueOf(ticketType));
	                pst11.setString(8, arftid);
	                pst11.setString(9, loc[0]);
	                pst11.setString(10, loc[1]);	                
	                pst11.executeUpdate();
	                
	                //Delete this particular row from Availability
	                String q13 = "DELETE FROM Availability A WHERE A.availId = ?";
	                pst13 = conn.prepareStatement(q13);
	                pst13.setInt(1, avail);
	                pst13.executeUpdate();
	                
	                //commit the changes to DB
	                conn.commit();
	                System.out.println("Booked Non-Stop Reservation. Availability id:"+avail+". ReservationId:"+reserId);
	            }
			}else{
				System.out.println("There is no seat on flight on "+TripDate+" between "+loc[0]+" and "+loc[1]);
			}			
		}catch(SQLException e) {
			// TODO Auto-generated catch block
			//Rollback the changes
			try {
				conn.rollback();
				conn.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}			
			e.printStackTrace();
			System.err.println("Could not close connection");
			
		}
	}
	
	//Function to run one-way transaction - one stop/transfer flights
	public static void twoFlightsinOneTransaction(Connection conn, int custID, char ticketType, Date TripDate, 
				String tripType, String reserId, String payId, String tripID, String payType, String[] loc) {
		
		try {
			PreparedStatement pst = null;
		    ResultSet rs = null;
		    PreparedStatement pst1 = null;
		    ResultSet rs1 = null;
		    PreparedStatement pst2 = null;
		    ResultSet rs2 = null;
		    PreparedStatement pst3 = null;
		    ResultSet rs3 = null;
		    PreparedStatement pst4 = null;
		    ResultSet rs4 = null;
		    PreparedStatement pst5 = null;
		    ResultSet rs5 = null;
		    PreparedStatement pst6 = null;
		    PreparedStatement pst7 = null;
		    PreparedStatement pst8 = null;
		    PreparedStatement pst9 = null;
		    PreparedStatement pst10 = null;
		    ResultSet rs10 = null;
		    PreparedStatement pst11 = null;
		    ResultSet rs11 = null;
		    PreparedStatement pst12 = null;
		    ResultSet rs12 = null;
		    PreparedStatement pst13 = null;
		    ResultSet rs13 = null;
		    PreparedStatement pst14 = null;
		    PreparedStatement pst15 = null;
		    PreparedStatement pst16 = null;
		    PreparedStatement pst17 = null;
		    PreparedStatement pst18 = null;
		    PreparedStatement pst19 = null;
		    PreparedStatement pst20 = null;
		    ResultSet rs20 = null;
		    PreparedStatement pst21 = null;
		    ResultSet rs21 = null;
		    PreparedStatement pst22 = null;
		    PreparedStatement pst23 = null;
		    PreparedStatement pst24 = null;
		    PreparedStatement pst25 = null;
		    
			//To start the transaction turn off the auto-commit
			conn.setAutoCommit(false);

			//Get the connectivity
			int connectivity = Integer.parseInt(loc[2]);
			
			//For 2nd flight in the reservation we need to find the origin and destination
            String q = "select A.origin from connections A where A.connid = "+connectivity+";";
            pst = conn.prepareStatement(q);
            rs = pst.executeQuery();
            rs.next();
            String origin2 = rs.getString(1);  //Origin of the second flight in reservation
            //System.out.println("Origin of the second fl is : "+origin2);
			
            String q1 = "select A.destination from connections A where A.connid = "+connectivity+";";
            pst1 = conn.prepareStatement(q1);
            rs1 = pst1.executeQuery();
            rs1.next();
            String dest2 = rs1.getString(1);  //Destination of the second flight in reservation
            //System.out.println("Destination of the second fl is : "+dest2);
            
            java.sql.Date TripDate2=java.sql.Date.valueOf("2016-05-06"); //Date of the second flight 
            
            //Now perform the real transaction - book two flights in one trip with legid=1 and legid=2
            //Check if seat is present in the first/second flights
			//int count1 = checkavailability(conn, TripDate, loc[0], loc[1]);
            //int count2 = checkavailability(conn, TripDate2, origin2, dest2);
			int count1s = checkavailabilityInClass(conn, TripDate, loc[0], loc[1], ticketType);
			int count2s = checkavailabilityInClass(conn, TripDate2, origin2, dest2, ticketType);
			
			if(count1s > 0 & count2s > 0){ //Means seat of the class we need is present in both the flights
				
				//Get the availid1 from the availability table for flight 1
				String q2 = "SELECT B.availid FROM availability B "+
							"where B.flightdate = '"+TripDate+"' and B.seatclass = '"+ticketType+
							"' and B.origin = '"+loc[0]+"' and B.destination = '"+loc[1]+
							"' order by random() LIMIT 1 for Update;";
				pst2 = conn.prepareStatement(q2);
	            rs2 = pst2.executeQuery();
	            //Get the availid2 from the availability table for flight 2
				String q3 = "SELECT B.availid FROM availability B "+
							"where B.flightdate = '"+TripDate2+"' and B.seatclass = '"+ticketType+
							"' and B.origin = '"+origin2+"' and B.destination = '"+dest2+
							"' order by random() LIMIT 1 for Update;";
				pst3 = conn.prepareStatement(q3);
	            rs3 = pst3.executeQuery();
	            
	            //if there are no rows returned then  - particular seatclass
	            if (!rs2.next() || !rs3.next()) {                            //if rs.next() returns false then there are no rows.
	            	System.out.println("There is no seat with seat class "+loc[0]+" -> "+loc[1]+" and "+origin2+" -> "+dest2);
	            }else { //Case where seat is present in both the flights
	            	//get avail1 from Flight 1
	            	int avail1 = rs2.getInt(1);
	            	//System.out.println("Availability id is Flight 1: "+avail1);
	            	//get avail2 from Flight 2
	            	int avail2 = rs3.getInt(1);
	            	//System.out.println("Availability id is Flight 2: "+avail2);
	            	
	            	//Get the flightid1
	            	String q4 = "select A.flightid from availability A where A.availid = "+avail1+";"; 
	            	pst4 = conn.prepareStatement(q4);
	                rs4 = pst4.executeQuery();
	                rs4.next();
	                String flid1 = rs4.getString(1); //Got the flight id 1 here
	                //System.out.println("Flight1 id is : "+flid1);
	                
	                //Get the flightid2
	            	String q5 = "select A.flightid from availability A where A.availid = "+avail2+";"; 
	            	pst5 = conn.prepareStatement(q5);
	                rs5 = pst5.executeQuery();
	                rs5.next();
	                String flid2 = rs5.getString(1); //Got the flight id 2 here
	                //System.out.println("Flight1 id is : "+flid2);
	                
	                //1. Now update the table flight_customer (flightid, reservationid, customerid, flightdate)
	                String q6 = "INSERT INTO flight_customer (flightid, reservationid, customerid, flightdate) values (?, ?, ?, ?)";
	            	pst6 = conn.prepareStatement(q6);
	            	pst6.setString(1, flid1);
	                pst6.setString(2, reserId);
	                pst6.setInt(3, custID);
	                pst6.setDate(4, TripDate);
	                pst6.executeUpdate();
	                //Flight 2
	                String q7 = "INSERT INTO flight_customer (flightid, reservationid, customerid, flightdate) values (?, ?, ?, ?)";
	            	pst7 = conn.prepareStatement(q7);
	            	pst7.setString(1, flid2);
	                pst7.setString(2, reserId);
	                pst7.setInt(3, custID);
	                pst7.setDate(4, TripDate2);
	                pst7.executeUpdate();
	                
	                //2. Now update this information in the trip table
	                String q8 = "INSERT INTO trip (tripid, triplegid, flightid, flightdate, tripstatus, origin, destination) values (?,?,?,?,?,?,?)";
	                pst8 = conn.prepareStatement(q8);
	            	pst8.setString(1, tripID);
	                pst8.setString(2, "1");
	                pst8.setString(3, flid1);
	                pst8.setDate(4, TripDate);
	                pst8.setString(5, "OnTime");
	                pst8.setString(6, loc[0]);
	                pst8.setString(7, loc[1]);
	                pst8.executeUpdate();
	                //Flight 2
	                String q9 = "INSERT INTO trip (tripid, triplegid, flightid, flightdate, tripstatus, origin, destination) values (?,?,?,?,?,?,?)";
	                pst9 = conn.prepareStatement(q9);
	            	pst9.setString(1, tripID);
	                pst9.setString(2, "2");
	                pst9.setString(3, flid2);
	                pst9.setDate(4, TripDate2);
	                pst9.setString(5, "OnTime");
	                pst9.setString(6, origin2);
	                pst9.setString(7, dest2);
	                pst9.executeUpdate();
	                
	                //For payment we need to find cost from the flight_price table
	                String q10 = "select A.total from flight_price A where A.flightid = '"+flid1+"' and A.seatclass = '"+ticketType+"';";
	                pst10 = conn.prepareStatement(q10);
	                rs10 = pst10.executeQuery();
	                rs10.next();
	                int cst1 = rs10.getInt(1);  //Cost of the seat on this flight1
	                //System.out.println("Flight1 cost is : "+cst1);
	                //Flight 2
	                String q11 = "select A.total from flight_price A where A.flightid = '"+flid2+"' and A.seatclass = '"+ticketType+"';";
	                pst11 = conn.prepareStatement(q11);
	                rs11 = pst11.executeQuery();
	                rs11.next();
	                int cst2 = rs11.getInt(1);  //Cost of the seat on this flight2
	                //System.out.println("Flight2 cost is : "+cst2);
	                
	                //Get the flight seatnum
	            	String q12 = "select A.seatnum from availability A where A.availid = "+avail1+";"; 
	            	pst12 = conn.prepareStatement(q12);
	                rs12 = pst12.executeQuery();
	                rs12.next();
	                String flst1 = rs12.getString(1); //Got the flight1 seat
	                //System.out.println("Flight1 seat is : "+flst1);
	                //Flight 2
	                String q13 = "select A.seatnum from availability A where A.availid = "+avail2+";"; 
	            	pst13 = conn.prepareStatement(q13);
	                rs13 = pst13.executeQuery();
	                rs13.next();
	                String flst2 = rs13.getString(1); //Got the flight2 seat
	                //System.out.println("Flight2 seat is : "+flst2);
	                
	                
	                //3. Now update this information in the tripdetails
	                String q14 = "INSERT INTO tripdetails (reservationid, tripid, triplegid, seatnum, legcost) values (?, ?, ?, ?, ?)";
	                pst14 = conn.prepareStatement(q14);
	            	pst14.setString(1, reserId);
	                pst14.setString(2, tripID);
	                pst14.setString(3, "1");
	                pst14.setString(4, flst1);
	                pst14.setInt(5, cst1);
	                pst14.executeUpdate(); 
	                //Flight 2
	                String q15 = "INSERT INTO tripdetails (reservationid, tripid, triplegid, seatnum, legcost) values (?, ?, ?, ?, ?)";
	                pst15 = conn.prepareStatement(q15);
	            	pst15.setString(1, reserId);
	                pst15.setString(2, tripID);
	                pst15.setString(3, "2");
	                pst15.setString(4, flst2);
	                pst15.setInt(5, cst2);
	                pst15.executeUpdate(); 
	                
	                //4. Now update this information in the trip_location
	                String q16 = "INSERT INTO trip_location (tripid, triplegid, locationid) values (?, ?, ?)";
	                pst16 = conn.prepareStatement(q16);
	            	pst16.setString(1, tripID);
	                pst16.setString(2, "1");
	                pst16.setString(3, loc[0]);
	                pst16.executeUpdate();
	                //Flight 2
	                String q17 = "INSERT INTO trip_location (tripid, triplegid, locationid) values (?, ?, ?)";
	                pst17 = conn.prepareStatement(q17);
	            	pst17.setString(1, tripID);
	                pst17.setString(2, "2");
	                pst17.setString(3, origin2);
	                pst17.executeUpdate();
	                
	                Date dt = new java.sql.Date(System.currentTimeMillis()); //Reservation Date
	                int tito = cst1 + cst2; //Total reservation cost is legid1 cst1 + legid2 cst2
	                
	                //5. Now update this information in the reservation table
	                String q18 = "INSERT INTO reservation (reservationid, reservationdate, reservationtype, customerid, paymentid, reservationcost) values (?,?,?,?,?,?)";
	                pst18 = conn.prepareStatement(q18);
	            	pst18.setString(1, reserId);
	                pst18.setDate(2, dt);
	                pst18.setString(3, tripType);
	                pst18.setInt(4, custID);
	                pst18.setString(5, payId);
	                pst18.setInt(6, tito);
	                pst18.executeUpdate();
	                
	                //6. Now update this information in the payment table
	                String q19 = "INSERT INTO payment (paymentid, paymentdate, paymenttype, paidamount) values (?,?,?,?)";
	                pst19 = conn.prepareStatement(q19);
	                pst19.setString(1, payId);
	                pst19.setDate(2, dt);
	                pst19.setString(3, payType);
	                pst19.setInt(4, tito);
	                pst19.executeUpdate();
	                
	                //Get the aircraftId
	            	String q20 = "select A.aircraftid from availability A where A.availid = "+avail1+";"; 
	            	pst20 = conn.prepareStatement(q20);
	                rs20 = pst20.executeQuery();
	                rs20.next();
	                String arftid1 = rs20.getString(1); //Got the aircraft id 1
	                //System.out.println("Aircraft ID 1 is : "+arftid1);
	                //Flight 2
	                String q21 = "select A.aircraftid from availability A where A.availid = "+avail2+";"; 
	            	pst21 = conn.prepareStatement(q21);
	                rs21 = pst21.executeQuery();
	                rs21.next();
	                String arftid2 = rs21.getString(1); //Got the aircraft id 2
	                //System.out.println("Aircraft ID 2 is : "+arftid2);
	                
	                //Now we need to add this row to booked table and remove this row from the availability table
	                String q22 = "INSERT INTO BookedReservation (availId, customerID, reservationID, flightDate, flightID, "
	                				+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
	                pst22 = conn.prepareStatement(q22);
	            	pst22.setInt(1, avail1);
	                pst22.setInt(2, custID);
	                pst22.setString(3, reserId);
	                pst22.setDate(4, TripDate);
	                pst22.setString(5, flid1);
	                pst22.setString(6, flst1);
	                pst22.setString(7, String.valueOf(ticketType));
	                pst22.setString(8, arftid1);
	                pst22.setString(9, loc[0]);
	                pst22.setString(10, loc[1]);	                
	                pst22.executeUpdate();
	                //FLight 2
	                String q23 = "INSERT INTO BookedReservation (availId, customerID, reservationID, flightDate, flightID, "
            				+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
		            pst23 = conn.prepareStatement(q23);
		        	pst23.setInt(1, avail2);
		            pst23.setInt(2, custID);
		            pst23.setString(3, reserId);
		            pst23.setDate(4, TripDate2);
		            pst23.setString(5, flid2);
		            pst23.setString(6, flst2);
		            pst23.setString(7, String.valueOf(ticketType));
		            pst23.setString(8, arftid2);
		            pst23.setString(9, origin2);
		            pst23.setString(10, dest2);	                
		            pst23.executeUpdate();
		            
		            //Delete this particular row from Availability
	                String q24 = "DELETE FROM Availability A WHERE A.availId = ?";
	                pst24 = conn.prepareStatement(q24);
	                pst24.setInt(1, avail1);
	                pst24.executeUpdate();
	                //Flight 2
	                String q25 = "DELETE FROM Availability A WHERE A.availId = ?";
	                pst25 = conn.prepareStatement(q25);
	                pst25.setInt(1, avail2);
	                pst25.executeUpdate();
	                
	                //commit the changes to DB
	                conn.commit();
	                System.out.println("Booked One-Stop Reservation. Availability id:"+avail1+", "+avail2+". ReservationId:"+reserId);
	            }
	            
			}else{
				System.out.println("There is no seat on either flight 1 or flight 2 of this trip."+loc[0]+" -> "+loc[1]+" and "+origin2+" -> "+dest2);
			}  		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//Rollback the changes
			try {
				conn.rollback();
				conn.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}			
			e.printStackTrace();
			System.err.println("Could not close connection");
			
		}
	}
	
	//Function to run one-way transaction - two stop/transfer flights
	public static void threeFlightsinOneTransaction(Connection conn, int custID, char ticketType, Date TripDate, 
					String tripType, String reserId, String payId, String tripID, String payType, String[] loc) {
				
		try {
			
			PreparedStatement pst = null;
		    ResultSet rs = null;
		    PreparedStatement pst1 = null;
		    ResultSet rs1 = null;
		    PreparedStatement pst2 = null;
		    ResultSet rs2 = null;
		    PreparedStatement pst3 = null;
		    ResultSet rs3 = null;
		    PreparedStatement pst4 = null;
		    ResultSet rs4 = null;
		    PreparedStatement pst5 = null;
		    ResultSet rs5 = null;
		    PreparedStatement pst6 = null;
		    PreparedStatement pst7 = null;
		    PreparedStatement pst8 = null;
		    PreparedStatement pst9 = null;
		    PreparedStatement pst10 = null;
		    ResultSet rs10 = null;
		    PreparedStatement pst11 = null;
		    ResultSet rs11 = null;
		    PreparedStatement pst12 = null;
		    ResultSet rs12 = null;
		    PreparedStatement pst13 = null;
		    ResultSet rs13 = null;
		    PreparedStatement pst14 = null;
		    PreparedStatement pst15 = null;
		    PreparedStatement pst16 = null;
		    PreparedStatement pst17 = null;
		    PreparedStatement pst18 = null;
		    PreparedStatement pst19 = null;
		    PreparedStatement pst20 = null;
		    ResultSet rs20 = null;
		    PreparedStatement pst21 = null;
		    PreparedStatement pst22 = null;
		    PreparedStatement pst23 = null;
		    PreparedStatement pst24 = null;
		    PreparedStatement pst25 = null;
		    ResultSet rs21 = null;
		    PreparedStatement pst80 = null;
		    ResultSet rs80 = null;
		    PreparedStatement pst81 = null;
		    ResultSet rs81 = null;
		    PreparedStatement pst82 = null;
		    ResultSet rs82 = null;
		    PreparedStatement pst83 = null;
		    ResultSet rs83 = null;
		    PreparedStatement pst84 = null;
		    ResultSet rs84 = null;
		    PreparedStatement pst85 = null;
		    PreparedStatement pst86 = null;
		    PreparedStatement pst87 = null;
		    ResultSet rs87 = null;
		    PreparedStatement pst88 = null;
		    ResultSet rs88 = null;
		    PreparedStatement pst89 = null;
		    PreparedStatement pst90 = null;
		    PreparedStatement pst91 = null;
		    ResultSet rs91 = null;
		    PreparedStatement pst92 = null;
		    PreparedStatement pst93 = null;
		    
			//To start the transaction turn off the auto-commit
			conn.setAutoCommit(false);
			
			//Get the connectivity
			int connectivity = Integer.parseInt(loc[2]);
			
			//For 2nd flight in the reservation we need to find the origin and destination
            String q = "select A.origin from connections A where A.connid = "+connectivity+";";
            pst = conn.prepareStatement(q);
            rs = pst.executeQuery();
            rs.next();
            String origin2 = rs.getString(1);  //Origin of the second flight in reservation
            //System.out.println("Origin of the second fl is : "+origin2);
			
            String q1 = "select A.destination from connections A where A.connid = "+connectivity+";";
            pst1 = conn.prepareStatement(q1);
            rs1 = pst1.executeQuery();
            rs1.next();
            String dest2 = rs1.getString(1);  //Destination of the second flight in reservation
            //System.out.println("Destination of the second fl is : "+dest2);
            
            java.sql.Date TripDate2=java.sql.Date.valueOf("2016-05-05"); //Date of the second flight 
            
            //Get the connectivity from the Flight 2 to get to Flight 3
            String q80 = "select A.connectivity from connections A where A.connid = "+connectivity+";";
            pst80 = conn.prepareStatement(q80);
            rs80 = pst80.executeQuery();
            rs80.next();
            int connectivity2 = rs80.getInt(1);  //connectivity of the second flight in reservation
            //System.out.println("Connectivity of the second fl is : "+connectivity2);
            
            //For 3rd flight in the reservation we need to find the origin and destination
            String q81 = "select A.origin from connections A where A.connid = "+connectivity2+";";
            pst81 = conn.prepareStatement(q81);
            rs81 = pst81.executeQuery();
            rs81.next();
            String origin3 = rs81.getString(1);  //Origin of the third flight in reservation
            //System.out.println("Origin of the third fl is : "+origin3);
			
            String q82 = "select A.destination from connections A where A.connid = "+connectivity2+";";
            pst82 = conn.prepareStatement(q82);
            rs82 = pst82.executeQuery();
            rs82.next();
            String dest3 = rs82.getString(1);  //Destination of the third flight in reservation
            //System.out.println("Destination of the third fl is : "+dest3);
            
            java.sql.Date TripDate3=java.sql.Date.valueOf("2016-05-06"); //Date of the third flight 
            
            //Now perform the real transaction - book three flights in one trip with legid=1 and legid=2 and legid=3
            //Check if seat is present in the first/second/third flights
            int count1s = checkavailabilityInClass(conn, TripDate, loc[0], loc[1], ticketType);
			int count2s = checkavailabilityInClass(conn, TripDate2, origin2, dest2, ticketType);
			int count3s = checkavailabilityInClass(conn, TripDate3, origin3, dest3, ticketType);
            
			if(count1s > 0 & count2s > 0 & count3s > 0){ //Means seat of the class we need is present all of the three flights
				
				//Get the availid1 from the availability table for flight 1
				String q2 = "SELECT B.availid FROM availability B "+
							"where B.flightdate = '"+TripDate+"' and B.seatclass = '"+ticketType+
							"' and B.origin = '"+loc[0]+"' and B.destination = '"+loc[1]+
							"' order by random() LIMIT 1 for Update;";
				pst2 = conn.prepareStatement(q2);
	            rs2 = pst2.executeQuery();
	            //Get the availid2 from the availability table for flight 2
				String q3 = "SELECT B.availid FROM availability B "+
							"where B.flightdate = '"+TripDate2+"' and B.seatclass = '"+ticketType+
							"' and B.origin = '"+origin2+"' and B.destination = '"+dest2+
							"' order by random() LIMIT 1 for Update;";
				pst3 = conn.prepareStatement(q3);
	            rs3 = pst3.executeQuery();
	            //Get the availid3 from the availability table for flight 3
	            String q83 = "SELECT B.availid FROM availability B "+
						"where B.flightdate = '"+TripDate3+"' and B.seatclass = '"+ticketType+
						"' and B.origin = '"+origin3+"' and B.destination = '"+dest3+
						"' order by random() LIMIT 1 for Update;";
				pst83 = conn.prepareStatement(q83);
	            rs83 = pst83.executeQuery();
				
	            //if there are no rows returned then  - particular seatclass
	            if (!rs2.next() || !rs3.next() || !rs83.next()) {                            //if rs.next() returns false then there are no rows.
	            	System.out.println("There is no seat with seat class "+loc[0]+" -> "+loc[1]+" and "+origin2+" -> "+dest2+" and "+origin3+" -> "+dest3);
	            }else { //Case where seat is present in all of the three flights
	            	//get avail1 from Flight 1
	            	int avail1 = rs2.getInt(1);
	            	//System.out.println("Availability id is Flight 1: "+avail1);
	            	//get avail2 from Flight 2
	            	int avail2 = rs3.getInt(1);
	            	//System.out.println("Availability id is Flight 2: "+avail2);
	            	//get avail3 from Flight 3
	            	int avail3 = rs83.getInt(1);
	            	//System.out.println("Availability id is Flight 3: "+avail3);
	            	
	            	//Get the flightid1
	            	String q4 = "select A.flightid from availability A where A.availid = "+avail1+";"; 
	            	pst4 = conn.prepareStatement(q4);
	                rs4 = pst4.executeQuery();
	                rs4.next();
	                String flid1 = rs4.getString(1); //Got the flight id 1 here
	                //System.out.println("Flight1 id is : "+flid1);
	                
	                //Get the flightid2
	            	String q5 = "select A.flightid from availability A where A.availid = "+avail2+";"; 
	            	pst5 = conn.prepareStatement(q5);
	                rs5 = pst5.executeQuery();
	                rs5.next();
	                String flid2 = rs5.getString(1); //Got the flight id 2 here
	                //System.out.println("Flight2 id is : "+flid2);
	                
	                //Get the flightid3
	            	String q84 = "select A.flightid from availability A where A.availid = "+avail3+";"; 
	            	pst84 = conn.prepareStatement(q84);
	                rs84 = pst84.executeQuery();
	                rs84.next();
	                String flid3 = rs84.getString(1); //Got the flight id 3 here
	                //System.out.println("Flight3 id is : "+flid3);
	                
	                //1. Now update the table flight_customer (flightid, reservationid, customerid, flightdate)
	                String q6 = "INSERT INTO flight_customer (flightid, reservationid, customerid, flightdate) values (?, ?, ?, ?)";
	            	pst6 = conn.prepareStatement(q6);
	            	pst6.setString(1, flid1);
	                pst6.setString(2, reserId);
	                pst6.setInt(3, custID);
	                pst6.setDate(4, TripDate);
	                pst6.executeUpdate();
	                //Flight 2
	                String q7 = "INSERT INTO flight_customer (flightid, reservationid, customerid, flightdate) values (?, ?, ?, ?)";
	            	pst7 = conn.prepareStatement(q7);
	            	pst7.setString(1, flid2);
	                pst7.setString(2, reserId);
	                pst7.setInt(3, custID);
	                pst7.setDate(4, TripDate2);
	                pst7.executeUpdate();
	                //Flight 3
	                String q85 = "INSERT INTO flight_customer (flightid, reservationid, customerid, flightdate) values (?, ?, ?, ?)";
	            	pst85 = conn.prepareStatement(q85);
	            	pst85.setString(1, flid3);
	                pst85.setString(2, reserId);
	                pst85.setInt(3, custID);
	                pst85.setDate(4, TripDate3);
	                pst85.executeUpdate();
	                
	                //2. Now update this information in the trip table
	                String q8 = "INSERT INTO trip (tripid, triplegid, flightid, flightdate, tripstatus, origin, destination) values (?,?,?,?,?,?,?)";
	                pst8 = conn.prepareStatement(q8);
	            	pst8.setString(1, tripID);
	                pst8.setString(2, "1");
	                pst8.setString(3, flid1);
	                pst8.setDate(4, TripDate);
	                pst8.setString(5, "OnTime");
	                pst8.setString(6, loc[0]);
	                pst8.setString(7, loc[1]);
	                pst8.executeUpdate();
	                //Flight 2
	                String q9 = "INSERT INTO trip (tripid, triplegid, flightid, flightdate, tripstatus, origin, destination) values (?,?,?,?,?,?,?)";
	                pst9 = conn.prepareStatement(q9);
	            	pst9.setString(1, tripID);
	                pst9.setString(2, "2");
	                pst9.setString(3, flid2);
	                pst9.setDate(4, TripDate2);
	                pst9.setString(5, "OnTime");
	                pst9.setString(6, origin2);
	                pst9.setString(7, dest2);
	                pst9.executeUpdate();
	                //Flight 3
	                String q86 = "INSERT INTO trip (tripid, triplegid, flightid, flightdate, tripstatus, origin, destination) values (?,?,?,?,?,?,?)";
	                pst86 = conn.prepareStatement(q86);
	            	pst86.setString(1, tripID);
	                pst86.setString(2, "3");
	                pst86.setString(3, flid3);
	                pst86.setDate(4, TripDate3);
	                pst86.setString(5, "OnTime");
	                pst86.setString(6, origin3);
	                pst86.setString(7, dest3);
	                pst86.executeUpdate();
	                
	                //For payment we need to find cost from the flight_price table
	                String q10 = "select A.total from flight_price A where A.flightid = '"+flid1+"' and A.seatclass = '"+ticketType+"';";
	                pst10 = conn.prepareStatement(q10);
	                rs10 = pst10.executeQuery();
	                rs10.next();
	                int cst1 = rs10.getInt(1);  //Cost of the seat on this flight1
	                //System.out.println("Flight1 cost is : "+cst1);
	                //Flight 2
	                String q11 = "select A.total from flight_price A where A.flightid = '"+flid2+"' and A.seatclass = '"+ticketType+"';";
	                pst11 = conn.prepareStatement(q11);
	                rs11 = pst11.executeQuery();
	                rs11.next();
	                int cst2 = rs11.getInt(1);  //Cost of the seat on this flight2
	                //System.out.println("Flight2 cost is : "+cst2);
	                //Flight 3
	                String q87 = "select A.total from flight_price A where A.flightid = '"+flid3+"' and A.seatclass = '"+ticketType+"';";
	                pst87 = conn.prepareStatement(q87);
	                rs87 = pst87.executeQuery();
	                rs87.next();
	                int cst3 = rs87.getInt(1);  //Cost of the seat on this flight3
	                //System.out.println("Flight3 cost is : "+cst3);
	                
	                //Get the flight seatnum
	            	String q12 = "select A.seatnum from availability A where A.availid = "+avail1+";"; 
	            	pst12 = conn.prepareStatement(q12);
	                rs12 = pst12.executeQuery();
	                rs12.next();
	                String flst1 = rs12.getString(1); //Got the flight1 seat
	                //System.out.println("Flight1 seat is : "+flst1);
	                //Flight 2
	                String q13 = "select A.seatnum from availability A where A.availid = "+avail2+";"; 
	            	pst13 = conn.prepareStatement(q13);
	                rs13 = pst13.executeQuery();
	                rs13.next();
	                String flst2 = rs13.getString(1); //Got the flight2 seat
	                //System.out.println("Flight2 seat is : "+flst2);
	                //Flight 3
	                String q88 = "select A.seatnum from availability A where A.availid = "+avail3+";"; 
	            	pst88 = conn.prepareStatement(q88);
	                rs88 = pst88.executeQuery();
	                rs88.next();
	                String flst3 = rs88.getString(1); //Got the flight3 seat
	                //System.out.println("Flight3 seat is : "+flst3);
	                
	                //3. Now update this information in the tripdetails
	                String q14 = "INSERT INTO tripdetails (reservationid, tripid, triplegid, seatnum, legcost) values (?, ?, ?, ?, ?)";
	                pst14 = conn.prepareStatement(q14);
	            	pst14.setString(1, reserId);
	                pst14.setString(2, tripID);
	                pst14.setString(3, "1");
	                pst14.setString(4, flst1);
	                pst14.setInt(5, cst1);
	                pst14.executeUpdate(); 
	                //Flight 2
	                String q15 = "INSERT INTO tripdetails (reservationid, tripid, triplegid, seatnum, legcost) values (?, ?, ?, ?, ?)";
	                pst15 = conn.prepareStatement(q15);
	            	pst15.setString(1, reserId);
	                pst15.setString(2, tripID);
	                pst15.setString(3, "2");
	                pst15.setString(4, flst2);
	                pst15.setInt(5, cst2);
	                pst15.executeUpdate();
	                //Flight 3
	                String q89 = "INSERT INTO tripdetails (reservationid, tripid, triplegid, seatnum, legcost) values (?, ?, ?, ?, ?)";
	                pst89 = conn.prepareStatement(q89);
	            	pst89.setString(1, reserId);
	                pst89.setString(2, tripID);
	                pst89.setString(3, "3");
	                pst89.setString(4, flst3);
	                pst89.setInt(5, cst3);
	                pst89.executeUpdate();
	                
	                //4. Now update this information in the trip_location
	                String q16 = "INSERT INTO trip_location (tripid, triplegid, locationid) values (?, ?, ?)";
	                pst16 = conn.prepareStatement(q16);
	            	pst16.setString(1, tripID);
	                pst16.setString(2, "1");
	                pst16.setString(3, loc[0]);
	                pst16.executeUpdate();
	                //Flight 2
	                String q17 = "INSERT INTO trip_location (tripid, triplegid, locationid) values (?, ?, ?)";
	                pst17 = conn.prepareStatement(q17);
	            	pst17.setString(1, tripID);
	                pst17.setString(2, "2");
	                pst17.setString(3, origin2);
	                pst17.executeUpdate();
	                //Flight 3
	                String q90 = "INSERT INTO trip_location (tripid, triplegid, locationid) values (?, ?, ?)";
	                pst90 = conn.prepareStatement(q90);
	            	pst90.setString(1, tripID);
	                pst90.setString(2, "3");
	                pst90.setString(3, origin3);
	                pst90.executeUpdate();
	                
	                Date dt = new java.sql.Date(System.currentTimeMillis()); //Reservation Date
	                int tito = cst1 + cst2 + cst3; //Total reservation cost is legid1 cst1 + legid2 cst2 + legid3 cst3
	                
	                //5. Now update this information in the reservation table
	                String q18 = "INSERT INTO reservation (reservationid, reservationdate, reservationtype, customerid, paymentid, reservationcost) values (?,?,?,?,?,?)";
	                pst18 = conn.prepareStatement(q18);
	            	pst18.setString(1, reserId);
	                pst18.setDate(2, dt);
	                pst18.setString(3, tripType);
	                pst18.setInt(4, custID);
	                pst18.setString(5, payId);
	                pst18.setInt(6, tito);
	                pst18.executeUpdate();
	                
	                //6. Now update this information in the payment table
	                String q19 = "INSERT INTO payment (paymentid, paymentdate, paymenttype, paidamount) values (?,?,?,?)";
	                pst19 = conn.prepareStatement(q19);
	                pst19.setString(1, payId);
	                pst19.setDate(2, dt);
	                pst19.setString(3, payType);
	                pst19.setInt(4, tito);
	                pst19.executeUpdate();
	                
	                //Get the aircraftId
	            	String q20 = "select A.aircraftid from availability A where A.availid = "+avail1+";"; 
	            	pst20 = conn.prepareStatement(q20);
	                rs20 = pst20.executeQuery();
	                rs20.next();
	                String arftid1 = rs20.getString(1); //Got the aircraft id 1
	                //System.out.println("Aircraft ID 1 is : "+arftid1);
	                //Flight 2
	                String q21 = "select A.aircraftid from availability A where A.availid = "+avail2+";"; 
	            	pst21 = conn.prepareStatement(q21);
	                rs21 = pst21.executeQuery();
	                rs21.next();
	                String arftid2 = rs21.getString(1); //Got the aircraft id 2
	                //System.out.println("Aircraft ID 2 is : "+arftid2);
	                //Flight 3
	                String q91 = "select A.aircraftid from availability A where A.availid = "+avail3+";"; 
	            	pst91 = conn.prepareStatement(q91);
	                rs91 = pst91.executeQuery();
	                rs91.next();
	                String arftid3 = rs91.getString(1); //Got the aircraft id 3
	                //System.out.println("Aircraft ID 3 is : "+arftid3);
	                
	                //Now we need to add this row to booked table and remove this row from the availability table
	                String q22 = "INSERT INTO BookedReservation (availId, customerID, reservationID, flightDate, flightID, "
	                				+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
	                pst22 = conn.prepareStatement(q22);
	            	pst22.setInt(1, avail1);
	                pst22.setInt(2, custID);
	                pst22.setString(3, reserId);
	                pst22.setDate(4, TripDate);
	                pst22.setString(5, flid1);
	                pst22.setString(6, flst1);
	                pst22.setString(7, String.valueOf(ticketType));
	                pst22.setString(8, arftid1);
	                pst22.setString(9, loc[0]);
	                pst22.setString(10, loc[1]);	                
	                pst22.executeUpdate();
	                //FLight 2
	                String q23 = "INSERT INTO BookedReservation (availId, customerID, reservationID, flightDate, flightID, "
            				+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
		            pst23 = conn.prepareStatement(q23);
		        	pst23.setInt(1, avail2);
		            pst23.setInt(2, custID);
		            pst23.setString(3, reserId);
		            pst23.setDate(4, TripDate2);
		            pst23.setString(5, flid2);
		            pst23.setString(6, flst2);
		            pst23.setString(7, String.valueOf(ticketType));
		            pst23.setString(8, arftid2);
		            pst23.setString(9, origin2);
		            pst23.setString(10, dest2);	                
		            pst23.executeUpdate();
		            //FLight 3
	                String q92 = "INSERT INTO BookedReservation (availId, customerID, reservationID, flightDate, flightID, "
            				+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
		            pst92 = conn.prepareStatement(q92);
		        	pst92.setInt(1, avail3);
		            pst92.setInt(2, custID);
		            pst92.setString(3, reserId);
		            pst92.setDate(4, TripDate3);
		            pst92.setString(5, flid3);
		            pst92.setString(6, flst3);
		            pst92.setString(7, String.valueOf(ticketType));
		            pst92.setString(8, arftid3);
		            pst92.setString(9, origin3);
		            pst92.setString(10, dest3);	                
		            pst92.executeUpdate();
		            
		            
		            //Delete this particular row from Availability
	                String q24 = "DELETE FROM Availability A WHERE A.availId = ?";
	                pst24 = conn.prepareStatement(q24);
	                pst24.setInt(1, avail1);
	                pst24.executeUpdate();
	                //Flight 2
	                String q25 = "DELETE FROM Availability A WHERE A.availId = ?";
	                pst25 = conn.prepareStatement(q25);
	                pst25.setInt(1, avail2);
	                pst25.executeUpdate();
	                //Flight 3
	                String q93 = "DELETE FROM Availability A WHERE A.availId = ?";
	                pst93 = conn.prepareStatement(q93);
	                pst93.setInt(1, avail3);
	                pst93.executeUpdate();
	                
	                //commit the changes to DB
	                conn.commit();
	                System.out.println("Booked Two-Stop Reservation. Availability id:"+avail1+", "+avail2+", "+avail3+". ReservationId:"+reserId);
	            }
	            
	            
			}else{
				System.out.println("There is no seat on either flight 1 or flight 2 or flight 3 of this trip."+loc[0]+" -> "+loc[1]+" and "+origin2+" -> "+dest2+" and "+origin3+" -> "+dest3);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//Rollback the changes
			try {
				conn.rollback();
				conn.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}			
			e.printStackTrace();
			System.err.println("Could not close connection");
			
		}
		
	}
	

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		System.out.println("Running " +  threadName );
		// Getting connection from createConnection Class
		Connection conn = createConnection.createStatement();				
				
				//create a statement for this connection
				try {
					//st = conn.createStatement();
					
					//Get random customerID
					int custID = getCustID(conn);
					//System.out.println(custID);
					//Get random seat class - Business/Economy/FirstClass
					char ticketType = getTicketType(conn);
					//System.out.println(ticketType);
					//Get random TripDate from the DB
					Date TripDate = getTripDate(conn);
					//System.out.println(TripDate);
					//Get random tripType - one way or round trip
					String tripType = getTripType();
					//System.out.println(tripType);
					//Get random reservation ID
					String reserId = randomString(10);
					//System.out.println("Reservation ID : "+reserId);
					//Get random payment ID
					String payId = randomString(10);
					//System.out.println("Payment ID : "+payId);
					//Get random trip ID
					String tripID = randomString(10);
					//System.out.println("Trip ID : "+tripID);
					//Get random tripType - one way or round trip
					String payType = getPaymentType();
					//System.out.println(payType);
					//Get origin and destination
					String[] loc = new String[2];
					loc = getOrgDest(conn);
					//System.out.println("Origin is : "+loc[0]);
					//System.out.println("Destination is : "+loc[1]);
					//System.out.println("Connectivity is : "+loc[2]);
					//System.out.println("Stops is : "+loc[3]);
					
					//Get the connectivity
					int connectivity = Integer.parseInt(loc[2]);
					//Get the stops
					int stops = Integer.parseInt(loc[3]);
					
					int count = checkavailability(conn, TripDate, loc[0], loc[1]);
					//System.out.println("count is : "+count);
					
					//Direct Flights : Without transfers
					if(count>0 & stops == 0){ //Seats are there on that day
						oneTransaction(conn, custID, ticketType, TripDate, tripType, reserId, payId, tripID, payType, loc);
					}
					//One Stop flight : One transfer
					if(count>0 & stops == 1){
						//System.out.println("In the twoFlightsinOneTransaction if cond");
						twoFlightsinOneTransaction(conn, custID, ticketType, TripDate, tripType, reserId, payId, tripID, payType, loc);
						//System.out.println("Completed twoFlightsinOneTransaction if cond");
					}
					//Two stops flight : Two transfers
					if(count>0 & stops == 2){
						threeFlightsinOneTransaction(conn, custID, ticketType, TripDate, tripType, reserId, payId, tripID, payType, loc);
					}
					//Get the results from the table
					/*ResultSet rs = executeQuery("SELECT * from airlines");
		            while(rs.next()){
		               System.out.println( rs.getString(1) + " : " + rs.getString(2));
		            }*/
		            
				} catch (SQLException e) {
					System.err.println("Could not create statement");
					e.printStackTrace();
				}finally{
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						System.err.println("Could not close connection");
					}
				}
	}
	
	public void start (){
	      System.out.println("Starting " +  threadName );
	      if (t == null)
	      {
	         t = new Thread (this, threadName);
	         t.start ();
	      }
	}
}
