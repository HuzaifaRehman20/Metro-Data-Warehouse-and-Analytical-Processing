package project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
//import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.time.Month;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;


public class dwh 

{
	
	static String day(Calendar date1)
	
	{
		if(date1.get(date1.DAY_OF_WEEK) ==  date1.MONDAY)
			return "Monday";
		if(date1.get(date1.DAY_OF_WEEK) ==  date1.TUESDAY)
			return "Tuesday";
		if(date1.get(date1.DAY_OF_WEEK) == date1.WEDNESDAY)
			return "Wednesday";
		if(date1.get(date1.DAY_OF_WEEK) == date1.THURSDAY)
			return "Thursday";
		if(date1.get(date1.DAY_OF_WEEK) ==  date1.FRIDAY)
			return "Friday";
		if(date1.get(Calendar.DAY_OF_WEEK) ==  date1.SATURDAY)
			return "Saturday";
		if(date1.get(date1.DAY_OF_WEEK) ==  date1.SUNDAY)
			return "Sunday";
		return null;
		
	}

	static void meshjoin_implementation() throws SQLException, ParseException 
	
	{
		
		Scanner project = new Scanner(System.in);
		System.out.println("ENTER THE SCHEMA NAME (MYSQL):");
		String project1 = project.nextLine();
		
		Scanner username = new Scanner(System.in);
		System.out.println("ENTER YOUR USERNAME (MYSQL):");
		String username1 = username.nextLine();
		
		Scanner password = new Scanner(System.in);
		System.out.println("ENTER THE YOUR PASSWORD (MYSQL):");
		String password1 = password.nextLine();
		
		Connection data = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + project1, username1, password1);
		Connection dwh_starflake = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + "metro_dwh", username1, password1);

		sqlConnection(data,dwh_starflake);
		
	}
	
	static void sqlConnection(Connection data, Connection dwh_starflake) throws SQLException, ParseException 
	
	{
			
			Statement master_data = data.createStatement();
			Statement stream_data = data.createStatement();

			Statement dimensions = dwh_starflake.createStatement();
			Statement product_id = dwh_starflake.createStatement();
			Statement customer_id = dwh_starflake.createStatement();
			Statement store_id = dwh_starflake.createStatement();
			Statement supplier_id = dwh_starflake.createStatement();
			Statement time_id1 = dwh_starflake.createStatement();
			
			List<Map <String, String>> dataset = new ArrayList <Map <String, String>>();
			
			ArrayBlockingQueue<List<Map<String,String>>> q = new ArrayBlockingQueue<List<Map<String,String>>>(10);
			MultiValuedMap<String,Map<String,String>> map1 = new ArrayListValuedHashMap<>();
			
			int transaction_limit1 = 0;
			int transaction_limit2 = 50;
			int limit1 = 0;
			int amount1 = 10;
			int t_id = 1;
			int count = 0;
			int limit2 = 0;
			int amount2 = 5;
			
			while(true)
				
			{
				
				if (count == 30247)
					
				{
					
					break;
					
				}
				
				if (limit1 == 100)
					
				{
					
					limit1 = 0;
					
				}
				
				if (limit2 == 50)
					
				{
					
					limit2 = 0;
					
				}

				String query1 = "select * from transactions " + " limit " + transaction_limit1 + ", " + transaction_limit2;
				ResultSet rs1 = master_data.executeQuery(query1);
				
				List <Map<String, String>> stream = new ArrayList<Map<String, String>>(); 
				
				int count1 = 0;
				
				while(rs1.next())
					
				{
					
					Map <String, String> data1 = new HashMap<String,String>();
					
					String prodid = rs1.getString("ProductID");
					String orderid = rs1.getString("Order ID");
					String customerid = rs1.getString("customer_id");
					String time_id = rs1.getString("time_id");
					String timedate1 = rs1.getString("Order Date");
					String quantity = rs1.getString("Quantity Ordered");
					
					data1.put("PID", prodid);
					data1.put("ORDER_ID", orderid);
					data1.put("CUSTOMER_ID", customerid);
					data1.put("TIME_ID", time_id);
					data1.put("T_DATE", timedate1);
					data1.put("QUANTITY", quantity);
		
					dataset.add(data1);
					stream.add(data1);
					
					map1.put(data1.get("PID"), data1);
					map1.put(data1.get("CUSTOMER_ID"), data1);

					count1++;
					
				}
				
				int size = 10;
				
				if(q.size() == size)
					
				{
					
					for(Map<String,String> x : q.remove())
						
					{
						
						String pid_check = x.get("PID");
						String cid_check = x.get("CUSTOMER_ID");
						map1.removeMapping(pid_check, x);
						map1.removeMapping(cid_check, x);	
						
					}
					
				}
				
				q.add(stream);

				String query2 = "select * from products_data limit " + limit1 + ", " + amount1;
				ResultSet master = master_data.executeQuery(query2);
				
				String query3 = "select * from customers_data limit " + limit2 + ", " + amount2;  
				ResultSet master1 = stream_data.executeQuery(query3);
				
				while(master.next())
					
				{
					
					if(master1.next()) 
					
					{

					Map<String,String> data2 = new HashMap<String,String>();
					String prodid1 = master.getString("productID");
					String prodname = master.getString("productName");
					String price = master.getString("productPrice");
					String storeid = master.getString("storeID");
					String storename = master.getString("storeName");
					String suppid = master.getString("supplierID");
					String suppname = master.getString("supplierName");
					String custid = master1.getString("customer_id");
					String custname = master1.getString("customer_name");
					String gender = master1.getString("gender");
					data2.put("PID", prodid1);
					data2.put("PRODUCT_NAME", prodname);
					data2.put("PRICE", price);
					data2.put("STORE_ID", storeid);
					data2.put("STORE_NAME", storename);
					data2.put("SUPPLIER_ID", suppid);
					data2.put("SUPPLIER_NAME", suppname);
					
					
						if((map1.get(custid)) != null )
							
						{
							
							for(Map<String,String> x:map1.get(prodid1))
								
							{	
								
								String split = data2.get("PRICE");
								String price1 = split.replace("$", "");
								Double price2 = Double.parseDouble(price1);
								Double quantity1 = Double.parseDouble(x.get("QUANTITY"));
								String time_id = x.get("TIME_ID");
								
								String dates = x.get("T_DATE");
								String[] time_stamp = dates.split(" ");
								String date2 = time_stamp[0];
								//String time1 = time_stamp[1];
								
								String[] date = date2.split("-");
								
								String prodcheck = "SELECT * from Product WHERE ProductID = '" + data2.get("PID") + "'";
								String custcheck = "SELECT * from Customers WHERE CustomerID = '" + x.get("CUSTOMER_ID") + "'";
								String storecheck = "SELECT * from Store WHERE StoreID = '" + data2.get("STORE_ID") + "'";
								String suppcheck = "SELECT * from Supplier WHERE SupplierID = '" + data2.get("SUPPLIER_ID") + "'";
								String timecheck = "SELECT * from Transaction_Time WHERE TimePK = '" + t_id + "'";
								
								ResultSet prodc = product_id.executeQuery(prodcheck);
								ResultSet custc = customer_id.executeQuery(custcheck);
								ResultSet storec = store_id.executeQuery(storecheck);
								ResultSet supplierc = supplier_id.executeQuery(suppcheck);
								ResultSet timec = time_id1.executeQuery(timecheck); 
								
								Statement productinsert = dwh_starflake.createStatement();
								Statement customerinsert = dwh_starflake.createStatement();
								Statement storeinsert = dwh_starflake.createStatement();
								Statement supplierinsert = dwh_starflake.createStatement();
								Statement timeinsert = dwh_starflake.createStatement();
								
								if (prodc.next() == false)
									
								{
									
									String prod = "INSERT INTO Product (ProductID, ProductName, ProductPrice) VALUES ('" +  data2.get("PID") + "', '" +  data2.get("PRODUCT_NAME") + "','" +  price1 + "')";
									productinsert.executeUpdate(prod);
									
								}
								
								if (custc.next() == false)
									
								{
									
									String cust = "INSERT INTO Customers (CustomerID, CustomerName, Gender) VALUES ('" +  x.get("CUSTOMER_ID") + "', '" + custname + "', '" + gender + "')";
									customerinsert.executeUpdate(cust);
									
								}
								
								if (storec.next() == false)
									
								{
									
									String store = "INSERT INTO Store (StoreID, StoreName) VALUES ('" +  data2.get("STORE_ID") + "','" +  data2.get("STORE_NAME") + "')";
									storeinsert.executeUpdate(store);
									
								}
								
								if (supplierc.next() == false)
									
								{
		
									String supplier = "INSERT INTO Supplier (SupplierID, SupplierName) VALUES ('" +  data2.get("SUPPLIER_ID") + "', '" + data2.get("SUPPLIER_Name") + "')";
									supplierinsert.executeUpdate(supplier);
									
								}
								
								if (timec.next() == false)
									
								{
									
									Calendar ddd1 = Calendar.getInstance();
								    Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(date2);  

									ddd1.setTime(date1);
									
									String day_ = day(ddd1);
									
							        String monthName = Month.of(Integer.parseInt(date[1])).name();
							        monthName = monthName.charAt(0) + monthName.substring(1).toLowerCase();
									
									int quarter = 0;
									
									if (Float.parseFloat(date[1]) <= 3)
										
									{
										
										quarter = 1;
										
									}
									
									else if (Float.parseFloat(date[1]) > 3 & Float.parseFloat(date[1]) <= 6)
										
									{
										
										quarter = 2;
										
									}
									
									else if (Float.parseFloat(date[1]) > 6 & Float.parseFloat(date[1]) <= 9)
										
									{
										
										quarter = 3;
										
									}
									
									else if (Float.parseFloat(date[1]) > 9 & Float.parseFloat(date[1]) <= 12)
										
									{
										
										quarter = 4;
										
									}
									
									String time = "INSERT INTO Transaction_Time (TimePK, TimeID, TransactionDate, TransactionDay, TransactionMonth, TransactionQuarter, TransactionYear) VALUES ('" + (t_id) + "','" + time_id + "','" + date2 + "','" + day_ + "', '" + monthName + "', '" + String.valueOf(quarter) + "','" + date[0] + "')";
									timeinsert.executeUpdate(time);
									
//									try {
//								        
//								        System.out.println("Inserted TimeID: " + time_id);
//								    } catch (SQLException e) {
//								        System.err.println("Error inserting into Transaction_Time: " + e.getMessage());
//								    }
									
								}
								
								//System.out.println("Checking TimeID: " + time_id);

								Double sales_sum = quantity1 * price2;
								
								String sales = String.format("%.02f", sales_sum);
								
								String sale = "INSERT INTO Sales (OrderID, OrderDate, ProductID, CustomerID, StoreID, SupplierID, TimePK, Sales, Quantity) VALUES ('" + x.get("ORDER_ID") + "','" + date2 + "','" + data2.get("PID") + "','" + x.get("CUSTOMER_ID") + "','" + data2.get("STORE_ID") + "','" + data2.get("SUPPLIER_ID") + "','" + (t_id) + "','" + sales + "','" + x.get("QUANTITY") + "')";

								dimensions.executeUpdate(sale);
								System.out.println(sale);
								count++;
								t_id++;
							}
							
						}
						
					}
					
					else 
					
					{
						
						limit2+=5;
						query1 = "select * from customers_data limit " + limit2 + ", " + amount2;  
						master1 = stream_data.executeQuery(query1);
						
					}
					
				}
	
				transaction_limit1+=50;
				limit1+=10;
				limit2+=5;
			
			}
			
			System.out.print("Transactions Joined : " + count + "\n");
		
	}
	

	public static void main(String[] args) throws SQLException, ParseException 
	
	{
		
		meshjoin_implementation();
		
	}
	
}