package com.mycompany.app;

import java.util.List;

public class LeftOuterJoin {
	
	static String employee_pay = "src/main/java/data/employee_pay.csv";
	static String employee_names = "src/main/java/data/employee_names.csv";
	static String order = "src/main/java/data/order.csv";
	
	public static void main(String[] args) {

		DataSet dEmpName = new DataSetImpl(employee_names);
		
		//DataSet orderD = new DataSetImpl(order);
		DataSet dEmpPay = new DataSetImpl(employee_pay);

		
		//DataSet joinedDataSet   = dEmpName.leftJoin(orderD, "id", "customer_id");
		DataSet joinedDataSet   = dEmpName.leftJoin(dEmpPay, "id", "id");
		
		// to read empname and empsal
		String cols [] = {"id","first_name","last_name","salary","bonus"};
		
		// to read  empname and orders
		//String cols[] = {"id","first_name","last_name","prod_name","value"};
		
		DataSet joinResults = joinedDataSet.select(cols);
		joinResults.show();

		
	}

}
