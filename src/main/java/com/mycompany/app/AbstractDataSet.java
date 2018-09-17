package com.mycompany.app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class   AbstractDataSet {
	
	String dataSourceLocation;
	String [] columns;
	List<Map<String, String>>  listRows = new ArrayList<Map<String, String>> ();
	
	
    static SparkConf sparkConf = new SparkConf().setAppName("Left Outer Join").setMaster("local[*]");;
    static JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	protected  void loadData() 
	{
	        /**
	         *  Read the source file , just leveraging spark code to read file .
	         *  Could have used simple file io .
	         */
		    
	    JavaRDD<String> empName = sparkContext.textFile(dataSourceLocation);
	   List <String> listData =  empName.collect();
	   // extracting column name 
	   String columnNamesCsv = listData.get(0);
	   columns=columnNamesCsv.split(",");
	   	   
	   listData.remove(0);  // this is to remove column names which is already caputured in mapCol and columns.
	   	   
	   Map <String,String> mapRow = null;

	   for(String str :listData) 
	   {
		  String [] rowData=str.split(",");
		  mapRow = new HashMap<String,String> ();
	      
		  for(int i =0 ;i< rowData.length;i++) 
			  mapRow.put(columns[i], rowData[i]);
			  	      	      
	      listRows.add(mapRow);
	   }
		   
	}
	
	
	protected String [] mergeColumnLeftJoin(String[] left , String[] right , String rightColumnMatching) {
		
		Map<Integer,String> map = new HashMap<Integer,String>();
		
		int i = 0;

		for(;i<left.length;i++) {
			map.put(i, left[i]);
		}
		
		for(int j = 0 ;j <right.length ;j++) {
			if(! rightColumnMatching.equals(right[j]))          // excluding matchin column from right if same
				if(!map.containsValue(right[j])) 
				map.put(i++, right[j]);
		}
		  
		String joinCol []  = new String[map.size()];
		
		for(int j = 0 ; j < map.size() ;j++) {
			joinCol[j] = map.get(j);
		}

		return joinCol;
		
	}
	
	 public abstract  List<Map<String, String>> getListRows();
	 
	 
	
	 
	

}
