package com.mycompany.app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 
 *  DataSet loads data and stores in memory are key value in a map<String, List<String>>
 *  Key is column and value - list of string as  column values
 * 
 * @author dsingh
 *
 */

public class DataSetImpl extends AbstractDataSet implements DataSet{
   
   /**
    * DataSet created from a csv file 
    * @param str
    */
   
	public DataSetImpl(String str){
		dataSourceLocation = str;
		loadData();

	}
	
	/**
	 * DataSet created using columns and data provided externally.
	 * @param columns
	 * @param listRows
	 */
	
	
	public DataSetImpl(String [] columns , List<Map<String, String>>  listRows ){
		this.columns = columns;
		this.listRows = listRows;

	}
	
	
	public List<Map<String, String>> getListRows(){
		
		return listRows;
	}
	
	public String [] getColumns() {
		
		return columns;
	}
	
	public void setListRows(List<Map<String, String>> rows) {

		this.listRows = rows;
	}


	public void setColums(String[] str) {

		this.columns = str;
	}
	
	public  Map<String, List<Map<String, String>>>  indexOnColumn(String colToIndex){
		
		
		Map<String,List<Map<String, String>>> mapIndexedOutput = new HashMap<String, List<Map<String, String>>> ();
		
		List<Map<String, String>> listMap ;
		
		for(Map<String, String> rowM : this.listRows)
		{
			String colValInRow = rowM.get(colToIndex);
			if(mapIndexedOutput.get(colValInRow) == null) 
			{	
				listMap = new ArrayList<Map<String, String>>();
				listMap.add(rowM); 
				mapIndexedOutput.put(colValInRow, listMap);

			}
			else
			{
				mapIndexedOutput.get(colValInRow).add(rowM);
			}
					
		}
		
		return mapIndexedOutput;
	}	
	
	
	
	public DataSet leftJoin(DataSet dSet , String colLeft , String colRight){
				
		List<Map<String, String>> output = new ArrayList<Map<String, String>> ();
		
		/*  The column join takes care of not repeating column names if they are 
		 *  present in left and right tables both. Left table takes precedence since its a left join.
		 *   
		 *   In a join like  customer.id = order.customer_id ,   customer_id will be omitted from output 
		 *   after the match have been established . 
		 *   
		 */
		
		String [] joinCol = mergeColumnLeftJoin(this.columns,dSet.getColumns(), colRight);
	   				
		// Indexing helps in faster join . in  O(N) time complexity .
		Map<String, List<Map<String, String>>> mapL = ((DataSetImpl) dSet).indexOnColumn(colRight);
		
		for(Map<String,String> map:this.getListRows()) {			
			List<Map<String, String>> matchingRowsToRight = mapL.get(map.get(colLeft));  //  mapL.get(list.get(this.mapCol.get(colLeft)));
			Map<String,String> mapOutput  = null;
			if(matchingRowsToRight != null) 
			{
				for(Map<String,String> mapR:matchingRowsToRight)
				{
					mapOutput  = new HashMap<String,String>();
					
					// this for loop gets item from left first if not present then it goes to right.
					// this takes care of scenarios when columns names are sames in left and right tables in the join.
					for(String str:joinCol) {
						if(map.get(str)!=null) {
							mapOutput.put(str, map.get(str));
						}else {
							mapOutput.put(str, mapR.get(str));
						}
					}

					output.add(mapOutput);
				}
				
			}
			else 
			{
	
				output.add(map);
			}
			
		}
		
		return new DataSetImpl(joinCol,output); 
	}		


	
	public DataSet select(String [] cols) {
		
		List<Map<String, String>> output = new ArrayList<Map<String, String>> ();
		
		for(Map<String,String> row :this.getListRows()) 
		{
			Map<String,String>  rowOutput = new HashMap<String,String>();
			
			for(String col:cols) 
			{
				rowOutput.put(col, row.get(col));

			}
			output.add(rowOutput);
			
		}
			
		return new DataSetImpl(cols,output); 

		
	}

	public void show() 
	{
	
	   for(String str:this.getColumns()) 
		   System.out.print(str +"        ");

	   	System.out.println();
	   
	   for(Map<String,String> row:this.getListRows()) 
		   {
			   for(String str:this.getColumns())
			   {
				   System.out.print(row.get(str) +"        ");
			   }
			   	System.out.println();
 
	
		   }
		
	}



}
