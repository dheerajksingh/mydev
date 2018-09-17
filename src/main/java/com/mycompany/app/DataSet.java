package com.mycompany.app;

import java.util.List;
import java.util.Map;


public interface  DataSet {

	public List<Map<String, String>> getListRows();
	
	public void setListRows(List<Map<String, String>> rows);
	
	public String [] getColumns() ;
	
	public void setColums(String[] str);
		
	public DataSet leftJoin(DataSet dSet , String col1 , String col2);
	
	public void show();
	
	public DataSet select(String [] cols) ;

		

}
