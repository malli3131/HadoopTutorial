package com.thread.local;

import java.util.HashMap;
import java.util.Map;

public class GoveranceSingleTon {
	private ThreadLocal<Map<String, String>> govObject = new ThreadLocal<Map<String, String>>();
	private static GoveranceSingleTon instance = null;
	private GoveranceSingleTon(){
		
	}
	public static GoveranceSingleTon getInstance(){
		if(GoveranceSingleTon.instance==null)GoveranceSingleTon.instance = new GoveranceSingleTon();
		return GoveranceSingleTon.instance;
	}
	public void setGoveObject(Map<String, String> gov){
		this.govObject.set(gov);
	}
	public Map<String, String> getGoveObject(){
		return this.govObject.get();
	}
	
	public static void main(String args[])
	{
		Map<String, String> gov = new HashMap<String, String>();
		gov.put("company", "impetus");
		gov.put("name", "naga");
		gov.put("age", "27");
		gov.put("location", "bangalore");
		
		GoveranceSingleTon myObject = GoveranceSingleTon.getInstance();
		
		myObject.setGoveObject(gov);
		Map<String, String> result = myObject.getGoveObject();
		System.out.println(result);
	}
}

