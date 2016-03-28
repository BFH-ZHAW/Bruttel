package com.bruttel;

import java.io.IOException;



public class PreActus {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		readContracts();
		readInterest();
		calculate();
		mapReduce();
		

	}
	private static void readContracts() {
		// KontraktdatenCSV einlesen
		try{
		System.out.println("CSV mit Kontrakten einlesen");
		
		} catch (Exception e) {
		e.printStackTrace();
	}
	}
	
	private static void readInterest() {
		// ZinsCSV einlesen
		try{
		System.out.println("CSV mit Zinsesn einlesen");
		
		} catch (Exception e) {
			e.printStackTrace();
	}
	}
	
	private static void calculate() {
		// Daten berechnen
		try{
		System.out.println("Rechnen");
		} catch (Exception e) {
			e.printStackTrace();
	}
	}
	private static void mapReduce() {
		// Daten Mappen
		try{
		System.out.println("Map and Reduce");
		} catch (Exception e) {
			e.printStackTrace();
	}
	}
}
