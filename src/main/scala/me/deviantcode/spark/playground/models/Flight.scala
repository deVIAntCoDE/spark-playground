package me.deviantcode.spark.playground.models


//define schema for raw data
case class Flight(Month: String, DayofMonth: String, DayOfWeek: String, DepTime: Int, CRSDepTime: Int, ArrTime: Int, CRSArrTime: Int,
                  UniqueCarrier: String, ActualElapsedTime: Int, CRSElapsedTime: Int, AirTime: Int, ArrDelay: Double, DepDelay: Int, Origin: String, Distance: Int)