# FlightDataQuery
This is a flight database built for companies and the health insurance sector.
By connecting with the front-end flight inquiry platform,
it assists the claim settlement unit to inquire about flight history and real-time information.

## Purpose
Travel inconvenience insurance in company health insurance In the process of claim settlement, 
every time a customer makes a claim, 
he needs to apply for a flight delay document to the airline and provide it to the company's claim unit so that the claim unit can verify the fact of the flight delay,
which not only increases the The process by which a customer makes a claim, and it also takes time for a claims adjuster to check and confirm the fact that the flight was delayed.
To this end, 
a flight information database was designed using publicly available information from the Ministry of Transportation, which is regularly updated every fifteen minutes.

Through the front-end platform, 
the assisting claim settlement unit can directly query the flight history and real-time information.

## Table Schema
<pre><code>
create table flight_table(
		Id number(38) generated always as identity(start with 1 increment by 1),
		FlightDate timestamp,
		FlightNumber nvarchar2(60),
		AirRouteType nvarchar2(60),
		AirlineID nvarchar2(60),
		DepartureAirportID nvarchar2(60),
		ArrivalAirportID nvarchar2(60),
		ScheduleDepartureTime timestamp,
		ActualDepartureTime timestamp,
		ScheduleArrivalTime timestamp,
		ActualArrivalTime timestamp,
		DepartureRemark nvarchar2(60),
	  ArrivalRemark nvarchar2(60),
		ArrivalTerminal nvarchar2(60),
		DepartureTerminal nvarchar2(60),
		ArrivalGate nvarchar2(60),
		DepartureGate nvarchar2(60),
		IsCargo number(1,0),
		UpdateTime timestamp,
		EstimatedArrivalTime timestamp,
		EstimatedDepartureTime timestamp,
		CheckCounter nvarchar2(60),
		BaggageClaim nvarchar2(60)
)
</code></pre>
