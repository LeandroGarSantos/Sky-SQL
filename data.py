from sqlalchemy import create_engine, text, bindparam

QUERY_FLIGHT_BY_ID = """
            SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY
            FROM flights
            JOIN airlines ON flights.airline = airlines.id 
            WHERE flights.ID = :id
"""

QUERY_FLIGHTS_BY_DATE = """
    SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY 
    FROM flights 
    JOIN airlines ON flights.airline = airlines.id 
    WHERE flights.YEAR = :year AND flights.MONTH = :month AND flights.DAY = :day
"""


class FlightData:
    """
    The FlightData class is a Data Access Layer (DAL) object that provides an
    interface to the flight data in the SQLITE database. When the object is created,
    the class forms connection to the sqlite database file, which remains active
    until the object is destroyed.
    """

    def __init__(self, db_uri):
        """
        Initialize a new engine using the given database URI
        """
        self._engine = create_engine(db_uri)

    def _execute_query(self, query, params=None):
        """
        Execute an SQL query with the params provided in a dictionary,
        and returns a list of records (dictionary-like objects).
        If an exception was raised, print the error, and return an empty list.
        """
        # if params is None:
        #     params = {}
        try:
            with self._engine.connect() as connection:
                result = connection.execute(text(query), params)
                return [dict(row) for row in result]
        except Exception as e:
            print("Error executing query:", e)
            return []

    def get_flight_by_id(self, flight_id):
        """
        Searches for flight details using flight ID.
        If the flight was found, returns a list with a single record.
        """

        print("Inside get_flights_by_id function")  # Add debug print

        query = """
            SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY 
            FROM flights 
            JOIN airlines ON flights.airline = airlines.id 
            WHERE flights.ID = :flight_id
        """
        params = {'flight_id': flight_id}
        return self._execute_query(query, params=params)

    def get_flights_by_date(self, day, month, year):
        """
        Searches for flight details using the specified date (day, month, year).
        If any flights were found on that date, returns a list of records.
        """
        print("Inside get_flights_by_date function")  # Add debug print
        print("Date:", f"{day}/{month}/{year}")

        # Define the query inside the method
        query = """
            SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY 
            FROM flights 
            JOIN airlines ON flights.airline = airlines.id 
            WHERE flights.YEAR = :year AND flights.MONTH = :month AND flights.DAY = :day
        """

        params = {'day': day, 'month': month, 'year': year}
        return self._execute_query(query, params=params)

    def get_delayed_flights_by_airline(self, airline_name):
        """
        Searches for delayed flight details for a given airline name.
        Returns a list of delayed flight records.
        """
        query = """
            SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY 
            FROM flights 
            JOIN airlines ON flights.airline = airlines.id 
            WHERE flights.DEPARTURE_DELAY > 0 AND airlines.airline = :airline_name
        """
        params = {'airline_name': airline_name}
        return self._execute_query(query, params=params)

    def get_delayed_flights_by_airport(self, airport_IATA_CODE):
        """
        Searches for delayed flight details for a given origin airport (IATA code).
        Returns a list of delayed flight records.
        """
        query = """
            SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY 
            FROM flights 
            JOIN airlines ON flights.airline = airlines.id 
            WHERE flights.DEPARTURE_DELAY >= 20 AND flights.ORIGIN_AIRPORT = :airport_IATA_CODE
        """
        params = {'airport_IATA_CODE': airport_IATA_CODE}

        return self._execute_query(query, params=params)

    def __del__(self):
        """
        Closes the connection to the database when the object is about to be destroyed
        """
        self._engine.dispose()
