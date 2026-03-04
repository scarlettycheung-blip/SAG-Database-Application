#!/usr/bin/env python3
import psycopg2

"""
Project: Sydney Automotive Group (SAG) Management System
Module: Data Access Layer (database.py)
Description: 
    This module handles all interactions between the Flask web application 
    and the PostgreSQL database using the Python DB-API (psycopg2). 
    It implements core business logic for user authentication, sales reporting, 
    and transaction management.
Author: Scarlett

"""

def openConnection():

    myHost = "awsprddbs4836.shared.sydney.edu.au"
    userid = "y25s1c9120_xzha0509"
    passwd = "rYGfjaX8"

    conn = None
    try:
        conn = psycopg2.connect(database=userid,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    return conn

'''
Validate salesperson based on username and password
'''
def checkLogin(login, password):
    try:
        conn = openConnection()
        curs = conn.cursor()
        curs.execute(
            "SELECT username, salesperson.firstname, salesperson.firstname FROM Salesperson WHERE username = %s AND password = %s",
            (login, password))
        result = curs.fetchone()
        conn.close()
        return result if result else None
    except psycopg2.Error as e:
        print("Database error: ", e)
        return None


"""
    Retrieves the summary of car sales.

    This method fetches the summary of car sales from the database and returns it 
    as a collection of summary objects. Each summary contains key information 
    about a particular car sale.

    :return: A list of car sale summaries.
"""


def getCarSalesSummary():
    try:
        conn = openConnection()
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT ma.MakeName                                                           AS make,
                              mo.ModelName                                                          AS model,
                              COUNT(c.CarSaleID)                                                       FILTER (WHERE c.IsSold = false) AS available_units, COUNT(c.CarSaleID) FILTER (WHERE c.IsSold = true) AS sold_units, COALESCE(SUM(c.Price) FILTER(WHERE c.IsSold = true), 0) AS total_sales,
                              TO_CHAR(MAX(c.SaleDate) FILTER (WHERE c.IsSold = true), 'DD-MM-YYYY') AS last_purchased_at
                       FROM CarSales c
                                JOIN Make ma ON c.MakeCode = ma.MakeCode
                                JOIN Model mo ON c.ModelCode = mo.ModelCode
                       GROUP BY ma.MakeName, mo.ModelName
                       ORDER BY ma.MakeName, mo.ModelName;
                       """)
        results = cursor.fetchall()
        conn.close()

        summary_list = [
            {
                'make': row[0],
                'model': row[1],
                'availableUnits': row[2],
                'soldUnits': row[3],
                'soldTotalPrices': row[4],
                'lastPurchaseAt': row[5] if row[5] else ""
            }
            for row in results
        ]
        return summary_list
    except psycopg2.Error as e:
        print("Database error: ", e)
        return []


"""
    Finds car sales based on the provided search string.

    This method searches the database for car sales that match the provided search
    string. See assignment description for search specification

    :param search_string: The search string to use for finding car sales in the database.
    :return: A list of car sales matching the search string.
"""


def findCarSales(searchString=None, username=None):
    try:
        conn = openConnection()
        cursor = conn.cursor()

        if searchString is None or searchString.strip() == "":
            if username:
                cursor.execute("""
                               SELECT c.CarSaleID,
                                      ma.MakeName,
                                      mo.ModelName,
                                      c.BuiltYear,
                                      c.Odometer,
                                      c.Price,
                                      c.IsSold,
                                      TO_CHAR(c.SaleDate, 'DD-MM-YYYY')                AS sale_date,
                                      COALESCE(cu.FirstName || ' ' || cu.LastName, '') AS customer_name,
                                      COALESCE(sp.FirstName || ' ' || sp.LastName, '') AS salesperson_name
                               FROM CarSales c
                                        LEFT JOIN Customer cu ON c.BuyerID = cu.CustomerID
                                        LEFT JOIN Salesperson sp ON c.SalespersonID = sp.UserName
                                        JOIN Make ma ON c.MakeCode = ma.MakeCode
                                        JOIN Model mo ON c.ModelCode = mo.ModelCode
                               WHERE c.IsSold = false
                                  OR (c.IsSold = true AND c.SaleDate >= (CURRENT_DATE - INTERVAL '3 years'))
                               ORDER BY c.IsSold ASC, c.SaleDate ASC NULLS LAST, ma.MakeName ASC, mo.ModelName ASC;
                               """)
            else:
                cursor.execute("""
                               SELECT c.CarSaleID,
                                      ma.MakeName,
                                      mo.ModelName,
                                      c.BuiltYear,
                                      c.Odometer,
                                      c.Price,
                                      c.IsSold,
                                      TO_CHAR(c.SaleDate, 'DD-MM-YYYY')                AS sale_date,
                                      COALESCE(cu.FirstName || ' ' || cu.LastName, '') AS customer_name,
                                      COALESCE(sp.FirstName || ' ' || sp.LastName, '') AS salesperson_name
                               FROM CarSales c
                                        LEFT JOIN Customer cu ON c.BuyerID = cu.CustomerID
                                        LEFT JOIN Salesperson sp ON c.SalespersonID = sp.UserName
                                        JOIN Make ma ON c.MakeCode = ma.MakeCode
                                        JOIN Model mo ON c.ModelCode = mo.ModelCode
                               WHERE c.IsSold = false
                                  OR (c.IsSold = true AND c.SaleDate >= (CURRENT_DATE - INTERVAL '3 years'))
                               ORDER BY c.IsSold ASC, c.SaleDate ASC NULLS LAST, ma.MakeName ASC, mo.ModelName ASC;
                               """)
        else:
            search_pattern = f"%{searchString.strip().lower()}%"
            cursor.execute("""
                           SELECT c.CarSaleID,
                                  ma.MakeName,
                                  mo.ModelName,
                                  c.BuiltYear,
                                  c.Odometer,
                                  c.Price,
                                  c.IsSold,
                                  TO_CHAR(c.SaleDate, 'DD-MM-YYYY')                AS sale_date,
                                  COALESCE(cu.FirstName || ' ' || cu.LastName, '') AS customer_name,
                                  COALESCE(sp.FirstName || ' ' || sp.LastName, '') AS salesperson_name
                           FROM CarSales c
                                    LEFT JOIN Customer cu ON c.BuyerID = cu.CustomerID
                                    LEFT JOIN Salesperson sp ON c.SalespersonID = sp.UserName
                                    JOIN Make ma ON c.MakeCode = ma.MakeCode
                                    JOIN Model mo ON c.ModelCode = mo.ModelCode
                           WHERE (LOWER(ma.MakeName) LIKE %s
                               OR LOWER(mo.ModelName) LIKE %s
                               OR LOWER(cu.FirstName || ' ' || cu.LastName) LIKE %s
                               OR LOWER(sp.FirstName || ' ' || sp.LastName) LIKE %s)
                             AND (c.IsSold = false
                               OR (c.IsSold = true AND c.SaleDate >= (CURRENT_DATE - INTERVAL '3 years')))
                           ORDER BY c.IsSold ASC, c.SaleDate ASC NULLS LAST, ma.MakeName ASC, mo.ModelName ASC;
                           """, (search_pattern, search_pattern, search_pattern, search_pattern))

        results = cursor.fetchall()
        conn.close()

        carsales_list = [
            {
                'carsale_id': row[0],
                'make': row[1],
                'model': row[2],
                'builtYear': row[3],
                'odometer': row[4],
                'price': row[5],
                'isSold': row[6],
                'sale_date': row[7] if row[7] else "",
                'buyer': row[8],
                'salesperson': row[9]
            }
            for row in results
        ]
        return carsales_list
    except psycopg2.Error as e:
        print("Database error: ", e)
        return []

"""
    Adds a new car sale to the database.

    This method accepts a CarSale object, which contains all the necessary details 
    for a new car sale. It inserts the data into the database and returns a confirmation 
    of the operation.

    :param car_sale: The CarSale object to be added to the database.
    :return: A boolean indicating if the operation was successful or not.
"""


def addCarSale(make, model, builtYear, odometer, price):
    try:
        conn = openConnection()
        cursor = conn.cursor()

        cursor.execute("SELECT add_car_sale(%s::VARCHAR, %s::VARCHAR, %s::INTEGER, %s::INTEGER, %s::DECIMAL);",
                       (make, model, builtYear, odometer, price))
        conn.commit()
        conn.close()
        return True
    except psycopg2.Error as e:
        print("Database error: ", e)
        return False


"""
    Updates an existing car sale in the database.

    This method updates the details of a specific car sale in the database, ensuring
    that all fields of the CarSale object are modified correctly. It assumes that 
    the car sale to be updated already exists.

    :param car_sale: The CarSale object containing updated details for the car sale.
    :return: A boolean indicating whether the update was successful or not.
"""


def updateCarSale(carsale_id, customer_id, salesperson, sale_date=None):
    try:
        if not customer_id or not salesperson or not sale_date:
            print("Error: All three fields (customer_id, salesperson, sale_date) must be provided.")
            return False

        conn = openConnection()
        cursor = conn.cursor()
        cursor.execute("SELECT update_car_sale(%s, %s, %s, %s);",
                       (carsale_id, customer_id.strip(), salesperson.strip(), sale_date))
        conn.commit()
        conn.close()
        return True
    except psycopg2.Error as e:
        print("Database error: ", e)
        return False



