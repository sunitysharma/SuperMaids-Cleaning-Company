# Group Project of CSC623 – Theory of Relational Databases in Fall 2023 (University of Miami)
# Part 3 of project: Design, development and implementation of a relational database
# SuperMaids Cleaning Company
# --------------------------------
# Author: Pornkamon Sirivoravijit, Roxana Rodriguez, and Sunity Sharma

import sqlite3
import pandas as pd

db_connect = sqlite3.connect('SuperMaidsCleaningCompany.db')
cursor = db_connect.cursor()

# (A) Develop SQL code to create the entire database schema,
# reflecting the constraints identified in previous steps.

queryClient = """
    CREATE TABLE Client(
        clientID INT PRIMARY KEY NOT NULL, 
        firstName VARCHAR(255) NOT NULL, 
        lastName VARCHAR(255) NOT NULL, 
        address VARCHAR(255) NOT NULL, 
        phoneNumber INTEGER NOT NULL CHECK (phoneNumber >= 0 AND LENGTH(CAST(phoneNumber AS TEXT)) <= 15)
        ); 
    """


queryEmployees = """
    CREATE TABLE Employees(
        staffNo INT PRIMARY KEY NOT NULL, 
        firstName VARCHAR(255) NOT NULL, 
        lastName VARCHAR(255) NOT NULL, 
        address VARCHAR(255) NOT NULL, 
        salary REAL NOT NULL CHECK (salary >0), 
        phoneNumber INTEGER NOT NULL CHECK (phoneNumber >= 0 AND LENGTH(CAST(phoneNumber AS TEXT)) <= 15)
        );
    """

queryRequirement  = """
    CREATE TABLE Requirement(
        requirementID INT IDENTITY(1,1) PRIMARY KEY, 
        serviceID INT REFERENCES Service(serviceID) ON UPDATE CASCADE ON DELETE NO ACTION,
        equipmentID INT REFERENCES Service(serviceID) ON UPDATE CASCADE ON DELETE SET NULL
        );
    """

create_service = """
    CREATE TABLE Service(
    serviceID INT PRIMARY KEY, 
    clientID INT REFERENCES Client(clientID) ON UPDATE CASCADE ON DELETE NO ACTION, 
    day VARCHAR(10) NOT NULL, 
    date DATE NOT NULL,
    startTime TIME,
    duration INT CHECK (duration > 0) NOT NULL,
    comment VARCHAR(255),
    CONSTRAINT fk_client
        FOREIGN KEY (clientID) 
        REFERENCES Client (clientID) 
        ON UPDATE CASCADE ON DELETE NO ACTION 
    );
"""

create_equiment = """
    CREATE TABLE Equipment(
    equipmentID INT PRIMARY KEY,
    description VARCHAR(255),
    usage VARCHAR(255),
    cost DECIMAL(10, 2) CHECK (cost >0) NOT NULL
    );
"""

# Assignment
create_assignment = """
    CREATE TABLE Assignment(
    assignmentID INTEGER PRIMARY KEY,
    serviceID INT REFERENCES Service (serviceID) ON UPDATE CASCADE ON DELETE NO ACTION,
    staffNo INT REFERENCES Employees (staffNo) ON UPDATE CASCADE ON DELETE NO ACTION NOT NULL,
    hours INT CHECK (hours < 20) NOT NULL);
"""

# Execute query, the result is stored in cursor
cursor.execute(queryClient)
cursor.execute(queryEmployees)
cursor.execute(queryRequirement)
cursor.execute(create_service)
cursor.execute(create_equiment)
cursor.execute(create_assignment)

# (B) Create at least 5 tuples for each relation in your database.

queryInsertClient = """
    INSERT INTO Client 
    VALUES 
        ('C001', 'John', 'Doe', '123 Main St', '555-1234567'),
        ('C002', 'Jane', 'Smith', '456 Oak St', '555-7654321'),
        ('C003', 'Bob', 'Johnson', '789 Pine St', '555-9876543'),
        ('C004', 'Alice', 'Williams', '101 Elm St', '555-2345678'),
        ('C005', 'Charlie', 'Smith', '222 Maple St', '555-8765432')
        ;
    """

queryInsertEmployee = """
    INSERT INTO Employees 
    VALUES 
    ('ST001', 'Alice', 'Johnson', '789 Elm St', 50000.00, '555-8765432'),
    ('ST002', 'Bob', 'Williams', '101 Pine St', 60000.00, '555-1234567'),
    ('ST003', 'Charlie', 'Smith', '222 Maple St', 55000.00, '555-2345678'),
    ('ST004', 'David', 'Miller', '333 Cedar St', 70000.00, '555-3456789'),
    ('ST005', 'Eva', 'Jones', '444 Oak St', 75000.00, '555-4567890')
    ;
    """

queryInsertRequirement = """
    INSERT INTO Requirement 
    VALUES 
    (1,'S001', 'E001'),
    (2, 'S002', 'E002'),
    (3, 'S003', 'E003'),
    (4, 'S004', 'E004'),
    (5, 'S005', 'E005')
    ;
    """

insert_service = """
    INSERT INTO Service 
    VALUES
        ('S001', 'C001', 'Monday', '2023-12-04', '08:00:00', 2, 'Regular cleaning session'),
        ('SOO2', 'C001', 'Tuesday', '2023-12-05', '08:00:00', 2, 'Regular cleaning session'),
        ('S003', 'C001', 'Wednesday', '2023-12-06', '08:00:00', 2, 'Regular cleaning session'),
        ('S004', 'C001', 'Thursday', '2023-12-07', '08:00:00', 2, 'Regular cleaning session'),
        ('S005', 'C001', 'Friday', '2023-12-08', '08:00:00', 2, 'Regular cleaning session'),
        ('S006', 'C001', 'Monday', '2023-12-04', '17:00:00', 2, 'Regular cleaning session'),
        ('SOO7', 'C001', 'Tuesday', '2023-12-05', '17:00:00', 2, 'Regular cleaning session'),
        ('S008', 'C001', 'Wednesday', '2023-12-06', '17:00:00', 2, 'Regular cleaning session'),
        ('S009', 'C001', 'Thursday', '2023-12-07', '17:00:00', 2, 'Regular cleaning session'),
        ('S010', 'C001', 'Friday', '2023-12-08', '17:00:00', 2, 'Regular cleaning session'),
        ('S011', 'C002', 'Wednesday', '2023-01-03', '10:00:00', 1, 'Deep cleaning appointment'),
        ('S012', 'C003', 'Friday', '2023-01-05', '13:00:00', 3, 'Specialized cleaning and disinfection'),
        ('S013', 'C004', 'Monday', '2023-01-08', '09:30:00', 2, 'Carpet Cleaning'),
        ('S014', 'C005', 'Thursday', '2023-01-11', '11:00:00', 1, 'Kitchen Disinfection')
        ;
    """

insert_equipment = """
    INSERT INTO Equipment
    VALUES
    ('E001', 'Cleaning supplies', 'Disinfection', 2000.00),
    ('E002', 'Vacuum cleaner', 'Cleaning', 500.00),
    ('E003', 'Sanitizing solution', 'Disinfection', 800.00),
    ('E004', 'Broom and mop set', 'Cleaning', 300.00),
    ('E005', 'Trash bags', 'Waste disposal', 50.00)
    ;
    """

insert_assignment = """
    INSERT INTO Assignment
    VALUES
    (1, 'S001', 'ST001', 2),
    (2, 'S001', 'ST002', 2),
    (3, 'S002', 'ST001', 2),
    (4, 'S002', 'ST002', 2),
    (5, 'S003', 'ST001', 2),
    (6, 'S003', 'ST002', 2),
    (7, 'S004', 'ST001', 2),
    (8, 'S004', 'ST002', 2),
    (9, 'S005', 'ST001', 2),
    (10, 'S005', 'ST002', 2),
    (11, 'S006', 'ST001', 2),
    (12, 'S006', 'ST002', 2),
    (13, 'S007', 'ST001', 2),
    (14, 'S007', 'ST002', 2),
    (15, 'S008', 'ST001', 2),
    (16, 'S008', 'ST002', 2),
    (17, 'S009', 'ST001', 2),
    (18, 'S009', 'ST002', 2),
    (19, 'S010', 'ST001', 2),
    (20, 'S010', 'ST002', 2),
    (21, 'S011', 'ST003', 1),
    (22, 'S012', 'ST003', 1),
    (23, 'S013', 'ST004', 1),
    (24, 'S014', 'ST005', 1)
    """

cursor.execute(queryInsertClient)
cursor.execute(queryInsertEmployee)
cursor.execute(queryInsertRequirement)
cursor.execute(insert_service)
cursor.execute(insert_equipment)
cursor.execute(insert_assignment)

# Print Service table after updating
cursor.execute("SELECT * FROM Service;")
column_names = [row[0] for row in cursor.description] # Extract column names from cursor
result_service = cursor.fetchall()  # Fetch data and load into a pandas dataframe
df_service = pd.DataFrame(result_service, columns=column_names)

print("\nService table:")
print(df_service)

# Print Equipment after inserting
cursor.execute("SELECT * FROM Equipment;")
column_names = [row[0] for row in cursor.description] # Extract column names from cursor
result_equipment = cursor.fetchall() # Fetch data and load into a pandas dataframe
df_equipment = pd.DataFrame(result_equipment, columns=column_names)

print("\nReservation table:")
print(df_equipment)


# Print Assignment after inserting
cursor.execute("SELECT * FROM Assignment;")
column_names = [row[0] for row in cursor.description] # Extract column names from cursor
result_assignment = cursor.fetchall() # Fetch data and load into a pandas dataframe
df_assignment = pd.DataFrame(result_assignment, columns=column_names)

print("\nAssignment table:")
print(df_assignment)


# Print Client after inserting
querySelectClient = """
    SELECT *
    FROM Client
    """
cursor.execute(querySelectClient)

column_names = [row[0] for row in cursor.description]
table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)
print("ALL ROWS IN TABLE CLIENT:")
print(df)
print(df.columns)


# Print Employees after inserting
querySelectClient = """
    SELECT *
    FROM Employees
    """
cursor.execute(querySelectClient)

column_names = [row[0] for row in cursor.description]
table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)
print("ALL ROWS IN TABLE Employees:")
print(df)
print(df.columns)


# Print Requirement after inserting
querySelectClient = """
    SELECT *
    FROM Requirement
    """
cursor.execute(querySelectClient)

column_names = [row[0] for row in cursor.description]
table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)
print("ALL ROWS IN TABLE Requirement:")
print(df)
print(df.columns)


# C) Develop 5 SQL queries using embedded SQL (see Python tutorial)
# C1) Update employee salary 20% due to increasing in working hours
update_statement = """
UPDATE Employees
SET salary = salary * 1.20
;
"""
cursor.execute(update_statement)
db_connect.commit()

# Print out the position table to check on the employee salary
query = """
    SELECT *
    FROM Employees
    """
cursor.execute(query)

# Print out the employee after decreasing Analyse salary 10%
column_names = [row[0] for row in cursor.description] # Extract column names from cursor
update_salary = cursor.fetchall()# Fetch data and load into a pandas dataframe
df_emp = pd.DataFrame(update_salary, columns=column_names)

# Examine dataframe
print("\nNew salary increased by 20%:")
print(df_emp)

# C2) Update working hours +1 for each job
update_statement = """
UPDATE Service
SET duration = duration+1
;
"""
cursor.execute(update_statement)
db_connect.commit()

# Print out the position table to check on the employee salary
query = """
    SELECT *
    FROM Service
    """
cursor.execute(query)

# Extract column names from cursor to print the table
column_names = [row[0] for row in cursor.description]

# Fetch data and load into a pandas dataframe
view_for_print = cursor.fetchall()
df_service_removal = pd.DataFrame(view_for_print, columns=column_names)

# Examine dataframe
print("\nService Table with new update on 1 hour extra work on each transaction")
print(df_service_removal)


# C3) View the schedule of the employee
# Create a view to show employee details
empSchedule_table_view = """
CREATE VIEW EmployeeScheduleTable AS
SELECT
    Ass.staffNo AS employee_identified,
    Emp.firstName || ' ' || Emp.lastName AS employee_name,
    Serv.day AS working_day,
    Serv.date AS working_date,
    Serv.startTime AS start_time,
    Cli.firstName || ' ' || Cli.lastName AS client_name,
    Cli.address AS client_address
    
FROM Assignment AS Ass
JOIN Employees AS Emp ON Ass.staffNo = Emp.staffNo
JOIN Service AS Serv ON Ass.serviceID = Serv.serviceID
JOIN Client AS Cli ON Serv.clientID = Cli.clientID;
"""

cursor.execute(empSchedule_table_view)
db_connect.commit()

# Print the Employee table
emp_table = "SELECT * FROM EmployeeScheduleTable"
cursor.execute(emp_table)

# Extract column names from cursor
column_names = [row[0] for row in cursor.description]

# Fetch data and load into a pandas dataframe
view_for_print = cursor.fetchall()
df_empSchedule = pd.DataFrame(view_for_print, columns=column_names)

# Examine dataframe
print("\nEmployee Schedule Table:")
print(df_empSchedule)


# C4) Delete record from Service table
query = """
DELETE FROM Service
WHERE serviceID = 'SOO2'
"""
cursor.execute(query)

# Select data
query1 = """
    SELECT *
    FROM Service
    """
cursor.execute(query1)

# Extract column names from cursor to print the table
column_names = [row[0] for row in cursor.description]

# Fetch data and load into a pandas dataframe
view_for_print = cursor.fetchall()
df_service_removal = pd.DataFrame(view_for_print, columns=column_names)

# Examine dataframe
print("\nService Table with new update on removal transaction of S002:")
print(df_service_removal)

# C5) Update the structure of the table in the Client table (adding email attribute)
alter_sql = """
ALTER TABLE Client
ADD email VARCHAR(100);
"""
cursor.execute(alter_sql)

query = """
INSERT INTO Client VALUES ('C006', 'Ana', 'Cruz', '125 Main St', '565-1234567','ana.crz@gmail.com'); 
      """
cursor.execute(query)

# Select data
query1 = """
    SELECT *
    FROM Client
    """
cursor.execute(query1)

# Extract column names from cursor to print the table
column_names = [row[0] for row in cursor.description]

# Fetch data and load into a pandas dataframe
view_for_print = cursor.fetchall()
df_client = pd.DataFrame(view_for_print, columns=column_names)

# Examine dataframe
print("\nClient Table with email attribute:")
print(df_client)

db_connect.commit()
db_connect.close()