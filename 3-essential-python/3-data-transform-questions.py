print(
    "################################################################################"
)
print("Use standard python libraries to do the transformations")
print(
    "################################################################################"
)

# Question: How do you read data from a CSV file at ./data/sample_data.csv into a list of dictionaries?
import csv

sample_data_list = []
file_path = './data/sample_data.csv'
with open(file_path,"r",newline="") as csvfile:
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:
        sample_data_list.append(row)


# Question: How do you remove duplicate rows based on customer ID?
customer_id_set = set()
removed_customer_set = set()
for customer in sample_data_list:
    if customer['Customer_ID'] in customer_id_set:
        removed_customer_set.add(customer)
        del(customer)
    else:
        customer_id_set.add(customer['Customer_ID'])

# Question: How do you handle missing values by replacing them with 0?
for customer in sample_data_list:
    if not customer['Purchase_Amount']:
        customer['Purchase_Amount'] = 0.0
    if not customer['Age']:
        customer['Age'] = 0


# Question: How do you remove outliers such as age > 100 or purchase amount > 1000?
for customer in sample_data_list:
    if int(customer['Age']) > 100 or float(customer['Purchase_Amount']) > 1000:
        del(customer)

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male)?
for customer in sample_data_list:
    if customer['Gender'] == 'Male':
        customer['Gender'] = 1
    elif customer['Gender'] == 'Female':
        customer['Gender'] = 0


# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns?
for customer in sample_data_list:
    split_name = customer['Customer_Name'].split(" ",1)
    customer['First_Name'] = split_name[0]
    customer['Last_Name'] = split_name[1]
    del(customer['Customer_Name'])

# Question: How do you calculate the total purchase amount by Gender?
gender_total_purchase = {0:0.0, 1:0.0}
for customer in sample_data_list:
    gender_total_purchase[customer['Gender']] += float(customer['Purchase_Amount'])
    

# Question: How do you calculate the average purchase amount by Age group?
# assume age_groups is the grouping we want
# hint: Why do we convert to float?
age_groups = {"18-30": [], "31-40": [], "41-50": [], "51-60": [], "61-70": []}
for customer in sample_data_list:
    if int(customer['Age']) >= 18 and int(customer['Age']) <= 30:
        age_groups['18-30'].append(float(customer['Purchase_Amount']))
    elif int(customer['Age']) >= 31 and int(customer['Age']) <= 40:
        age_groups['31-40'].append(float(customer['Purchase_Amount']))
    elif int(customer['Age']) >= 41 and int(customer['Age']) <= 50:
        age_groups['41-50'].append(float(customer['Purchase_Amount']))
    elif int(customer['Age']) >= 51 and int(customer['Age']) <= 60:
        age_groups['51-60'].append(float(customer['Purchase_Amount']))
    elif int(customer['Age']) >= 61 and int(customer['Age']) <= 70:
        age_groups['61-70'].append(float(customer['Purchase_Amount']))

average_purchase_by_age_group = {
    group: sum(amount)/len(amount) for group, amount in age_groups.items()
    }



# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group?
your_total_purchase_amount_by_gender = gender_total_purchase # your results should be assigned to this variable

print(f"Total purchase amount by Gender: {your_total_purchase_amount_by_gender}")
print(f"Average purchase amount by Age group: {average_purchase_by_age_group}")

print(
    "################################################################################"
)
print("Use DuckDB to do the transformations")
print(
    "################################################################################"
)

# Question: How do you connect to DuckDB and load data from a CSV file into a DuckDB table?
# Connect to DuckDB and load data
import duckdb
duckdb_con = duckdb.connect(database=":memory:", read_only=False)
# Read data from CSV file into DuckDB table
duckdb_con.execute("Create table as Sample_Data (Customer_ID INTEGER, Customer_Name VARCHAR, Age INTEGER, Gender VARCHAR, Purchase_Amount FLOAT, Purchase_Date DATE)")
duckdb_con.execute("COPY Sample_Data FROM './data/sample_data.csv' WITH HEADER CSV")
duckdb_con.close()
# Question: How do you remove duplicate rows based on customer ID in DuckDB?

# Question: How do you handle missing values by replacing them with 0 in DuckDB?

# Question: How do you remove outliers (e.g., age > 100 or purchase amount > 1000) in DuckDB?

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male) in DuckDB?

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns in DuckDB?

# Question: How do you calculate the total purchase amount by Gender in DuckDB?

# Question: How do you calculate the average purchase amount by Age group in DuckDB?

# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group in DuckDB?
print("====================== Results ======================")
print("Total purchase amount by Gender:")
print("Average purchase amount by Age group:")
