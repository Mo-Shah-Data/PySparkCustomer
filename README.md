# PySparkCustomer

Project: Customer and Transaction Data Integration

Objective: Join two separate datasets to create a complete view of customer activity.

Dataset 1 – Customer Data
Contains customer information such as:

* Customer ID
* Customer name
* Email
* City
* State
* Registration date

Dataset 2 – Transaction Data
Contains:

* Transaction ID
* Customer ID
* Product
* Transaction date
* Quantity
* Amount

Join key: Customer ID

The project would use PySpark to load both datasets, validate and clean the join key, remove duplicates, and join the datasets. An inner join can identify customers with transactions, while a left join can retain every customer, including those who have never purchased anything.

After joining, analyze the combined data to answer questions such as: Which customers spend the most? Which cities generate the most revenue? Which customers have no transactions? What products are most popular? How many transactions does each customer make?

Final output: A cleaned, joined customer-transaction dataset plus a summarized dataset containing customer ID, customer name, total transactions, total quantity purchased, and total spending.