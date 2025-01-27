# Dropping Schema if the schema already exists, then creating metro_dwh.
Drop schema if exists `metro_dwh`;
Create schema `metro_dwh`;
Use `metro_dwh`;

# Dropping tables if the tables already exists.
Drop table if exists `product`;
Drop table if exists `customers`;
Drop table if exists `store`;
Drop table if exists `supplier`;
Drop table if exists `transaction_time`;
Drop table if exists `sales`;

# Creating Tables from the Star Schema
Use metro_dwh;
Create table Product(ProductID int(10) Primary key, ProductName varchar(255) not null, ProductPrice double(10,2) not null);
Create table Customers(CustomerID int(10) Primary key, CustomerName varchar(255) not null, Gender varchar(255) not null);
Create table Store(StoreID int(10) Primary key, StoreName varchar(255) not null);
Create table Supplier(SupplierID int(10) Primary key, SupplierName varchar(255) not null);
Create table Transaction_Time(TimePK int(10) Primary Key, TimeID int(10), TransactionDate date, TransactionDay varchar(255) not null, TransactionMonth varchar(255) not null, TransactionQuarter int(50) not null, TransactionYear int(50) not null);
Create table Sales(OrderID int(10), OrderDate date, ProductID int(10), CustomerID int(10), StoreID int(10), SupplierID int(10), TimePK int(10), Sales double(10, 2), Quantity int(10),
    constraint Product_fk foreign key (ProductID) references Product(ProductID), 
    constraint Customer_fk foreign key (CustomerID) references Customers(CustomerID),
    constraint Store_fk foreign key (StoreID) references Store(StoreID),
    constraint Supplier_fk foreign key (SupplierID) references Supplier(SupplierID),
    constraint Time_fk foreign key (TimePK) references Transaction_Time(TimePK));