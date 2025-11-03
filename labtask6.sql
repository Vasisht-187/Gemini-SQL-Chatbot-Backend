CREATE DATABASE Customers_DB;

USE Customers_DB;

CREATE TABLE Customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100)
);

SELECT * FROM Customers;

SET profiling = 1;

SELECT * FROM customers WHERE email = 'jeffrey80@example.com';
SHOW PROFILES;

CREATE INDEX idx_email ON customers(email);

SELECT * FROM customers WHERE email = 'jeffrey80@example.com';
SHOW PROFILES;

CREATE INDEX idx_city ON customers(city);

SELECT * FROM customers WHERE city = 'Adrianafort';
SHOW PROFILES;

ALTER TABLE customers DROP INDEX idx_email;
ALTER TABLE customers DROP INDEX idx_city;

-- Ensure engine supports partitioning
ALTER TABLE customers ENGINE = InnoDB;

ALTER TABLE Customers DROP INDEX email;

ALTER TABLE Customers
PARTITION BY RANGE (id) (
    PARTITION p0 VALUES LESS THAN (200000),
    PARTITION p1 VALUES LESS THAN (400000),
    PARTITION p2 VALUES LESS THAN (600000),
    PARTITION p3 VALUES LESS THAN (800000),
    PARTITION p4 VALUES LESS THAN MAXVALUE
);

CREATE INDEX idx_email ON customers(email);
CREATE INDEX idx_city ON customers(city);

EXPLAIN SELECT * FROM customers WHERE email = 'jeffrey80@example.com';

SELECT * FROM customers WHERE city = 'Adrianafort';

SHOW PROFILES;

SELECT PARTITION_NAME, TABLE_ROWS
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_NAME = 'Customers'
AND TABLE_SCHEMA = 'Customers_db';

