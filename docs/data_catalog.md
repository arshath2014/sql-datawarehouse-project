Gold Layer Data Catalog

1) gold.dim_customers

Purpose: Stores customer master data enriched with demographic attributes. Used for customer-level analytics and joins with fact tables.

| Column Name     | Data Type    | Description                                                                     |
| --------------- | ------------ | ------------------------------------------------------------------------------- |
| customer_key    | INT          | Surrogate key uniquely identifying each customer record in the dimension table. |
| customer_id     | INT          | Source-system unique numerical identifier for the customer.                     |
| customer_number | NVARCHAR(50) | Business customer identifier used for tracking and external references.         |
| first_name      | NVARCHAR(50) | Customer’s given name.                                                          |
| last_name       | NVARCHAR(50) | Customer’s family name.                                                         |
| country         | NVARCHAR(50) | Country of residence.                                                           |
| marital_status  | NVARCHAR(50) | Marital status (e.g., Single, Married).                                         |
| gender          | NVARCHAR(50) | Gender value (e.g., Male, Female, n/a).                                         |
| birthdate       | DATE         | Customer date of birth (YYYY-MM-DD).                                            |
| create_date     | DATE         | Record creation timestamp in the warehouse.                                     |


2) gold.dim_products

Purpose: Stores product master data and classification attributes for product-level reporting and slicing of sales facts.

| Column Name    | Data Type          | Description                                                                    |
| -------------- | ------------------ | ------------------------------------------------------------------------------ |
| product_key    | INT                | Surrogate key uniquely identifying each product record in the dimension table. |
| product_id     | INT                | Source-system product identifier.                                              |
| product_number | NVARCHAR(50)       | Business product code or SKU.                                                  |
| product_name   | NVARCHAR(255)      | Full product display name.                                                     |
| category_id    | NVARCHAR(50)       | Category code from source classification.                                      |
| category       | NVARCHAR(100)      | High-level product category (e.g., Bikes, Components).                         |
| subcategory    | NVARCHAR(100)      | Product subcategory grouping.                                                  |
| maintenance    | NVARCHAR(10) / BIT | Indicates whether the product requires maintenance (Yes/No flag).              |
| product_cost   | DECIMAL / NUMERIC  | Standard product cost amount.                                                  |
| product_line   | NVARCHAR(50)       | Product line grouping (e.g., Road, Mountain).                                  |
| start_date     | DATE               | Date when the product became active/available. 



3) gold.fact_sales

Purpose: Transactional fact table containing order line sales metrics. 
Links to product and customer dimensions for analytical reporting.

| Column Name   | Data Type         | Description                                       |
| ------------- | ----------------- | ------------------------------------------------- |
| order_number  | NVARCHAR(50)      | Business order identifier.                        |
| product_key   | INT               | Foreign key to `gold.dim_products.product_key`.   |
| customer_key  | INT               | Foreign key to `gold.dim_customers.customer_key`. |
| order_date    | DATE              | Date the order was placed.                        |
| shipping_date | DATE              | Date the order was shipped.                       |
| due_date      | DATE              | Promised delivery due date.                       |
| sales_amount  | DECIMAL / NUMERIC | Total line sales amount.                          |
| quantity      | INT               | Number of units sold in the order line.           |
| price         | DECIMAL / NUMERIC | Unit selling price.                               |


