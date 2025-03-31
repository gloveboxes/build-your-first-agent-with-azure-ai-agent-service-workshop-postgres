CREATE SCHEMA IF NOT EXISTS contoso;

CREATE TABLE IF NOT EXISTS contoso.sales_data (  
    id SERIAL PRIMARY KEY,  
    main_category TEXT,  
    product_type TEXT,  
    revenue REAL,  
    shipping_cost REAL,
    number_of_orders INTEGER,  
    year INTEGER,  
    month INTEGER,  
    discount INTEGER,  
    region TEXT,  
    month_date TEXT  
);


CREATE INDEX idx_main_category ON contoso.sales_data(main_category);
CREATE INDEX idx_month_date ON contoso.sales_data(month_date);
CREATE INDEX idx_product_type ON contoso.sales_data(product_type);
CREATE INDEX idx_region ON contoso.sales_data(region);
CREATE INDEX idx_year ON contoso.sales_data(year);