
CREATE TABLE IF NOT EXISTS processed_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    description VARCHAR(1000),
    price FLOAT, 
    category VARCHAR(255),
    created_at VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS processed_purchases (
    purchase_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50),
    credit_card_type VARCHAR(100),
    purchase_date VARCHAR(20) 
);

CREATE TABLE IF NOT EXISTS purchase_relations (
    purchase_id VARCHAR(255),
    product_id INT,
    quantity BIGINT,
    discount INT,
    PRIMARY KEY (purchase_id, product_id),
    FOREIGN KEY (purchase_id) REFERENCES purchases(purchase_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);


SELECT 1 
FROM information_schema.columns 
WHERE table_schema = 'public' 
AND table_name = 'processed_purchases' 
AND column_name = 'total';

-- Si no existe (no devuelve resultados), ejecutar:
ALTER TABLE "ecommerce"."public"."processed_purchases"
ADD COLUMN total DOUBLE PRECISION DEFAULT 0;

WITH purchase_totals AS (
    SELECT 
        pr.purchase_id,
        SUM(COALESCE(pp.price, 0) * COALESCE(pr.quantity, 0) * (1 - COALESCE(pr.discount, 0)/100.0)) AS total_with_discount,
        SUM(COALESCE(pp.price, 0) * COALESCE(pr.quantity, 0)) AS subtotal
    FROM 
        "ecommerce"."public"."purchase_relations" pr
    JOIN 
        "ecommerce"."public"."processed_products" pp ON pr.product_id = pp.product_id
    JOIN 
        "ecommerce"."public"."processed_purchases" pu ON pr.purchase_id = pu.purchase_id
    GROUP BY 
        pr.purchase_id
)

UPDATE "ecommerce"."public"."processed_purchases"
SET 
    total = pt.total_with_discount
FROM 
    purchase_totals pt
WHERE 
    "ecommerce"."public"."processed_purchases".purchase_id = pt.purchase_id;


-- Crear usuario para pruebas
CREATE USER evaluador_ro PASSWORD 'TempP@ss1234';

GRANT SELECT ON ALL TABLES IN SCHEMA public TO evaluador_ro;

GRANT USAGE ON SCHEMA public TO evaluador_ro;

--Usuario: evaluador_ro
--Contraseña: TempP@ss1234