-- View
CREATE VIEW dbo.v_seller_revenue AS
SELECT s.seller_id, s.seller_name, s.seller_state, SUM(f.price) AS revenue, COUNT(*) AS line_items
FROM dbo.fact_order_items f
JOIN dbo.dim_seller s ON s.seller_sk = f.seller_sk
GROUP BY s.seller_id, s.seller_name, s.seller_state;

GO