-- Inline table-valued function
CREATE FUNCTION dbo.fn_orders_by_state(@state VARCHAR(2))
RETURNS TABLE
AS RETURN
    SELECT * FROM dbo.v_seller_revenue WHERE seller_state = @state;

GO