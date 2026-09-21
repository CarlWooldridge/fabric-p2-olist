-- Stored procedure
CREATE PROCEDURE dbo.sp_refresh_state_summary @state VARCHAR(2)
AS BEGIN
    DELETE FROM dbo.state_summary WHERE seller_state = @state;
    INSERT INTO dbo.state_summary
    SELECT seller_state, SUM(revenue) FROM dbo.fn_orders_by_state(@state)
    GROUP BY seller_state;
END;

GO