-- Scalar function
CREATE FUNCTION dbo.fn_freight_ratio(@price DECIMAL(10,2), @freight DECIMAL(10,2))
RETURNS DECIMAL(10,4)
AS BEGIN
    RETURN CASE WHEN @price = 0 THEN NULL ELSE @freight / @price END;
END;

GO