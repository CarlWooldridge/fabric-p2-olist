CREATE TABLE [dbo].[fact_order_items] (
    [order_key]           INT             NULL,
    [order_item_id]       INT             NULL,
    [product_sk]          INT             NULL,
    [seller_sk]           INT             NULL,
    [shipping_limit_date] DATE            NULL,
    [price]               DECIMAL (10, 2) NULL,
    [freight_value]       DECIMAL (10, 2) NULL,
    [distance_km]         FLOAT (53)      NULL,
    [is_same_state]       BIT             NULL
);


GO