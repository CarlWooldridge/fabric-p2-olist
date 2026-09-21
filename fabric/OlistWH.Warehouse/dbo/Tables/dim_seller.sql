CREATE TABLE [dbo].[dim_seller] (
    [seller_sk]         INT            NULL,
    [seller_id]         VARCHAR (8000) NULL,
    [seller_name]       VARCHAR (8000) NULL,
    [seller_zip_prefix] VARCHAR (8000) NULL,
    [seller_city]       VARCHAR (8000) NULL,
    [seller_state]      VARCHAR (8000) NULL,
    [seller_lat]        FLOAT (53)     NULL,
    [seller_lng]        FLOAT (53)     NULL
);


GO

ALTER TABLE [dbo].[dim_seller]
    ADD CONSTRAINT [uq_dim_seller] UNIQUE NONCLUSTERED ([seller_sk] ASC) NOT ENFORCED;


GO