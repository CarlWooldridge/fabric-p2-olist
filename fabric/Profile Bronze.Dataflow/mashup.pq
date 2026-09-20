[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"bronze olist_products_dataset" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "<p2-olist-workspace-id>"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "<olistlh-lakehouse-id>"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.olist_products_dataset", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"bronze olist_customers_dataset" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "<p2-olist-workspace-id>"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "<olistlh-lakehouse-id>"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.olist_customers_dataset", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"bronze olist_geolocation_dataset" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "<p2-olist-workspace-id>"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "<olistlh-lakehouse-id>"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.olist_geolocation_dataset", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"bronze olist_order_items_dataset" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "<p2-olist-workspace-id>"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "<olistlh-lakehouse-id>"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.olist_order_items_dataset", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"bronze olist_order_payments_dataset" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "<p2-olist-workspace-id>"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "<olistlh-lakehouse-id>"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.olist_order_payments_dataset", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"bronze olist_order_reviews_dataset" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "<p2-olist-workspace-id>"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "<olistlh-lakehouse-id>"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.olist_order_reviews_dataset", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"bronze olist_orders_dataset" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "<p2-olist-workspace-id>"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "<olistlh-lakehouse-id>"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.olist_orders_dataset", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"bronze olist_sellers_dataset" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "<p2-olist-workspace-id>"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "<olistlh-lakehouse-id>"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.olist_sellers_dataset", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"bronze product_category_name_translation" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "<p2-olist-workspace-id>"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "<olistlh-lakehouse-id>"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.product_category_name_translation", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"SQLAE folding bronze olist_products_dataset" = let
  Source = Sql.Databases("<sql-endpoint>.datawarehouse.fabric.microsoft.com"),
  #"Navigation 1" = Source{[Name = "OlistLH"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "bronze", Item = "olist_products_dataset"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([product_category_name] = "bebes")),
  #"Added label" = Table.AddColumn(#"Filtered rows", "label",
    each Text.Combine({[product_category_name], "-", Text.From([product_photos_qty])})),
  #"Added all categories" = Table.AddColumn(#"Added label", "all_categories",
    each Text.Combine(List.Distinct(Table.Column(#"Filtered rows", "product_category_name")), ", ")),
  #"Buffered" = Table.Buffer(#"Added label")
in
  #"Buffered";
