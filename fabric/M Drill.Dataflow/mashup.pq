[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Delivery Bands" = let
    // A record lookup: {[Schema=…, Item=…]} finds the row, [Data] takes its table
    Source    = Sql.Database("<sql-endpoint>.datawarehouse.fabric.microsoft.com", "OlistLH"),
    Orders    = Source{[Schema = "gold", Item = "fact_orders"]}[Data],

    // each [col] is shorthand for (_) => _[col] — a function run once per row
    Delivered = Table.SelectRows(Orders, each [order_status] = "delivered"),

    // {{column, type}, …} — a list of pairs
    Typed     = Table.TransformColumnTypes(Delivered, {{"days_to_deliver", Int64.Type}}),
    
  #"Invoked custom function" = Table.TransformColumnTypes(Table.AddColumn(Typed, "Invoked custom function", each fxBand([days_to_deliver])), {{"Invoked custom function", type text}}),

    // if … then … else if … — no CASE in M; null must be tested first
    Banded    = Table.AddColumn(#"Invoked custom function", "delivery_band", each
                    if [days_to_deliver] = null then "not delivered"
                    else if [days_to_deliver] <= 7  then "fast"
                    else if [days_to_deliver] <= 20 then "normal"
                    else "slow", type text),

    // try … otherwise catches an ERROR. order_status is text, so every row errors → null
    Tried     = Table.AddColumn(Banded, "status_as_number", each
                    try Number.FromText([order_status]) otherwise null, type nullable number),

    // Group: key columns, then {{new name, aggregation over the group "_", type}, …}
    ByBand    = Table.Group(Tried, {"delivery_band"}, {
                    {"orders",   each Table.RowCount(_),              Int64.Type},
                    {"avg_days", each List.Average([days_to_deliver]), type nullable number}}),
    Sorted    = Table.Sort(ByBand, {{"orders", Order.Descending}})
in
    Sorted;
shared fxBand = (days as nullable number) as text =>
    if days = null then "not delivered"
    else if days <= 7 then "fast"
    else if days <= 20 then "normal"
    else "slow";
shared #"Top Categories" = let
    Source   = Sql.Database("<sql-endpoint>.datawarehouse.fabric.microsoft.com", "OlistLH"),
    Lines    = Source{[Schema = "gold", Item = "fact_order_items"]}[Data],
    Products = Source{[Schema = "gold", Item = "dim_product"]}[Data],

    // NestedJoin puts the matching Products rows in ONE column of little tables ("p")…
    Joined   = Table.NestedJoin(Lines, {"product_sk"}, Products, {"product_sk"}, "p", JoinKind.Inner),
    // …and Expand pulls the columns you want out of it
    Expanded = Table.ExpandTableColumn(Joined, "p", {"category_display"}),

    Revenue  = Table.Group(Expanded, {"category_display"}, {{"revenue", each List.Sum([price]), type number}}),
    Top3     = Table.FirstN(Table.Sort(Revenue, {{"revenue", Order.Descending}}), 3)
in
    Top3;
