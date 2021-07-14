CREATE EXTERNAL TABLE IF NOT EXISTS amazon.parent_child_asin_mapping_json
(
    ASIN string,
    BrowseNodeInfo
         struct<
             BrowseNodes:
             array<
                 struct<
                     Ancestor:
                     struct<
                         ContextFreeName: string,
                         DisplayName: string,
                         Id: string
                     >,
                     ContextFreeName: string,
                     DisplayName: string,
                     Id: string,
                     SalesRank: bigint,
                     Children:
                     array<
                         struct<
                             ContextFreeName: string,
                             DisplayName: string,
                             Id: string
                         >
                     >
                 >
             >
         >,
    CustomerReviews
         struct<
             count: int,
             starRating:
             struct<
                 value: double
             >
         >,
    DetailPageURL string,
    Images
        struct<
            Primary:
            struct<
                Large:
                struct<Height: int, Width: int, URL: string>,
                Medium: struct<Height: int, Width: int, URL: string>,
                Small: struct<Height: int, Width: int, URL: string>
            >,
            Variants:
            array<
                struct<
                    Large: struct<Height: int, Width: int, URL: string>,
                    Medium: struct<Height: int, Width: int, URL: string>,
                    Small: struct<Height: int, Width: int, URL: string>
                >
            >
        >,
    ItemInfo
        struct<
            ByLineInfo:
            struct<
                Brand:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >,
                Contributors:
                array<
                    struct<
                        Locale: string,
                        Name: string,
                        Role: string,
                        RoleType: string
                    >
                >,
                Manufacturer:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >
            >,
            Classifications:
            struct<
                Binding:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >,
                ProductGroup:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >
            >,
            ContentInfo:
            struct<
                Edition:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >,
                Languages:
                struct<
                    DisplayValues:
                    array<
                        struct<
                            DisplayValue:string,
                            Type: string
                        >
                    >,
                    Label: string,
                    Locale: string
                >,
                PagesCount:
                struct<
                    DisplayValue: int,
                    Label: string,
                    Locale: string
                >,
                PublicationDate:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >
            >,
            ContentRating:
            struct<
                AudienceRating:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >
            >,
            ExternalIds:
            struct<
                EaNs:
                struct<
                    DisplayValues:
                    array<
                        string
                    >,
                    Label: string,
                    Locale: string
                >,
                IsBNs:
                struct<
                    DisplayValues:
                    array<
                        string
                    >,
                    Label: string,
                    Locale: string
                >,
                UpCs:
                struct<
                    DisplayValues:
                    array<
                        string
                    >,
                    Label: string,
                    Locale: string
                >
            >,
            Features:
            struct<
                DisplayValues:
                array<
                    string
                >,
                Label: string,
                Locale: string
            >,
            ManufactureInfo:
            struct<
                ItemPartNumber:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >,
                Model:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >,
                Warranty:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >
            >,
            ProductInfo:
            struct<
                Color:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >,
                IsAdultProduct:
                struct<
                    Label: string,
                    Locale: string
                >,
                ItemDimensions:
                struct<
                    Height:
                    struct<
                        DisplayValue: bigint,
                        Label: string,
                        Locale: string,
                        Unit: string
                    >,
                    Length:
                    struct<
                        DisplayValue: bigint,
                        Label: string,
                        Locale: string,
                        Unit: string
                    >,
                    Weight:
                    struct<
                        DisplayValue: bigint,
                        Label: string,
                        Locale: string,
                        Unit: string
                    >,
                    Width:
                    struct<
                        DisplayValue: bigint,
                        Label: string,
                        Locale: string,
                        Unit: string
                    >
                >,
                Size:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >,
                ReleaseDate:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >,
                UnitCount:
                struct<
                    DisplayValue: int,
                    Label: string,
                    Locale: string
                >
            >,
            TechnicalInfo:
            struct<
                Formats:
                struct<
                    DisplayValues:
                    array<
                        string
                    >,
                    Label: string,
                    Locale: string
                >,
                EnergyEfficiencyClass:
                struct<
                    DisplayValue: string,
                    Label: string,
                    Locale: string
                >
            >,
            Title:
            struct<
                DisplayValue: string,
                Label: string,
                Locale: string
            >,
            TradeInInfo:
            struct<
                Price:
                struct<
                    Amount: double,
                    Currency: string,
                    DisplayAmount: string
                >
            >
        >,
    Offers
        struct<
            Listings:
            array<
                struct<
                    Availability:
                    struct<
                        Message: string,
                        MaxOrderQuantity: int,
                        MinOrderQuantity: int,
                        Type: string
                    >,
                    Condition:
                    struct<
                        ConditionNote:
                        struct<
                            Locale: string,
                            Value: string
                        >,
                        SubCondition:
                        struct<
                            DisplayValue: string,
                            Label: string,
                            Locale: string,
                            Value: string
                        >,
                        DisplayValue: string,
                        Label: string,
                        Locale: string,
                        Value: string
                    >,
                    DeliveryInfo:
                    struct<
                        ShippingCharges:
                        array<
                            struct<
                                Amount: double,
                                Currency: string,
                                DisplayAmount: string,
                                Type: string
                            >
                        >
                    >,
                    Id: string,
                    Price:
                    struct<
                        Amount: double,
                        Currency: string,
                        DisplayAmount: string,
                        PricePerUnit: double,
                        Savings:
                        struct<
                            Amount: double,
                            Currency: string,
                            DisplayAmount: string,
                            Percentage: int,
                            PricePerUnit: double
                        >
                    >,
                    LoyaltyPoints:
                    struct<
                        Points: int
                    >,
                    MerchantInfo:
                    struct<
                        DefaultShippingCountry: string,
                        FeedbackCount: int,
                        FeedbackRating: double,
                        Id: string,
                        Name: string
                    >,
                    ProgramEligibility:
                    struct<
                        IsPrimeExclusive: boolean,
                        IsPrimePantry: boolean,
                        ViolatesMAP: boolean
                    >,
                    Promotions:
                    array<
                        struct<
                            Amount: double,
                            Currency: string,
                            DiscountPercent: int,
                            DisplayAmount: string,
                            PricePerUnit: double,
                            Type: string
                        >
                    >,
                    SavingBasis:
                    struct<
                        Savings:
                        struct<
                            Amount: double,
                            Currency: string,
                            DisplayAmount: string,
                            Percentage: int,
                            PricePerUnit: double
                        >
                    >
                >
            >,
            Summaries:
            array<
                struct<
                    Condition:
                    struct<
                        ConditionNote:
                        struct<
                            Locale: string,
                            Value: string
                        >,
                        SubCondition:
                        struct<
                            DisplayValue: string,
                            Label: string,
                            Locale: string,
                            Value: string
                        >,
                        DisplayValue: string,
                        Label: string,
                        Locale: string,
                        Value: string
                    >,
                    HighestPrice:
                    struct<
                        Savings:
                        struct<
                            Amount: double,
                            Currency: string,
                            DisplayAmount: string,
                            Percentage: int,
                            PricePerUnit: double
                        >,
                        Amount: double,
                        Currency: string,
                        DisplayAmount: string,
                        PricePerUnit: double
                    >,
                    LowestPrice:
                    struct<
                        Savings:
                        struct<
                            Amount: double,
                            Currency: string,
                            DisplayAmount: string,
                            Percentage: int,
                            PricePerUnit: double
                        >,
                        Amount: double,
                        Currency: string,
                        DisplayAmount: string,
                        PricePerUnit: double
                    >,
                    OfferCount: int
                >
            >
        >,
    ParentASIN string,
    RentalOffers
        struct<
            Listings:
            array<
                struct<
                    Availability:
                    struct<
                        Message: string,
                        MaxOrderQuantity: int,
                        MinOrderQuantity: int,
                        Type: string
                    >,
                    BasePrice:
                    struct<
                        Price:
                        struct<
                            Amount: double,
                            Currency: string,
                            DisplayAmount: string,
                            PricePerUnit: double,
                            Savings:
                            struct<
                                Amount: double,
                                Currency: string,
                                DisplayAmount: string,
                                Percentage: int,
                                PricePerUnit: double
                            >
                        >,
                        Duration:
                        struct<
                            DisplayValue: double,
                            Label: string,
                            Locale: string,
                            Unit: string
                        >,
                        Condition:
                        struct<
                            ConditionNote:
                            struct<
                                Locale: string,
                                Value: string
                            >,
                            SubCondition:
                            struct<
                                DisplayValue: string,
                                Label: string,
                                Locale: string,
                                Value: string
                            >,
                            DisplayValue: string,
                            Label: string,
                            Locale: string,
                            Value: string
                        >,
                        DeliveryInfo:
                        struct<
                            ShippingCharges:
                            array<
                                struct<
                                    Amount: double,
                                    Currency: string,
                                    DisplayAmount: string,
                                    Type: string
                                >
                            >
                        >,
                        Id: string,
                        MerchantInfo:
                        struct<
                            DefaultShippingCountry: string,
                            Id: string,
                            FeedbackCount: int,
                            FeedbackRating: double,
                            Name: string
                        >
                    >
                >
            >
        >,
    Score double,
    VariationAttributes
        array<
            struct<
                Name: string,
                Value: string
            >
        >
)
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
        "case.insensitive" = "true"
        )
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/amazon.db/parent_child_asin_mapping_json';


CREATE EXTERNAL TABLE IF NOT EXISTS amazon.parent_child_asin_mapping_raw
(
    event_timestamp timestamp,
    parent_asin string,
    child_asin string,
    product_title string,
    brand_name string
)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/amazon.db/parent_child_asin_mapping_raw'
    TBLPROPERTIES ('parquet.compression'='SNAPPY', 'skip.header.line.count'='1');


CREATE VIEW IF NOT EXISTS amazon.parent_child_asin_mapping
AS
SELECT all_data.event_timestamp AS event_timestamp,
       all_data.parent_asin AS parent_asin,
       all_data.child_asin AS child_asin,
       all_data.product_title AS product_title,
       all_data.brand_name AS brand_name
FROM amazon.parent_child_asin_mapping_raw AS all_data
         INNER JOIN
     (
         SELECT parent_asin,
                child_asin,
                MAX(event_timestamp) AS max_event_timestamp
         FROM amazon.parent_child_asin_mapping_raw
         GROUP BY parent_asin, child_asin
     ) max_data
     ON all_data.event_timestamp = max_data.max_event_timestamp
         AND all_data.parent_asin = max_data.parent_asin
         AND all_data.child_asin = max_data.child_asin;

CREATE TABLE IF NOT EXISTS amazon.custom_asins
(
    asin STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/amazon.db/custom_asins'
    TBLPROPERTIES ('parquet.compression'='SNAPPY', 'skip.header.line.count'='1');

SELECT ca.asin, pcamr.parent_asin, pcamr.child_asin FROM amazon.custom_asins ca
LEFT JOIN amazon.parent_child_asin_mapping_raw pcamr
ON ca.asin = pcamr.child_asin;
