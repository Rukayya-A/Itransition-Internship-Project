[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared OpenAQ_APIKey = "5df4edafd52cd0d8453d3c4b8356af86781d40e3bb3d9491d65a76b5e84f5a77" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "OpenAQ_locations_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "OpenAQ_locations_DataDestinationTransform"]]]}]
shared OpenAQ_locations = let
    BBox = "-74.2591,40.4774,-73.7004,40.9176",

    Source = Json.Document(Web.Contents(
        "https://api.openaq.org/v3/locations",
        [
            Headers=[#"X-API-Key"=OpenAQ_APIKey],
            Query=[bbox=BBox]
        ]
    )),

    Results = Source[results],

    AsTable = Table.FromList(Results, Splitter.SplitByNothing(), null, null, ExtraValues.Error),

    AsJsonLines = Table.TransformColumns(
        AsTable,
        {"Column1", each Text.FromBinary(Json.FromValue(_)), type text}
    )
in
    AsJsonLines;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "6d40a6b8-5032-4131-96e6-6d99b8654d16"]}[Data]{[lakehouseId = "11607498-1180-4567-98e7-39430f64772b"]}[Data];
shared OpenAQ_locations_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6d40a6b8-5032-4131-96e6-6d99b8654d16"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "11607498-1180-4567-98e7-39430f64772b"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "airquality"]}[Content],
  FileNavigation = Navigation_5{[Name = "locations.json"]}?[Content]?
in
  FileNavigation;
shared OpenAQ_locations_DataDestinationTransform = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "OpenAQ_measurements_DataDestination (2)", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "OpenAQ_measurements_DataDestinationTransform (2)"]]]}]
shared OpenAQ_measurements = let
    BBox = "-74.2591,40.4774,-73.7004,40.9176",
    Pollutants = {"co","o3","no2","pm25"},

    // Get locations
    LocSource = Json.Document(Web.Contents(
        "https://api.openaq.org/v3/locations",
        [Headers=[#"X-API-Key"=OpenAQ_APIKey], Query=[bbox=BBox]]
    )),
    LocResults = LocSource[results],
    LocTable = Table.FromList(LocResults, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    LocExpanded = Table.ExpandRecordColumn(LocTable, "Column1", {"sensors"}),
    SensorsExpanded = Table.ExpandListColumn(LocExpanded, "sensors"),

    // Filter sensors by pollutants
    FilteredSensors = Table.SelectRows(SensorsExpanded,
        each List.Contains(Pollutants, Text.Lower([sensors][parameter][name]))
    ),

    WithSensorId = Table.AddColumn(
        FilteredSensors,
        "sensor_id",
        each [sensors][id],
        Int64.Type
    ),

    // Generate month ranges between 2021-01 and 2026-01
    MonthRanges = List.Generate(
        () => #date(2021,1,1),
        each _ < #date(2026,1,1),
        each Date.AddMonths(_,1)
    ),

    // Function to fetch one page of data for a sensor/month
    GetPage = (sensorId as text, startDate as date, endDate as date, pg as number) =>
        let
            Response = Json.Document(Web.Contents(
                "https://api.openaq.org/v3/sensors/" & sensorId & "/measurements",
                [
                    Headers=[#"X-API-Key"=OpenAQ_APIKey],
                    Query=[
                        limit="1000",
                        page=Text.From(pg),
                        datetime_from=Date.ToText(startDate,"yyyy-MM-dd"),
                        datetime_to=Date.ToText(endDate,"yyyy-MM-dd")
                    ]
                ]
            )),
            Results = try Response[results] otherwise {}
        in
            List.Transform(Results, each [
                sensor_id = sensorId,
                measurement = _
            ]),

    // Function to fetch all pages for a sensor/month
    FetchSensorMonthPages = (sensorId as text, startDate as date) =>
        let
            endDate = Date.AddMonths(startDate,1),
            Pages = List.Generate(
                () => [pg=1, data=GetPage(sensorId, startDate, endDate, 1)],
                each List.Count([data]) > 0,
                each [pg=[pg]+1, data=GetPage(sensorId, startDate, endDate, [pg]+1)],
                each [data]
            ),
            Combined = List.Combine(Pages)
        in
            Combined,

    // Combine all months for each sensor
    FetchAllMonthsForSensor = (sensorId as text) =>
        List.Combine(List.Transform(MonthRanges, each FetchSensorMonthPages(sensorId,_))),

    // Apply to all sensors
    AllResults = List.Combine(
        List.Transform(Table.ToRecords(WithSensorId),
            each FetchAllMonthsForSensor(Text.From(_[sensor_id]))
        )
    ),

    AsTable = Table.FromList(AllResults, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    AsJsonLines = Table.TransformColumns(
        Table.SelectColumns(AsTable, {"Column1"}),
        {"Column1", each Text.FromBinary(Json.FromValue(_)), type text}
    )
in
    AsJsonLines;
shared #"OpenAQ_measurements_DataDestination (2)" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6d40a6b8-5032-4131-96e6-6d99b8654d16"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "11607498-1180-4567-98e7-39430f64772b"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "airquality"]}[Content],
  FileNavigation = Navigation_5{[Name = "measurements.json"]}?[Content]?
in
  FileNavigation;
shared #"OpenAQ_measurements_DataDestinationTransform (2)" = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
