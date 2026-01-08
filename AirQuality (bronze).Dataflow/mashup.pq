[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared OpenAQ_APIKey = "882996c2292d72368e212b120d965e41b0a4b1bf1e638368a43acb778385b764" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any];
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

    AsJson = Table.TransformColumns(
        AsTable,
        {"Column1", each Text.FromBinary(Json.FromValue(_)), type text}
    )
in
    AsJson;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "6d40a6b8-5032-4131-96e6-6d99b8654d16"]}[Data]{[lakehouseId = "11607498-1180-4567-98e7-39430f64772b"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "OpenAQ_sensors_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "OpenAQ_sensors_DataDestinationTransform"]]]}]
shared OpenAQ_sensors = let
    BBox = "-74.2591,40.4774,-73.7004,40.9176",
    Pollutants = {"co","o3","no2","pm25"},

    // Step 1: Get locations with sensors
    LocSource = Json.Document(Web.Contents(
        "https://api.openaq.org/v3/locations",
        [
            Headers=[#"X-API-Key"=OpenAQ_APIKey],
            Query=[bbox=BBox]
        ]
    )),
    LocResults = LocSource[results],

    // Step 2: Table of locations
    LocTable = Table.FromList(LocResults, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    LocExpanded = Table.ExpandRecordColumn(LocTable, "Column1", {"id","sensors"}, {"location_id","sensors"}),

    // Step 3: Explode sensors per location
    SensorsExpanded = Table.ExpandListColumn(LocExpanded, "sensors"),

    // Step 4: Filter sensors by pollutant name
    FilteredSensors = Table.SelectRows(
        SensorsExpanded,
        each List.Contains(Pollutants, Text.Lower([sensors][parameter][name]))
    ),

    // Step 5: Add sensor_id column explicitly
    WithSensorId = Table.AddColumn(
        FilteredSensors,
        "sensor_id",
        each [sensors][id],
        Int64.Type
    ),

    // Step 6: Build record with location_id + sensor_id + full raw sensor object
    Recordified = Table.AddColumn(
        WithSensorId,
        "json",
        each [
            location_id = [location_id],
            sensor_id = [sensor_id],
            sensor = [sensors]
        ],
        type record
    ),

    // Step 7: Output as NDJSON
    AsJsonLines = Table.TransformColumns(
        Table.SelectColumns(Recordified, {"json"}),
        {"json", each Text.FromBinary(Json.FromValue(_)), type text}
    )
in
    AsJsonLines;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "OpenAQ_measurements_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "OpenAQ_measurements_DataDestinationTransform"]]]}]
shared OpenAQ_measurements = let
    BBox = "-74.2591,40.4774,-73.7004,40.9176",
    Pollutants = {"co","o3","no2","pm25"},

    LocSource = Json.Document(Web.Contents(
        "https://api.openaq.org/v3/locations",
        [Headers=[#"X-API-Key"=OpenAQ_APIKey], Query=[bbox=BBox]]
    )),
    LocResults = LocSource[results],
    LocTable = Table.FromList(LocResults, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    LocExpanded = Table.ExpandRecordColumn(LocTable, "Column1", {"sensors"}),
    SensorsExpanded = Table.ExpandListColumn(LocExpanded, "sensors"),

    FilteredSensors = Table.SelectRows(SensorsExpanded,
        each List.Contains(Pollutants, Text.Lower([sensors][parameter][name]))
    ),

    WithSensorId = Table.AddColumn(
        FilteredSensors,
        "sensor_id",
        each [sensors][id],
        Int64.Type
    ),

    FetchSensorDays = (sensorId as text, pg as number) =>
        let
            Response = Json.Document(Web.Contents(
                "https://api.openaq.org/v3/sensors/" & sensorId & "/measurements",
                [
                    Headers=[#"X-API-Key"=OpenAQ_APIKey],
                    Query=[
                        limit="1000",
                        page=Text.From(pg),
                        date_from="2021-01-01",
                        date_to="2026-01-01"
                    ]
                ]
            )),
            Results = try Response[results] otherwise {}
        in
            List.Transform(Results, each [
                sensor_id = sensorId,
                measurement = _
            ]),

    FetchAllPagesForSensor = (sensorId as text) =>
        let
            Pages = List.Generate(
                () => [pg=1, data=FetchSensorDays(sensorId, 1)],
                each List.Count([data]) > 0,
                each [pg=[pg]+1, data=FetchSensorDays(sensorId, [pg]+1)],
                each [data]
            ),
            Combined = List.Combine(Pages)
        in
            Combined,

    AllResults = List.Combine(
        List.Transform(Table.ToRecords(WithSensorId),
            each FetchAllPagesForSensor(Text.From(_[sensor_id]))
        )
    ),

    AsTable = Table.FromList(AllResults, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    AsJsonLines = Table.TransformColumns(
        Table.SelectColumns(AsTable, {"Column1"}),
        {"Column1", each Text.FromBinary(Json.FromValue(_)), type text}
    )


in
    AsJsonLines;
shared OpenAQ_measurements_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6d40a6b8-5032-4131-96e6-6d99b8654d16"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "11607498-1180-4567-98e7-39430f64772b"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "airquality"]}[Content],
  FileNavigation = Navigation_5{[Name = "measurements.JSON"]}?[Content]?
in
  FileNavigation;
shared OpenAQ_measurements_DataDestinationTransform = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
shared OpenAQ_sensors_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6d40a6b8-5032-4131-96e6-6d99b8654d16"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "11607498-1180-4567-98e7-39430f64772b"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "airquality"]}[Content],
  FileNavigation = Navigation_5{[Name = "sensors.JSON"]}?[Content]?
in
  FileNavigation;
shared OpenAQ_sensors_DataDestinationTransform = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
shared OpenAQ_locations_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6d40a6b8-5032-4131-96e6-6d99b8654d16"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "11607498-1180-4567-98e7-39430f64772b"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "airquality"]}[Content],
  FileNavigation = Navigation_5{[Name = "locations.JSON"]}?[Content]?
in
  FileNavigation;
shared OpenAQ_locations_DataDestinationTransform = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
