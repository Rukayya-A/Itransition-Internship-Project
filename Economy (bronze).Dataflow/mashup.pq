[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "WB_GDP_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "WB_GDP_DataDestinationTransform"]]]}]
shared WB_GDP = let
    Source = Json.Document(
        Web.Contents(
            "https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&per_page=100"
        )
    ),

    Results = Source{1},

    AsTable = Table.FromList(Results, Splitter.SplitByNothing(), null, null, ExtraValues.Error),

    AsJson = Table.TransformColumns(
        AsTable,
        {"Column1", each Text.FromBinary(Json.FromValue(_)), type text}
    )
in
    AsJson;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ECB_FX_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "ECB_FX_DataDestinationTransform"]]]}]
shared ECB_FX = let
    Source = Csv.Document(
        Web.Contents(
            "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"
        ),
        [Delimiter = ",", QuoteStyle = QuoteStyle.None]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true])

in
    Promoted;
shared ECB_FX_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6d40a6b8-5032-4131-96e6-6d99b8654d16"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "11607498-1180-4567-98e7-39430f64772b"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "economy"]}[Content],
  FileNavigation = Navigation_5{[Name = "ecb_fx.csv"]}?[Content]?
in
  FileNavigation;
shared ECB_FX_DataDestinationTransform = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
shared WB_GDP_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6d40a6b8-5032-4131-96e6-6d99b8654d16"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "11607498-1180-4567-98e7-39430f64772b"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "economy"]}[Content],
  FileNavigation = Navigation_5{[Name = "wb_gdp.JSON"]}?[Content]?
in
  FileNavigation;
shared WB_GDP_DataDestinationTransform = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
