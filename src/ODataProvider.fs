module ODataProvider

open Hobbes
open Hobbes.FSharp.Compile
open System.IO
open FSharp.Data

type Value = Parsing.AST.Value
let md5Hash (input : string) =
    use md5Hash = System.Security.Cryptography.MD5.Create()
    let data = md5Hash.ComputeHash(System.Text.Encoding.UTF8.GetBytes(input))
    let sBuilder = System.Text.StringBuilder()
    (data
    |> Seq.fold(fun (sBuilder : System.Text.StringBuilder) d ->
            sBuilder.Append(d.ToString("x2"))
    ) sBuilder).ToString()  

type private ODataResponse = JsonProvider<"""{
    "@odata.context":"https://analytics.dev.azure.com/kmddk/kmdlogic/_odata/v2.0/$metadata#WorkItemRevisions(State,ChangedDate,WorkItemId,WorkItemType,StateCategory,Iteration)",
    "value":[{"WorkItemId":54820,"WorkItemType":"Task","ChangedDate":"2018-09-14T06:44:46.557+02:00","State":"New","StateCategory":"Proposed","Iteration":{"ProjectSK":"85b47b07-63fb-4dd2-8461-4c7204161d0b","IterationSK":"09803824-181d-4297-a5d5-9d19fe88fec2","IterationId":"09803824-181d-4297-a5d5-9d19fe88fec2","IterationName":"Team Australia","Number":1518,"IterationPath":"KMDLoGIC\\Team Australia","StartDate":"2019-12-09T00:00:00+01:00","EndDate":"2020-07-31T23:59:59.999+02:00","IterationLevel1":"KMDLoGIC","IterationLevel2":"Team Australia","IterationLevel3":null,"IterationLevel4":null,"IterationLevel5":null,"IterationLevel6":null,"IterationLevel7":null,"IterationLevel8":null,"IterationLevel9":null,"IterationLevel10":null,"IterationLevel11":null,"IterationLevel12":null,"IterationLevel13":null,"IterationLevel14":null,"Depth":1,"IsEnded":true}},{"WorkItemId":54820,"WorkItemType":"Task","ChangedDate":"2018-09-14T06:44:46.557+02:00","State":"New","StateCategory":"Proposed","Iteration":{"ProjectSK":"85b47b07-63fb-4dd2-8461-4c7204161d0b","IterationSK":"09803824-181d-4297-a5d5-9d19fe88fec2","IterationId":"09803824-181d-4297-a5d5-9d19fe88fec2","IterationName":"Team Australia","Number":1518,"IterationPath":"KMDLoGIC\\Team Australia","StartDate":"2019-12-09T00:00:00+01:00","EndDate":"2020-07-31T23:59:59.999+02:00","IterationLevel1":"KMDLoGIC","IterationLevel2":"Team Australia","IterationLevel3":null,"IterationLevel4":null,"IterationLevel5":null,"IterationLevel6":null,"IterationLevel7":null,"IterationLevel8":null,"IterationLevel9":null,"IterationLevel10":null,"IterationLevel11":null,"IterationLevel12":null,"IterationLevel13":null,"IterationLevel14":null,"Depth":1,"IsEnded":true}}],
    "@odata.nextLink":"https://analytics.dev.azure.com/kmddk/kmdlogic/_odata/v2.0/WorkItemRevisions?$expand=Iteration&$select=State%2CChangedDate%2CWorkITemId%2CWorkItemType%2CState%2CStateCategory%2CIteration&$filter=Iteration%2FStartDate%20gt%202019-01-01Z&$skiptoken=89900635"
    } """>

let readValue (json:JsonValue)  = 
    let rec inner json namePrefix = 
        let wrap (v : #System.IComparable) = 
          [|System.String.Join(".", namePrefix |> List.rev),v :> System.IComparable|]
        match json with
        | FSharp.Data.JsonValue.String s ->  
            match System.Double.TryParse(s) with
            true,f -> f |> wrap
            | _ -> 
                match System.DateTime.TryParse(s) with
                true,dt -> dt |> wrap
                | _ -> 
                    match System.Int32.TryParse(s) with
                    true,n -> n |> wrap
                    | _ -> s |> wrap
        | FSharp.Data.JsonValue.Number d -> d |> wrap
        | FSharp.Data.JsonValue.Float f -> f |> wrap
        | FSharp.Data.JsonValue.Boolean b ->  b |> wrap
        | FSharp.Data.JsonValue.Record properties ->
            properties
            |> Array.collect(fun (name,v) ->
                name::namePrefix |> inner v 
            )
        | FSharp.Data.JsonValue.Array elements ->
             elements
             |> Array.indexed
             |> Array.collect(fun (i,v) ->
                 (string i)::namePrefix |> inner v 
             ) 
        | FSharp.Data.JsonValue.Null -> wrap ""
    inner json []

let read (source : Source) = 
    let getStringFromValue name = 
        source.Properties
        |> Map.tryFind name
        |> Option.bind(function
                    Value.String s -> s |> Some
                    | Value.Null -> None
                    | v -> failwithf "Expecting a string but got %A" v
        )
 
    let requestData f =
        let rec readChunks url = 
            seq { 
                let record = 
                    let data = f url
                    let record = data |> ODataResponse.Parse
                    let nextLink = record.OdataNextLink
                    record
                    
                yield! record.Value 
                if record.OdataNextLink |> System.String.IsNullOrWhiteSpace |> not then 
                    yield! record.OdataNextLink |> readChunks 
            }

        let query = 
            [ 
                "filter",getStringFromValue "filter"
                "select",getStringFromValue "select"
                "expand",getStringFromValue "expand"
            ]

        let url = 
            (getStringFromValue "url").Value
            + System.String.Join("&",
                                query
                                |> List.filter(fun (_,v) -> v.IsSome)
                                |> List.map(fun (name,value) ->
                                    let value = value.Value
                                    sprintf "$%s=%s" name value
                                )
              )
        
        url
        |> readChunks
                    

    if source.ProviderName.ToLower() = "odata" then
        let user = getStringFromValue "user"
        let pwd = getStringFromValue "pwd"
        
        let values = requestData (fun url ->
            printfn "Reading data from %s" url
            Http.RequestString(url,
                headers = [
                  if user.IsSome then yield HttpRequestHeaders.BasicAuth user.Value pwd.Value
                ],
                httpMethod = "GET"
            )
        )

        values
        |> Seq.collect (fun v -> v.JsonValue |> readValue)
        |> Seq.groupBy fst
        |> Seq.map(fun (columnName,cells) -> 
            columnName,cells 
                       |> Seq.mapi(fun i (_,value) ->
                           Hobbes.Parsing.AST.KeyType.Create i,value
                       )
        )
    else
       failwithf "Expected an OData provider but got %s" source.ProviderName