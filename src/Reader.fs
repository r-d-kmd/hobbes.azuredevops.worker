namespace Readers.AzureDevOps

open FSharp.Data
open Readers.AzureDevOps.Data
open Hobbes.Helpers.Environment
open Hobbes.Web.RawdataTypes
open Hobbes.Web
open Hobbes.Web.Cache
open Thoth.Json.Net

module Reader =

    //looks whether it's the last record or there's a odatanextlink porperty 
    //which signals that the data has been paged and that we're not at the last page yet
    let private tryNextLink (data : string) = 
        let data = AzureDevOpsAnalyticsRecord.Parse data
        if System.String.IsNullOrWhiteSpace(data.OdataNextLink) |> not then
            Some data.OdataNextLink
        else 
            None
    //If there's no work item records returned, this will return true
    let isEmpty (data : string) = 
        let data = AzureDevOpsAnalyticsRecord.Parse data
        data.Value |> Array.isEmpty
    
    
    let private azureFields : (string * (AzureDevOpsAnalyticsRecord.Value -> obj) ) list= 
        [
             "WorkItemId",  fun row -> row.WorkItemId |> box
             "ChangedDate", fun row -> match row.ChangedDate with None -> null | Some v -> box v
             "WorkItemType",  fun row -> row.WorkItemType |> box
             "CreatedDate", fun row -> match row.CreatedDate with None -> null | Some v -> box v
             "ClosedDate", fun row -> match row.ClosedDate with None -> null | Some v -> box v
             "State", fun row -> match row.State with None -> null | Some v -> box v
             "StateCategory",fun row -> match row.StateCategory with None -> null | Some v -> box v
             "LeadTimeDays", fun row -> match row.LeadTimeDays with None -> null | Some v -> box v
             "CycleTimeDays", fun row -> match row.CycleTimeDays with None -> null | Some v -> box v
             "StoryPoints", fun row -> match row.StoryPoints with None -> null | Some v -> box v
             "RevisedDate", fun row -> match row.RevisedDate with None -> null | Some v -> box v
             "Priority", fun row -> match row.Priority with None -> null | Some v -> box v
             "IsLastRevisionOfDay" , fun row -> match row.IsLastRevisionOfDay with None -> null | Some v -> box v
             "Title",  fun row -> row.Title |> box
             "WorkItemRevisionSK", fun row -> row.WorkItemRevisionSk |> box
        ]    
    //The first url to start with, if there's already some stored data
    let private getInitialUrl (source : AzureDevOpsSource.Root)=
        let server = 
            if System.String.IsNullOrWhiteSpace source.Server then 
                let account = 
                    let acc = source.Account.Replace("_", "-")
                    if System.String.IsNullOrWhiteSpace(acc) then "kmddk"
                    else acc
                sprintf "https://analytics.dev.azure.com/%s/%s" account source.Project
            else
                source.Server
        let odataQuery = 
            if source.Query |> Array.isEmpty |> not then
                source.Query
                |> String.concat System.Environment.NewLine
                |> Hobbes.OData.Compile.expressions
            else
                {
                    Fields =  "iteration"::(azureFields |> List.map fst)
                    Filters =
                        [
                            Hobbes.OData.Compile.And(
                                Hobbes.OData.Compile.And(
                                    Hobbes.OData.Compile.Eq("IsLastRevisionOfDay", "true"),
                                    Hobbes.OData.Compile.Ne("WorkItemType", "'Task'")),
                                    Hobbes.OData.Compile.Eq("IsCurrent", "true")
                            )
                        ]
                    OrderBy = []
                }
        
                                        
                
        let initialUrl (lastId:int64) = 
            let query = 
                let emitIfSpecified fieldName =
                    Option.bind(fun (field : string) -> 
                        field
                        |> System.Web.HttpUtility.UrlEncode
                        |> sprintf "$%s=%s" fieldName
                        |> Some
                    )

                System.String.Join("&",
                    [
                      yield "filter", Hobbes.OData.Compile.Gt("WorkItemRevisionSK",string(lastId))::odataQuery.Filters
                                      |> List.reduce(fun filter c -> 
                                          Hobbes.OData.Compile.And(filter,c)
                                      ) |> string
                      if odataQuery.Fields |> List.isEmpty |> not then yield "select", System.String.Join(",", odataQuery.Fields)
                      if odataQuery.OrderBy |> List.isEmpty |> not then yield "orderby", System.String.Join(",", odataQuery.OrderBy)
                    ]
                    |> List.map(fun (a,b) -> sprintf "$%s=%s" a (System.Web.HttpUtility.UrlEncode b))
                )
            
            sprintf "%s/_odata/v2.0/WorkItemRevisions?$expand=Iteration,Area&%s" server query

        let key = source.JsonValue.ToString() |> keyFromSourceDoc 
        try
            match key |> Data.tryLatestId with
            Some workItemRevisionId -> 
                initialUrl workItemRevisionId
            | None -> 
                Log.debugf "Didn't get a work item revision id for %s" key
                initialUrl 0L
        with e -> 
            Log.excf e "Failed to get latest for (%s)" key 
            initialUrl 0L

    //sends a http request   
    let private request user pwd httpMethod body url  =
        let headers =
            [
                HttpRequestHeaders.BasicAuth user pwd
                HttpRequestHeaders.ContentType HttpContentTypes.Json
            ]       
        match body with
        None ->
            Http.AsyncRequest(url,
                httpMethod = httpMethod, 
                silentHttpErrors = true,
                headers = headers
            ) |> Async.RunSynchronously
        | Some body ->
            Http.Request(url,
                httpMethod = httpMethod, 
                silentHttpErrors = true, 
                body = TextRequest body,
                headers = headers
            )

    let formatRawdataCache (timeStamp : string ) rawdataCache =
        
        let columnNames = 
            [
                "TimeStamp"
                "Area.AreaPath"
                "Iteration.IterationPath"
                "Iteration.IterationLevel1"
                "Iteration.IterationLevel2"
                "Iteration.IterationLevel3"
                "Iteration.IterationLevel4"
                "Iteration.StartDate"
                "Iteration.EndDate"
                "Iteration.Number"
            ] @ (azureFields |> List.map fst)
            |> Array.ofList
        let rows = 
            rawdataCache
            |> Seq.map(fun (row : AzureDevOpsAnalyticsRecord.Value) ->
                let iterationProperties =
                    match (row.Iteration) with
                    Some iteration ->
                        [
                           iteration.IterationPath |> box
                           match iteration.IterationLevel1 with None -> null | Some v -> box v
                           match iteration.IterationLevel2 with None -> null | Some v -> box v
                           match iteration.IterationLevel3 with None -> null | Some v -> box v
                           match iteration.IterationLevel4 with None -> null | Some v -> box v
                           match iteration.StartDate with None -> null | Some v -> box v
                           match iteration.EndDate with None -> null | Some v -> box v
                           iteration.Number |> box
                        ]
                    | None -> []
                let areaProperty =
                    match row.Area with
                    Some area -> area.AreaPath |> box
                    | None -> null
                let properties = 
                    azureFields
                    |> List.map(fun (name, getter) ->
                        getter row
                    )
                
                ((box timeStamp)::areaProperty::iterationProperties@properties)
                |> Array.ofList
            ) |> Array.ofSeq
        let data = 
            {
               ColumnNames = columnNames
               Values = rows
               RowCount = rows.Length
            } : Cache.DataResult
            
        data

    let read (source : AzureDevOpsSource.Root) =
        let key = source.JsonValue.ToString() |> keyFromSourceDoc

        assert(System.String.IsNullOrWhiteSpace key |> not)

        let timeStamp = System.DateTime.Now.ToString("dd/MM/yyyy H:mm")

        let raw = 
            source
            |> bySource
            |> Option.bind(Seq.reduce(fun d d' -> 
                    {
                        d with
                           Values = d.Values |> Array.append d'.Values
                           RowCount = d.RowCount + d'.RowCount
                    }
                ) >> Some
            )

        Log.logf "\n\n azure devops:%s \n\n" (source.JsonValue.ToString())        
        key,raw

    let sync azureToken (source : AzureDevOpsSource.Root) = 
        
        let rec _read hashes url : int * string = 
            Log.logf "syncing with %s %s" url azureToken

            let resp = 
                url
                |> request azureToken azureToken "GET" None                 
            if resp.StatusCode = 200 then
                let body = 
                    resp |> Http.readBody
                let rawId =  (url |> hash)
                let hashes = rawId::hashes
                match body with
                _ when body |> isEmpty |> not ->

                    let rawdataRecord =
                        let present p =
                            System.String.IsNullOrWhiteSpace p |> not

                        let body = 
                            ((body
                             |> AzureDevOpsAnalyticsRecord.Parse).Value
                            |> formatRawdataCache (System.DateTime.Now.ToString())).JsonValue

                        Encode.object [
                            "_id", Encode.string rawId
                            "timeStamp", Encode.string (System.DateTime.Now.ToString()) 
                            "data", body
                            "url", Encode.string url
                            "source", JsonValue.Parse (source.JsonValue.ToString())
                            "recordCount", Encode.int hashes.Length
                            "hashes", Encode.array (hashes |> Seq.map(Encode.string) |> Array.ofSeq)
                        ] |> Encode.toString 0
                        
                        |> insertOrUpdate 

                    body
                    |> tryNextLink
                    |> Option.map(fun nextlink ->   
                           Log.logf "Continuing with %s" nextlink
                           _read hashes nextlink
                    ) |> Option.orElse(Some(200,body))
                    |> Option.get
                | _ -> 
                    200,"ok"
            else 
                let message = 
                    match resp.Body with 
                    Text t -> t 
                    | _ -> ""
                resp.StatusCode, message
        
        let url = 
            source
            |> getInitialUrl                                   
        url
        |> _read []