#nowarn "3061"
namespace Readers.AzureDevOps

open FSharp.Data
open Hobbes.Web
open Hobbes.Web.RawdataTypes

module Data =
    type internal AzureDevOpsAnalyticsRecord = JsonProvider<"""{
      "@odata.context": "https://analytics.dev.azure.com/kmddk/flowerpot/_odata/v2.0/$metadata#WorkItemRevisions(WorkItemId,WorkItemType,State,StateCategory,Iteration)",
      "timeStamp" : "ojsdfidsj",
      "value": [
        {
        "WorkItemId":3833,
        "RevisedDate":"2016-12-22T10:56:27.87+01:00",
        "IsCurrent":false,
        "IsLastRevisionOfDay":false,
        "WorkItemRevisionSK":62809820,
        "Title":"Manage templates",
        "WorkItemType":"Feature",
        "ChangedDate":"2016-12-22T09:22:40.967+01:00",
        "CreatedDate":"2016-12-19T11:19:08.42+01:00",
        "ClosedDate": "2018-09-13T16:14:24.323+02:00",
        "State":"User stories created",
        "Priority":2,
        "StateCategory":"Resolved",
        "StoryPoints": 2.0,
        "LeadTimeDays" : 200.1,
        "CycleTimeDays" : 9080.122,
        "Area": {
                "AreaPath": "Momentum"
            },
        "Iteration":{
            "IterationName":"Gandalf",
            "Number":159,
            "StartDate": "2017-05-15T00:00:00+02:00",
            "EndDate": "2017-05-28T23:59:59.999+02:00",
            "IterationPath":"Gandalf",
            "IterationLevel1":"Gandalf",
            "IterationLevel2":"Gandalf",
            "IterationLevel3":"Gandalf",
            "IterationLevel4":"Gandalf"
        }},{
        "WorkItemId":3833,
        "WorkItemRevisionSK":62809820,
        "Title":"Manage templates",
        "WorkItemType":"Feature",
        "Iteration":{
            "IterationName":"Gandalf",
            "Number":159,
            "IterationPath":"Gandalf"
        }},{
            "WorkItemId":3833,
            "WorkItemRevisionSK":62809820,
            "Title":"Manage templates",
            "WorkItemType":"Feature"
        }
    ], "@odata.nextLink":"https://analytics.dev.azure.com/"}""">

   
    [<Literal>]
    let AzureDevOpsSourceString = """{
            "name": "azuredevops",
            "account" : "kmddk",
            "project" : "gandalf",
            "dataset" : "commits",
            "server" : "https://analytics.dev.azure.com/kmddk/flowerpot",
            "query" : ["slice columns WorkItemType", "only 1 = 1", "only IsCurrent"]
        }"""

    [<Literal>]
    let AzureDevOpsDataString = """{
                "_id" : "name",
                "source" : """ + AzureDevOpsSourceString + """,
                "data" : {
                    "columnNames" : ["a","b"],
                    "values" : [["zcv","lkj"],[1.2,3.45],["2019-01-01","2019-01-01"]],
                    "rowCount" : 1
                }
        }"""
    type internal AzureDevOpsSource = FSharp.Data.JsonProvider<AzureDevOpsSourceString>
    type internal AzureDevOpsData = FSharp.Data.JsonProvider<AzureDevOpsDataString>

    let source2AzureSource (source : Config.Source) =
        source.JsonValue.ToString() |> AzureDevOpsSource.Parse

    let parseConfiguration doc = 
       let config = Config.Parse doc
       assert(config.Source.Provider = "azure devops")
       let source = config.Source |> source2AzureSource
       
       if System.String.IsNullOrWhiteSpace source.Server then
          if System.String.IsNullOrWhiteSpace source.Project then failwithf "Didn't supply a project %s" doc
          if System.String.IsNullOrWhiteSpace source.Account then failwithf "Account can't be empty %s" doc
       
       config

    type private WorkItemRevisionRecord = JsonProvider<"""
        {
             "id": "07e9a2611a712c808bd422425c9dcda2",
             "key": [
              "Azure DevOps",
              "flowerpot"
             ],
             "value": 90060205
    }""">

    type private RawList = JsonProvider<"""["id_a","id_b"]""">
    let private db = 
        Database.Database("azure_devops_rawdata", AzureDevOpsData.Parse, Log.loggerInstance)

    let insertOrUpdate (doc : string) = 
        db.InsertOrUpdate doc
        
    let get key = 
        db.TryGet key

    let tryLatestId searchKey =
        None //should ideal return the latest workitem id but views in production are unstable

    let list = 
        db.ListIds
    
    let projectsBySource (source : AzureDevOpsSource.Root) = 
        let configSearchKey = source.JsonValue.ToString() |> keyFromSourceDoc
        let res = 
            db.List() 
            |> Seq.filter(fun doc ->
                let hasData = 
                    doc.JsonValue.Properties() 
                    |> Seq.tryFind(fun (n,_) -> n = "data") 
                    |> Option.isSome
                hasData
                && (doc.Source.JsonValue.ToString() |> keyFromSourceDoc) = configSearchKey
            )
        res

    let bySource (source : AzureDevOpsSource.Root) = 
        let data =
            source
            |> projectsBySource
       
        let result = 
            data
            |> Seq.map(fun s -> 
                s.Data.JsonValue.ToString()
                |> Cache.DataResult.OfJson
            )
        if result |> Seq.isEmpty then None
        else Some result