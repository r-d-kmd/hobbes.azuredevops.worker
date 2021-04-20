open Hobbes.Helpers.Environment
open Hobbes.Web
open Hobbes.Messaging
open Hobbes.Messaging.Broker
open Hobbes.FSharp.Compile
open Hobbes.Web.RawdataTypes
open Hobbes.Workers.ODataProvider

let private handleMessage message =
    match message with
    Empty -> Success
    | Sync configDoc -> 
        Log.debugf "Received message. %s" configDoc
        try
            let configuration = 
                configDoc |> Config.Parse
            let source = configuration.Source
            let data = 
                {
                    Filter = source.Filter
                    Select = source.Select
                    Expand = source.Expand
                    User   = source.User
                    Pwd    = source.Pwd
                    Url    = source.Url.Value
                } |> Hobbes.Workers.ODataProvider.read
            let transformation = 
                match (configuration.Transformation |> Hobbes.FSharp.Compile.compile).Head.Blocks
                    |> Seq.filter(function
                                    Transformation _ -> true
                                    | _ -> false
                    ) |> Seq.exactlyOne with
                Transformation t -> t
                | _ -> failwith "should have been removed"
            let resultAsJson = 
                data 
                |> Hobbes.FSharp.DataStructures.DataMatrix.fromTable
                |> transformation
                |> Hobbes.FSharp.DataStructures.DataMatrix.toJson    

            
            match Http.post (Http.UniformData Http.Update) resultAsJson with
            Http.Success _ -> 
               Log.logf "Data uploaded to cache"
               Success
            | Http.Error(status,msg) -> 
                sprintf "Upload of %s to uniform data failed. %d %s" (data.ToString()) status msg
                |> Failure
        with e ->
            Log.excf e "Failed to process message"
            Excep e
            
[<EntryPoint>]
let main _ =
    Database.awaitDbServer()
    Database.initDatabases ["azure_devops_rawdata"]
    async{    
        do! awaitQueue()
        Broker.OData handleMessage
    } |> Async.RunSynchronously
    0