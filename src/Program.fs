open Hobbes.Helpers.Environment
open Hobbes.Web
open Hobbes.Messaging
open Hobbes.Messaging.Broker
open Hobbes.FSharp.Compile
open Hobbes.Web.RawdataTypes

let private handleMessage message =
    match message with
    Empty -> Success
    | Sync configDoc -> 
        Log.debugf "Received message. %s" configDoc
        try
            let config = 
                (configDoc |> Hobbes.Web.RawdataTypes.Config.Parse).Transformation
                |> Hobbes.FSharp.Compile.compile
                |> Seq.exactlyOne
            let data = ODataProvider.read config.Source
            let transformation = 
                match config.Blocks
                    |> Seq.filter(function
                                    Transformation _ -> true
                                    | _ -> false
                    ) |> Seq.exactlyOne with
                CompiledBlock.Transformation t -> t
                | _ -> failwith "should have been removed"
            let result = 
                data 
                |> Hobbes.FSharp.DataStructures.DataMatrix.fromTable
                |> transformation    

            
            match Http.post (Http.UniformData Http.Update) (data.ToString()) with
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